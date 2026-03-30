mod scylladb;

use crate::scylladb::{
    KeyFilter, KvRow, MvCurKeyPage, MvKeyPage, MvLastCurKeyPage, ScyllaDb, SkVLastAllPage,
    SkVLastPage, SkVPage,
};
use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::env;
use std::sync::Arc;

const PROJECT_ID: &str = "kv-fastdata-server";
const DEFAULT_LIMIT: i32 = 50;
const MAX_LIMIT: i32 = 200;
const MAX_MULTI_KEYS: usize = 100;

#[derive(Clone)]
pub struct AppState {
    pub scylladb: Arc<ScyllaDb>,
}

// ---- Request types ----

#[derive(Deserialize, Default)]
struct QueryRequest {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    key_prefix: Option<String>,
    #[serde(default)]
    limit: Option<i32>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    include_metadata: bool,
    #[serde(default)]
    asc: bool,
}

#[derive(Deserialize)]
struct ByKeyRequest {
    key: String,
    #[serde(default)]
    limit: Option<i32>,
    #[serde(default)]
    page_token: Option<String>,
    #[serde(default)]
    include_metadata: bool,
    #[serde(default)]
    asc: bool,
}

#[derive(Deserialize)]
struct MultiRequest {
    keys: Vec<String>,
    #[serde(default)]
    include_metadata: bool,
}

// ---- Response types ----

#[derive(Serialize)]
struct KvEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signer_id: Option<String>,
    predecessor_id: String,
    current_account_id: String,
    block_height: u64,
    block_timestamp: u64,
    key: String,
    value: Box<RawValue>,
}

#[derive(Serialize)]
struct ListResponse {
    entries: Vec<KvEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_token: Option<String>,
}

#[derive(Serialize)]
struct MultiResponse {
    entries: Vec<Option<KvEntry>>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// ---- Conversions ----

fn row_to_entry(row: KvRow, include_metadata: bool) -> KvEntry {
    let value = RawValue::from_string(row.value.clone()).unwrap_or_else(|_| {
        RawValue::from_string(serde_json::to_string(&row.value).unwrap()).unwrap()
    });
    KvEntry {
        receipt_id: if include_metadata {
            Some(row.receipt_id)
        } else {
            None
        },
        action_index: if include_metadata {
            Some(row.action_index as u32)
        } else {
            None
        },
        tx_hash: if include_metadata { row.tx_hash } else { None },
        signer_id: if include_metadata {
            Some(row.signer_id)
        } else {
            None
        },
        predecessor_id: row.predecessor_id,
        current_account_id: row.current_account_id,
        block_height: row.block_height as u64,
        block_timestamp: row.block_timestamp as u64,
        key: row.key,
        value,
    }
}

fn clamp_limit(limit: Option<i32>) -> i32 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

fn key_filter<'a>(
    key: &'a Option<String>,
    key_prefix: &'a Option<String>,
) -> Result<KeyFilter<'a>, HttpResponse> {
    match (key.as_deref(), key_prefix.as_deref()) {
        (Some(_), Some(_)) => Err(HttpResponse::BadRequest().json(ErrorResponse {
            error: "key and key_prefix are mutually exclusive".into(),
        })),
        (Some(k), None) => Ok(KeyFilter::Exact(k)),
        (None, Some(p)) => Ok(KeyFilter::Prefix(p)),
        (None, None) => Ok(KeyFilter::None),
    }
}

/// Parse multi-key string "current_account_id/predecessor_id/key" where key may contain slashes.
fn parse_multi_key(s: &str) -> Option<(&str, &str, &str)> {
    let mut parts = s.splitn(3, '/');
    let account = parts.next().filter(|s| !s.is_empty())?;
    let predecessor = parts.next().filter(|s| !s.is_empty())?;
    let key = parts.next().filter(|s| !s.is_empty())?;
    Some((account, predecessor, key))
}

// ---- Page token encode/decode ----

fn encode_token(value: &serde_json::Value) -> String {
    URL_SAFE_NO_PAD.encode(value.to_string())
}

fn decode_token(token: &str) -> Result<serde_json::Value, HttpResponse> {
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| {
        HttpResponse::BadRequest().json(ErrorResponse {
            error: "Invalid page_token".into(),
        })
    })?;
    serde_json::from_slice(&bytes).map_err(|_| {
        HttpResponse::BadRequest().json(ErrorResponse {
            error: "Invalid page_token".into(),
        })
    })
}

fn get_str(v: &serde_json::Value, field: &str) -> Result<String, HttpResponse> {
    v.get(field)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            HttpResponse::BadRequest().json(ErrorResponse {
                error: "Invalid page_token".into(),
            })
        })
}

fn get_i64(v: &serde_json::Value, field: &str) -> Result<i64, HttpResponse> {
    v.get(field).and_then(|v| v.as_i64()).ok_or_else(|| {
        HttpResponse::BadRequest().json(ErrorResponse {
            error: "Invalid page_token".into(),
        })
    })
}

fn make_page_token(
    rows: &[KvRow],
    limit: i32,
    make_token: impl FnOnce(&KvRow) -> serde_json::Value,
) -> Option<String> {
    if rows.len() as i32 == limit {
        rows.last().map(|last| encode_token(&make_token(last)))
    } else {
        None
    }
}

fn err_500(e: anyhow::Error) -> HttpResponse {
    tracing::error!(target: PROJECT_ID, "Database error: {e}");
    HttpResponse::InternalServerError().json(ErrorResponse {
        error: "Internal error".into(),
    })
}

// ---- Handlers ----

// POST /v0/history/{current_account_id}/{predecessor_id}
async fn history_by_predecessor(
    path: web::Path<(String, String)>,
    body: web::Json<QueryRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let (current_account_id, predecessor_id) = path.into_inner();
    let kf = match key_filter(&body.key, &body.key_prefix) {
        Ok(kf) => kf,
        Err(e) => return e,
    };
    let limit = clamp_limit(body.limit);
    let meta = body.include_metadata;

    let page = match &body.page_token {
        None => None,
        Some(token) => {
            let v = match decode_token(token) {
                Ok(v) => v,
                Err(e) => return e,
            };
            match &kf {
                KeyFilter::Exact(_) => {
                    let b = match get_i64(&v, "b") {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    let o = match get_i64(&v, "o") {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    Some(SkVPage {
                        key: body.key.clone().unwrap_or_default(),
                        block_height: b,
                        order_id: o,
                    })
                }
                _ => {
                    let k = match get_str(&v, "k") {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    let b = match get_i64(&v, "b") {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    let o = match get_i64(&v, "o") {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    Some(SkVPage {
                        key: k,
                        block_height: b,
                        order_id: o,
                    })
                }
            }
        }
    };

    let rows = state
        .scylladb
        .query_kv_by_predecessor(&predecessor_id, &current_account_id, kf, page, limit, !body.asc)
        .await;

    match rows {
        Ok(rows) => {
            let page_token = make_page_token(&rows, limit, |last| {
                serde_json::json!({"k": last.key, "b": last.block_height, "o": last.order_id})
            });
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, meta))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token,
            })
        }
        Err(e) => err_500(e),
    }
}

// POST /v0/latest/{current_account_id}/{predecessor_id}
async fn latest_by_predecessor(
    path: web::Path<(String, String)>,
    body: web::Json<QueryRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let (current_account_id, predecessor_id) = path.into_inner();
    let kf = match key_filter(&body.key, &body.key_prefix) {
        Ok(kf) => kf,
        Err(e) => return e,
    };
    let limit = clamp_limit(body.limit);
    let meta = body.include_metadata;

    let page = match &body.page_token {
        None => None,
        Some(token) => {
            let v = match decode_token(token) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let k = match get_str(&v, "k") {
                Ok(v) => v,
                Err(e) => return e,
            };
            Some(SkVLastPage { key: k })
        }
    };

    let rows = state
        .scylladb
        .query_kv_latest_by_predecessor(&predecessor_id, &current_account_id, kf, page, limit)
        .await;

    match rows {
        Ok(rows) => {
            let page_token =
                make_page_token(&rows, limit, |last| serde_json::json!({"k": last.key}));
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, meta))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token,
            })
        }
        Err(e) => err_500(e),
    }
}

// POST /v0/history/{current_account_id}
async fn history_by_account(
    path: web::Path<String>,
    body: web::Json<QueryRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let current_account_id = path.into_inner();
    let kf = match key_filter(&body.key, &body.key_prefix) {
        Ok(kf) => kf,
        Err(e) => return e,
    };
    let limit = clamp_limit(body.limit);
    let meta = body.include_metadata;

    let page = match &body.page_token {
        None => None,
        Some(token) => {
            let v = match decode_token(token) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let k = match get_str(&v, "k") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let b = match get_i64(&v, "b") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let o = match get_i64(&v, "o") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let p = match get_str(&v, "p") {
                Ok(v) => v,
                Err(e) => return e,
            };
            Some(MvCurKeyPage {
                key: k,
                block_height: b,
                order_id: o,
                predecessor_id: p,
            })
        }
    };

    let rows = state
        .scylladb
        .query_kv_by_account(&current_account_id, kf, page, limit, !body.asc)
        .await;

    match rows {
        Ok(rows) => {
            let page_token = make_page_token(&rows, limit, |last| {
                serde_json::json!({"k": last.key, "b": last.block_height, "o": last.order_id, "p": last.predecessor_id})
            });
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, meta))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token,
            })
        }
        Err(e) => err_500(e),
    }
}

// POST /v0/latest/{current_account_id}
async fn latest_by_account(
    path: web::Path<String>,
    body: web::Json<QueryRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let current_account_id = path.into_inner();
    let kf = match key_filter(&body.key, &body.key_prefix) {
        Ok(kf) => kf,
        Err(e) => return e,
    };
    let limit = clamp_limit(body.limit);
    let meta = body.include_metadata;

    let page = match &body.page_token {
        None => None,
        Some(token) => {
            let v = match decode_token(token) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let k = match get_str(&v, "k") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let p = match get_str(&v, "p") {
                Ok(v) => v,
                Err(e) => return e,
            };
            Some(MvLastCurKeyPage {
                key: k,
                predecessor_id: p,
            })
        }
    };

    let rows = state
        .scylladb
        .query_kv_latest_by_account(&current_account_id, kf, page, limit)
        .await;

    match rows {
        Ok(rows) => {
            let page_token = make_page_token(&rows, limit, |last| {
                serde_json::json!({"k": last.key, "p": last.predecessor_id})
            });
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, meta))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token,
            })
        }
        Err(e) => err_500(e),
    }
}

// POST /v0/all/{predecessor_id}
async fn all_by_predecessor(
    path: web::Path<String>,
    body: web::Json<QueryRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let predecessor_id = path.into_inner();
    let limit = clamp_limit(body.limit);
    let meta = body.include_metadata;

    let page = match &body.page_token {
        None => None,
        Some(token) => {
            let v = match decode_token(token) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let a = match get_str(&v, "a") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let k = match get_str(&v, "k") {
                Ok(v) => v,
                Err(e) => return e,
            };
            Some(SkVLastAllPage {
                current_account_id: a,
                key: k,
            })
        }
    };

    let rows = state
        .scylladb
        .query_kv_all_by_predecessor(&predecessor_id, page, limit)
        .await;

    match rows {
        Ok(rows) => {
            let page_token = make_page_token(&rows, limit, |last| {
                serde_json::json!({"a": last.current_account_id, "k": last.key})
            });
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, meta))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token,
            })
        }
        Err(e) => err_500(e),
    }
}

// POST /v0/history
async fn history_by_key(
    body: web::Json<ByKeyRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let limit = clamp_limit(body.limit);
    let meta = body.include_metadata;

    let page = match &body.page_token {
        None => None,
        Some(token) => {
            let v = match decode_token(token) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let b = match get_i64(&v, "b") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let o = match get_i64(&v, "o") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let p = match get_str(&v, "p") {
                Ok(v) => v,
                Err(e) => return e,
            };
            let a = match get_str(&v, "a") {
                Ok(v) => v,
                Err(e) => return e,
            };
            Some(MvKeyPage {
                block_height: b,
                order_id: o,
                predecessor_id: p,
                current_account_id: a,
            })
        }
    };

    let rows = state.scylladb.query_kv_by_key(&body.key, page, limit, !body.asc).await;

    match rows {
        Ok(rows) => {
            let page_token = make_page_token(&rows, limit, |last| {
                serde_json::json!({"b": last.block_height, "o": last.order_id, "p": last.predecessor_id, "a": last.current_account_id})
            });
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, meta))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token,
            })
        }
        Err(e) => err_500(e),
    }
}

// POST /v0/multi
async fn multi(body: web::Json<MultiRequest>, state: web::Data<AppState>) -> HttpResponse {
    if body.keys.len() > MAX_MULTI_KEYS {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: format!("Maximum {MAX_MULTI_KEYS} keys per request"),
        });
    }

    let parsed: Vec<(&str, &str, &str)> = match body
        .keys
        .iter()
        .map(|s| {
            parse_multi_key(s).ok_or_else(|| {
                HttpResponse::BadRequest().json(ErrorResponse {
                    error: format!(
                        "Invalid key format: {s:?}. Expected current_account_id/predecessor_id/key"
                    ),
                })
            })
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(v) => v,
        Err(e) => return e,
    };

    // query_kv_multi expects (predecessor_id, current_account_id, key)
    let keys: Vec<(&str, &str, &str)> = parsed
        .iter()
        .map(|(account, predecessor, key)| (*predecessor, *account, *key))
        .collect();

    let meta = body.include_metadata;
    let results = state.scylladb.query_kv_multi(&keys).await;

    match results {
        Ok(rows) => {
            let entries: Vec<Option<KvEntry>> = rows
                .into_iter()
                .map(|r| r.map(|r| row_to_entry(r, meta)))
                .collect();
            HttpResponse::Ok().json(MultiResponse { entries })
        }
        Err(e) => err_500(e),
    }
}

// GET /v0/history/{current_account_id}/{predecessor_id}/{key:.*}
async fn get_history_key(
    path: web::Path<(String, String, String)>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let (current_account_id, predecessor_id, key) = path.into_inner();
    let rows = state
        .scylladb
        .query_kv_by_predecessor(
            &predecessor_id,
            &current_account_id,
            KeyFilter::Exact(&key),
            None,
            DEFAULT_LIMIT,
            true,
        )
        .await;

    match rows {
        Ok(rows) => {
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, false))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token: None,
            })
        }
        Err(e) => err_500(e),
    }
}

// GET /v0/latest/{current_account_id}/{predecessor_id}/{key:.*}
async fn get_latest_key(
    path: web::Path<(String, String, String)>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let (current_account_id, predecessor_id, key) = path.into_inner();
    let rows = state
        .scylladb
        .query_kv_latest_by_predecessor(
            &predecessor_id,
            &current_account_id,
            KeyFilter::Exact(&key),
            None,
            1,
        )
        .await;

    match rows {
        Ok(rows) => {
            let entries: Vec<KvEntry> = rows
                .into_iter()
                .map(|r| row_to_entry(r, false))
                .collect();
            HttpResponse::Ok().json(ListResponse {
                entries,
                page_token: None,
            })
        }
        Err(e) => err_500(e),
    }
}

// ---- Documentation handlers ----

async fn serve_index(state: web::Data<DocsState>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(state.index_html.clone())
}

async fn serve_skill(state: web::Data<DocsState>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/markdown; charset=utf-8")
        .body(state.skill_md.clone())
}

#[derive(Clone)]
pub struct DocsState {
    pub index_html: String,
    pub skill_md: String,
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scylladb::KvRow;

    fn make_row(key: &str, value: &str, block_height: i64, order_id: i64) -> KvRow {
        KvRow {
            receipt_id: "abc123".into(),
            action_index: 0,
            tx_hash: None,
            signer_id: "signer.near".into(),
            predecessor_id: "pred.near".into(),
            current_account_id: "acct.near".into(),
            block_height,
            block_timestamp: 1_000_000_000,
            shard_id: 0,
            receipt_index: 0,
            order_id,
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn test_clamp_limit_default() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
    }

    #[test]
    fn test_clamp_limit_valid() {
        assert_eq!(clamp_limit(Some(10)), 10);
        assert_eq!(clamp_limit(Some(200)), 200);
    }

    #[test]
    fn test_clamp_limit_clamped() {
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(-5)), 1);
        assert_eq!(clamp_limit(Some(999)), MAX_LIMIT);
    }

    #[test]
    fn test_key_filter_none() {
        let key = None;
        let prefix = None;
        assert!(matches!(key_filter(&key, &prefix), Ok(KeyFilter::None)));
    }

    #[test]
    fn test_key_filter_exact() {
        let key = Some("score".into());
        let prefix = None;
        assert!(matches!(
            key_filter(&key, &prefix),
            Ok(KeyFilter::Exact("score"))
        ));
    }

    #[test]
    fn test_key_filter_prefix() {
        let key = None;
        let prefix = Some("settings/".into());
        assert!(matches!(
            key_filter(&key, &prefix),
            Ok(KeyFilter::Prefix("settings/"))
        ));
    }

    #[test]
    fn test_key_filter_both_errors() {
        let key = Some("a".into());
        let prefix = Some("b".into());
        assert!(key_filter(&key, &prefix).is_err());
    }

    #[test]
    fn test_parse_multi_key() {
        assert_eq!(
            parse_multi_key("app.near/alice.near/score"),
            Some(("app.near", "alice.near", "score"))
        );
        assert_eq!(
            parse_multi_key("app.near/alice.near/settings/volume"),
            Some(("app.near", "alice.near", "settings/volume"))
        );
        assert_eq!(parse_multi_key("app.near/alice.near/"), None);
        assert_eq!(parse_multi_key("app.near/"), None);
        assert_eq!(parse_multi_key("app.near"), None);
        assert_eq!(parse_multi_key(""), None);
    }

    #[test]
    fn test_page_token_roundtrip() {
        let original = serde_json::json!({"k": "score", "b": 12345_i64, "o": 42_i64});
        let token = encode_token(&original);
        let decoded = decode_token(&token).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_token_invalid_base64() {
        assert!(decode_token("!!!invalid!!!").is_err());
    }

    #[test]
    fn test_decode_token_invalid_json() {
        let token = URL_SAFE_NO_PAD.encode("not json");
        assert!(decode_token(&token).is_err());
    }

    #[test]
    fn test_get_str_and_i64() {
        let v = serde_json::json!({"k": "hello", "b": 42});
        assert_eq!(get_str(&v, "k").unwrap(), "hello");
        assert_eq!(get_i64(&v, "b").unwrap(), 42);
        assert!(get_str(&v, "missing").is_err());
        assert!(get_i64(&v, "missing").is_err());
        assert!(get_str(&v, "b").is_err());
        assert!(get_i64(&v, "k").is_err());
    }

    #[test]
    fn test_make_page_token_has_more() {
        let rows = vec![
            make_row("k1", "\"v1\"", 100, 1),
            make_row("k2", "\"v2\"", 100, 2),
        ];
        let token = make_page_token(&rows, 2, |last| serde_json::json!({"k": last.key}));
        assert!(token.is_some());
        let decoded = decode_token(&token.unwrap()).unwrap();
        assert_eq!(decoded["k"], "k2");
    }

    #[test]
    fn test_make_page_token_no_more() {
        let rows = vec![make_row("k1", "\"v1\"", 100, 1)];
        let token = make_page_token(&rows, 2, |last| serde_json::json!({"k": last.key}));
        assert!(token.is_none());
    }

    #[test]
    fn test_make_page_token_empty() {
        let rows: Vec<KvRow> = vec![];
        let token = make_page_token(&rows, 2, |last| serde_json::json!({"k": last.key}));
        assert!(token.is_none());
    }

    #[test]
    fn test_row_to_entry_without_metadata() {
        let row = make_row("score", "1500", 100, 1);
        let entry = row_to_entry(row, false);
        assert_eq!(entry.key, "score");
        assert_eq!(entry.block_height, 100);
        assert_eq!(entry.value.get(), "1500");
        assert!(entry.receipt_id.is_none());
        assert!(entry.action_index.is_none());
        assert!(entry.signer_id.is_none());
    }

    #[test]
    fn test_row_to_entry_with_metadata() {
        let row = make_row("score", "1500", 100, 1);
        let entry = row_to_entry(row, true);
        assert_eq!(entry.receipt_id.as_deref(), Some("abc123"));
        assert_eq!(entry.action_index, Some(0));
        assert_eq!(entry.signer_id.as_deref(), Some("signer.near"));
    }

    #[test]
    fn test_row_to_entry_invalid_json_fallback() {
        let row = make_row("bad", "not valid json", 100, 1);
        let entry = row_to_entry(row, false);
        assert_eq!(entry.value.get(), "\"not valid json\"");
    }

    #[test]
    fn test_entry_serialization_no_metadata() {
        let row = make_row("score", "1500", 200, 5);
        let entry = row_to_entry(row, false);
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["key"], "score");
        assert_eq!(json["value"], 1500);
        assert_eq!(json["block_height"], 200);
        assert_eq!(json["predecessor_id"], "pred.near");
        // Metadata fields should be absent
        assert!(json.get("receipt_id").is_none());
        assert!(json.get("action_index").is_none());
        assert!(json.get("signer_id").is_none());
        assert!(json.get("tx_hash").is_none());
    }

    #[test]
    fn test_entry_serialization_with_metadata() {
        let row = make_row("score", "1500", 200, 5);
        let entry = row_to_entry(row, true);
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["receipt_id"], "abc123");
        assert_eq!(json["action_index"], 0);
        assert_eq!(json["signer_id"], "signer.near");
        // tx_hash was None in make_row, still absent
        assert!(json.get("tx_hash").is_none());
    }

    #[test]
    fn test_list_response_serialization() {
        let rows = vec![make_row("k", "true", 100, 1)];
        let page_token = make_page_token(&rows, 1, |last| serde_json::json!({"k": last.key}));
        let entries: Vec<KvEntry> = rows
            .into_iter()
            .map(|r| row_to_entry(r, false))
            .collect();
        let resp = ListResponse {
            entries,
            page_token,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["entries"].as_array().unwrap().len(), 1);
        assert_eq!(json["entries"][0]["value"], true);
        assert!(json["page_token"].is_string());
    }

    #[test]
    fn test_list_response_no_page_token() {
        let resp = ListResponse {
            entries: vec![],
            page_token: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json.get("page_token").is_none());
    }
}

// ---- Main ----

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("info,kv_fastdata_server=debug")
        .init();

    let chain_id = env::var("CHAIN_ID").expect("CHAIN_ID required");
    let port = env::var("PORT").unwrap_or_else(|_| "3010".to_string());
    let web_hostname =
        env::var("WEB_HOSTNAME").unwrap_or_else(|_| format!("http://127.0.0.1:{port}"));

    let scylla_session = ScyllaDb::new_scylla_session()
        .await
        .expect("Can't create scylla session");

    ScyllaDb::test_connection(&scylla_session)
        .await
        .expect("Can't connect to scylla");

    tracing::info!(target: PROJECT_ID, "Connected to ScyllaDB");

    let scylladb = Arc::new(
        ScyllaDb::new(&chain_id, scylla_session)
            .await
            .expect("Can't initialize scylla db"),
    );

    let docs = DocsState {
        index_html: include_str!("../static/index.html").replace("{{HOSTNAME}}", &web_hostname),
        skill_md: include_str!("../static/skill.md").replace("{{HOSTNAME}}", &web_hostname),
    };

    tracing::info!(target: PROJECT_ID, "Listening on 127.0.0.1:{port}");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![
                actix_web::http::header::CONTENT_TYPE,
                actix_web::http::header::ACCEPT,
            ])
            .max_age(3600);

        App::new()
            .app_data(web::Data::new(AppState {
                scylladb: Arc::clone(&scylladb),
            }))
            .app_data(web::Data::new(docs.clone()))
            .wrap(cors)
            .wrap(middleware::Logger::new(
                "%{r}a \"%r\"\t%s %b \"%{User-Agent}i\" %T",
            ))
            .wrap(tracing_actix_web::TracingLogger::default())
            // Documentation
            .route("/", web::get().to(serve_index))
            .route("/index.html", web::get().to(serve_index))
            .route("/skill.md", web::get().to(serve_skill))
            .route("/SKILL.md", web::get().to(serve_skill))
            // POST endpoints
            .route("/v0/history", web::post().to(history_by_key))
            .route("/v0/multi", web::post().to(multi))
            .route(
                "/v0/all/{predecessor_id}",
                web::post().to(all_by_predecessor),
            )
            .route(
                "/v0/history/{current_account_id}",
                web::post().to(history_by_account),
            )
            .route(
                "/v0/latest/{current_account_id}",
                web::post().to(latest_by_account),
            )
            .route(
                "/v0/history/{current_account_id}/{predecessor_id}",
                web::post().to(history_by_predecessor),
            )
            .route(
                "/v0/latest/{current_account_id}/{predecessor_id}",
                web::post().to(latest_by_predecessor),
            )
            // GET endpoints
            .route(
                "/v0/history/{current_account_id}/{predecessor_id}/{key:.*}",
                web::get().to(get_history_key),
            )
            .route(
                "/v0/latest/{current_account_id}/{predecessor_id}/{key:.*}",
                web::get().to(get_latest_key),
            )
    })
    .bind(format!("127.0.0.1:{port}"))?
    .run()
    .await?;

    Ok(())
}
