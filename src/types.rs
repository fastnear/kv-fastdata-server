//! Wire types for the KV FastData API.
//!
//! These are the request and response bodies exchanged over HTTP. Under the
//! `openapi` feature they additionally derive `schemars::JsonSchema`, which is
//! what `generate-openapi` uses to emit the published schema. The runtime
//! (default features) sees plain `serde` types with identical behavior.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

// ---- Request types ----

// History endpoints (`asc` selects oldest-first ordering).
#[derive(Debug, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct QueryRequest {
    /// Exact key filter. Mutually exclusive with `key_prefix`.
    pub key: Option<String>,
    /// Prefix filter for matching key namespaces.
    pub key_prefix: Option<String>,
    /// Maximum number of entries to return in one page (1–200, default 50).
    #[cfg_attr(feature = "openapi", schemars(range(min = 1, max = 200)))]
    pub limit: Option<i32>,
    /// Opaque pagination cursor from a previous response for the same endpoint and filter set.
    pub page_token: Option<String>,
    /// Include receipt and signer metadata in each entry.
    #[serde(default)]
    pub include_metadata: bool,
    /// Sort ascending for history endpoints. Defaults to newest-first.
    #[serde(default)]
    pub asc: bool,
}

// Latest endpoints. Same as `QueryRequest` minus `asc`, which they do not honor.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct LatestRequest {
    /// Exact key filter. Mutually exclusive with `key_prefix`.
    pub key: Option<String>,
    /// Prefix filter for matching key namespaces.
    pub key_prefix: Option<String>,
    /// Maximum number of entries to return in one page (1–200, default 50).
    #[cfg_attr(feature = "openapi", schemars(range(min = 1, max = 200)))]
    pub limit: Option<i32>,
    /// Opaque pagination cursor from a previous response for the same endpoint and filter set.
    pub page_token: Option<String>,
    /// Include receipt and signer metadata in each entry.
    #[serde(default)]
    pub include_metadata: bool,
}

// `/v0/all/{predecessor_id}`: paginates across all contracts, no key filter.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct AllRequest {
    /// Maximum number of entries to return in one page (1–200, default 50).
    #[cfg_attr(feature = "openapi", schemars(range(min = 1, max = 200)))]
    pub limit: Option<i32>,
    /// Opaque pagination cursor from a previous `/v0/all/{predecessor_id}` response.
    pub page_token: Option<String>,
    /// Include receipt and signer metadata in each entry.
    #[serde(default)]
    pub include_metadata: bool,
}

// `/v0/history`: exact-key lookup across all indexed contracts.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct ByKeyRequest {
    /// Exact key name to match across all accounts and predecessors.
    pub key: String,
    /// Maximum number of entries to return in one page (1–200, default 50).
    #[cfg_attr(feature = "openapi", schemars(range(min = 1, max = 200)))]
    pub limit: Option<i32>,
    /// Opaque pagination cursor from a previous response for the same endpoint and filter set.
    pub page_token: Option<String>,
    /// Include receipt and signer metadata in each entry.
    #[serde(default)]
    pub include_metadata: bool,
    /// Sort ascending for history results. Defaults to newest-first.
    #[serde(default)]
    pub asc: bool,
}

// `/v0/multi`: batch latest-value lookup, up to 100 keys.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct MultiRequest {
    #[cfg_attr(feature = "openapi", schemars(schema_with = "multi_keys_schema"))]
    pub keys: Vec<String>,
    /// Include receipt and signer metadata in each entry.
    #[serde(default)]
    pub include_metadata: bool,
}

// ---- Response types ----

// A single key-value record. Metadata fields are present only when requested.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct KvEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer_id: Option<String>,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub key: String,
    #[cfg_attr(feature = "openapi", schemars(schema_with = "any_json_schema"))]
    pub value: Box<RawValue>,
}

// Paginated list returned by the history/latest/all endpoints.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct ListResponse {
    pub entries: Vec<KvEntry>,
    /// Opaque pagination cursor for the next page. Absent when there are no more results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

// `/v0/multi` response: one slot per requested key, `null` where missing.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct MultiResponse {
    #[cfg_attr(
        feature = "openapi",
        schemars(schema_with = "nullable_kv_entry_array_schema")
    )]
    pub entries: Vec<Option<KvEntry>>,
}

// Error body returned for 4xx/5xx responses.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct ErrorResponse {
    pub error: String,
}

// ---- OpenAPI schema customizations ----
//
// These describe shapes `schemars` cannot infer from the Rust types alone
// (a capped array, an opaque JSON value, a nullable-item array). They affect
// only the generated schema and are compiled out when the `openapi` feature
// is off.

#[cfg(feature = "openapi")]
fn multi_keys_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": {
            "type": "string"
        },
        "maxItems": 100,
        "description": "Fully qualified keys in current_account_id/predecessor_id/key form."
    })
}

#[cfg(feature = "openapi")]
fn any_json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let mut schema = schemars::json_schema!({});
    schema.ensure_object().insert(
        "description".into(),
        "Raw JSON value as stored in FastData.".into(),
    );
    schema
}

#[cfg(feature = "openapi")]
fn nullable_kv_entry_array_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let item = <KvEntry as schemars::JsonSchema>::json_schema(generator).to_value();
    schemars::json_schema!({
        "type": "array",
        "items": {
            "oneOf": [
                item,
                {
                    "enum": [null]
                }
            ]
        }
    })
}
