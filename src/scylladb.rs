use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use scylla::DeserializeRow;
use std::env;
use std::sync::Arc;

const ALL_COLS: &str = "receipt_id, action_index, tx_hash, signer_id, predecessor_id, \
    current_account_id, block_height, block_timestamp, shard_id, receipt_index, order_id, \
    key, value";

#[derive(DeserializeRow)]
pub struct KvRow {
    pub receipt_id: String,
    pub action_index: i32,
    pub tx_hash: Option<String>,
    pub signer_id: String,
    pub predecessor_id: String,
    pub current_account_id: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub order_id: i64,
    pub key: String,
    pub value: String,
}

pub enum KeyFilter<'a> {
    None,
    Exact(&'a str),
    Prefix(&'a str),
}

pub struct SkVPage {
    pub key: String,
    pub block_height: i64,
    pub order_id: i64,
}

pub struct SkVLastPage {
    pub key: String,
}

pub struct MvCurKeyPage {
    pub key: String,
    pub block_height: i64,
    pub order_id: i64,
    pub predecessor_id: String,
}

pub struct MvLastCurKeyPage {
    pub key: String,
    pub predecessor_id: String,
}

pub struct MvKeyPage {
    pub block_height: i64,
    pub order_id: i64,
    pub predecessor_id: String,
    pub current_account_id: String,
}

/// A pair of prepared statements: ASC and DESC variants.
struct AscDesc {
    asc: PreparedStatement,
    desc: PreparedStatement,
}

impl AscDesc {
    fn pick(&self, desc: bool) -> &PreparedStatement {
        if desc { &self.desc } else { &self.asc }
    }
}

pub struct ScyllaDb {
    pub scylla_session: Session,

    // s_kv (history by predecessor+account)
    skv_no_key: AscDesc,
    skv_no_key_page: AscDesc,
    skv_exact_key: AscDesc,
    skv_exact_key_page: AscDesc,
    skv_prefix: AscDesc,
    skv_prefix_page: AscDesc,

    // s_kv_last (latest by predecessor+account)
    skv_last_no_key: PreparedStatement,
    skv_last_no_key_page: PreparedStatement,
    skv_last_exact_key: PreparedStatement,
    skv_last_prefix: PreparedStatement,
    skv_last_prefix_page: PreparedStatement,

    // mv_kv_cur_key (history by account)
    mv_cur_no_key: AscDesc,
    mv_cur_no_key_page: AscDesc,
    mv_cur_exact_key: AscDesc,
    mv_cur_exact_key_page: AscDesc,
    mv_cur_prefix: AscDesc,
    mv_cur_prefix_page: AscDesc,

    // mv_kv_last_cur_key (latest by account)
    mv_last_cur_no_key: PreparedStatement,
    mv_last_cur_no_key_page: PreparedStatement,
    mv_last_cur_exact_key: PreparedStatement,
    mv_last_cur_exact_key_page: PreparedStatement,
    mv_last_cur_prefix: PreparedStatement,
    mv_last_cur_prefix_page: PreparedStatement,

    // mv_kv_key (global by key)
    mv_key: AscDesc,
    mv_key_page: AscDesc,
}

fn create_rustls_client_config() -> Arc<ClientConfig> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install default provider");
    }
    let ca_cert_path =
        env::var("SCYLLA_SSL_CA").expect("SCYLLA_SSL_CA environment variable not set");
    let client_cert_path =
        env::var("SCYLLA_SSL_CERT").expect("SCYLLA_SSL_CERT environment variable not set");
    let client_key_path =
        env::var("SCYLLA_SSL_KEY").expect("SCYLLA_SSL_KEY environment variable not set");

    let ca_certs = rustls::pki_types::CertificateDer::from_pem_file(ca_cert_path)
        .expect("Failed to load CA certs");
    let client_certs = rustls::pki_types::CertificateDer::from_pem_file(client_cert_path)
        .expect("Failed to load client certs");
    let client_key = rustls::pki_types::PrivateKeyDer::from_pem_file(client_key_path)
        .expect("Failed to load client key");

    let mut root_store = RootCertStore::empty();
    root_store.add(ca_certs).expect("Failed to add CA certs");

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_certs], client_key)
        .expect("Failed to create client config");

    Arc::new(config)
}

// ORDER BY clauses for DESC queries
const SKV_DESC: &str = "ORDER BY current_account_id DESC, key DESC, block_height DESC, order_id DESC";
const SKV_KEY_DESC: &str = "ORDER BY block_height DESC, order_id DESC";
const MV_CUR_DESC: &str =
    "ORDER BY key DESC, block_height DESC, order_id DESC, predecessor_id DESC";
const MV_KEY_DESC: &str =
    "ORDER BY block_height DESC, order_id DESC, predecessor_id DESC, current_account_id DESC";

impl ScyllaDb {
    pub async fn new_scylla_session() -> anyhow::Result<Session> {
        let scylla_url = env::var("SCYLLA_URL").expect("SCYLLA_URL must be set");

        let mut builder = SessionBuilder::new().known_node(scylla_url);

        if env::var("SCYLLA_SSL_CA").is_ok() {
            builder = builder.tls_context(Some(create_rustls_client_config()));
        }

        if let (Ok(username), Ok(password)) =
            (env::var("SCYLLA_USERNAME"), env::var("SCYLLA_PASSWORD"))
        {
            builder = builder.authenticator_provider(Arc::new(
                scylla::authentication::PlainTextAuthenticator::new(username, password),
            ));
        }

        let session: Session = builder.build().await?;
        Ok(session)
    }

    pub async fn test_connection(scylla_session: &Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await?;
        Ok(())
    }

    async fn prepare(session: &Session, query: String) -> anyhow::Result<PreparedStatement> {
        let mut stmt = scylla::statement::Statement::new(query);
        stmt.set_consistency(scylla::frame::types::Consistency::LocalOne);
        Ok(session.prepare(stmt).await?)
    }

    pub async fn new(chain_id: &str, session: Session) -> anyhow::Result<Self> {
        session
            .use_keyspace(format!("fastdata_{chain_id}"), false)
            .await?;

        macro_rules! q {
            ($($args:tt)*) => {
                Self::prepare(&session, format!($($args)*)).await?
            };
        }

        // Macro for ASC+DESC pair. $asc_sql is the ASC query, $desc_sql is the DESC query.
        macro_rules! ad {
            ($asc:expr, $desc:expr) => {
                AscDesc {
                    asc: Self::prepare(&session, format!($asc)).await?,
                    desc: Self::prepare(&session, format!($desc)).await?,
                }
            };
        }

        Ok(Self {
            // -- s_kv (history) --
            // Clustering: (current_account_id, key, block_height, order_id)
            skv_no_key: ad!(
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? AND current_account_id = ? LIMIT ?",
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? AND current_account_id = ? {SKV_DESC} LIMIT ?"
            ),
            skv_no_key_page: ad!(
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? \
                 AND (current_account_id, key, block_height, order_id) > (?, ?, ?, ?) \
                 AND (current_account_id) <= (?) LIMIT ?",
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? \
                 AND (current_account_id, key, block_height, order_id) < (?, ?, ?, ?) \
                 AND (current_account_id) >= (?) {SKV_DESC} LIMIT ?"
            ),
            skv_exact_key: ad!(
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? AND current_account_id = ? AND key = ? LIMIT ?",
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? AND current_account_id = ? AND key = ? {SKV_KEY_DESC} LIMIT ?"
            ),
            skv_exact_key_page: ad!(
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? \
                 AND (current_account_id, key, block_height, order_id) > (?, ?, ?, ?) \
                 AND (current_account_id, key) <= (?, ?) LIMIT ?",
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? \
                 AND (current_account_id, key, block_height, order_id) < (?, ?, ?, ?) \
                 AND (current_account_id, key) >= (?, ?) {SKV_DESC} LIMIT ?"
            ),
            skv_prefix: ad!(
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? AND current_account_id = ? \
                 AND key >= ? AND key < ? LIMIT ?",
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? AND current_account_id = ? \
                 AND key >= ? AND key < ? {SKV_DESC} LIMIT ?"
            ),
            skv_prefix_page: ad!(
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? \
                 AND (current_account_id, key, block_height, order_id) > (?, ?, ?, ?) \
                 AND (current_account_id, key) < (?, ?) LIMIT ?",
                "SELECT {ALL_COLS} FROM s_kv WHERE predecessor_id = ? \
                 AND (current_account_id, key, block_height, order_id) < (?, ?, ?, ?) \
                 AND (current_account_id, key) >= (?, ?) {SKV_DESC} LIMIT ?"
            ),

            // -- s_kv_last (latest, no ordering needed) --
            skv_last_no_key: q!(
                "SELECT {ALL_COLS} FROM s_kv_last WHERE predecessor_id = ? AND current_account_id = ? LIMIT ?"
            ),
            skv_last_no_key_page: q!(
                "SELECT {ALL_COLS} FROM s_kv_last WHERE predecessor_id = ? AND current_account_id = ? \
                 AND key > ? LIMIT ?"
            ),
            skv_last_exact_key: q!(
                "SELECT {ALL_COLS} FROM s_kv_last WHERE predecessor_id = ? AND current_account_id = ? AND key = ?"
            ),
            skv_last_prefix: q!(
                "SELECT {ALL_COLS} FROM s_kv_last WHERE predecessor_id = ? AND current_account_id = ? \
                 AND key >= ? AND key < ? LIMIT ?"
            ),
            skv_last_prefix_page: q!(
                "SELECT {ALL_COLS} FROM s_kv_last WHERE predecessor_id = ? AND current_account_id = ? \
                 AND key > ? AND key < ? LIMIT ?"
            ),

            // -- mv_kv_cur_key (history by account) --
            // Clustering: (key, block_height, order_id, predecessor_id)
            mv_cur_no_key: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? {MV_CUR_DESC} LIMIT ?"
            ),
            mv_cur_no_key_page: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND (key, block_height, order_id, predecessor_id) > (?, ?, ?, ?) LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND (key, block_height, order_id, predecessor_id) < (?, ?, ?, ?) {MV_CUR_DESC} LIMIT ?"
            ),
            mv_cur_exact_key: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? AND key = ? LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? AND key = ? \
                 ORDER BY block_height DESC, order_id DESC, predecessor_id DESC LIMIT ?"
            ),
            mv_cur_exact_key_page: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND (key, block_height, order_id, predecessor_id) > (?, ?, ?, ?) \
                 AND (key) <= (?) LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND (key, block_height, order_id, predecessor_id) < (?, ?, ?, ?) \
                 AND (key) >= (?) {MV_CUR_DESC} LIMIT ?"
            ),
            mv_cur_prefix: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND key >= ? AND key < ? LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND key >= ? AND key < ? {MV_CUR_DESC} LIMIT ?"
            ),
            mv_cur_prefix_page: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND (key, block_height, order_id, predecessor_id) > (?, ?, ?, ?) \
                 AND (key) < (?) LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_cur_key WHERE current_account_id = ? \
                 AND (key, block_height, order_id, predecessor_id) < (?, ?, ?, ?) \
                 AND (key) >= (?) {MV_CUR_DESC} LIMIT ?"
            ),

            // -- mv_kv_last_cur_key (latest, no ordering) --
            mv_last_cur_no_key: q!(
                "SELECT {ALL_COLS} FROM mv_kv_last_cur_key WHERE current_account_id = ? LIMIT ?"
            ),
            mv_last_cur_no_key_page: q!(
                "SELECT {ALL_COLS} FROM mv_kv_last_cur_key WHERE current_account_id = ? \
                 AND (key, predecessor_id) > (?, ?) LIMIT ?"
            ),
            mv_last_cur_exact_key: q!(
                "SELECT {ALL_COLS} FROM mv_kv_last_cur_key WHERE current_account_id = ? AND key = ? LIMIT ?"
            ),
            mv_last_cur_exact_key_page: q!(
                "SELECT {ALL_COLS} FROM mv_kv_last_cur_key WHERE current_account_id = ? AND key = ? \
                 AND predecessor_id > ? LIMIT ?"
            ),
            mv_last_cur_prefix: q!(
                "SELECT {ALL_COLS} FROM mv_kv_last_cur_key WHERE current_account_id = ? \
                 AND key >= ? AND key < ? LIMIT ?"
            ),
            mv_last_cur_prefix_page: q!(
                "SELECT {ALL_COLS} FROM mv_kv_last_cur_key WHERE current_account_id = ? \
                 AND (key, predecessor_id) > (?, ?) AND (key) < (?) LIMIT ?"
            ),

            // -- mv_kv_key (global by key) --
            // Clustering: (block_height, order_id, predecessor_id, current_account_id)
            mv_key: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_key WHERE key = ? LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_key WHERE key = ? {MV_KEY_DESC} LIMIT ?"
            ),
            mv_key_page: ad!(
                "SELECT {ALL_COLS} FROM mv_kv_key WHERE key = ? \
                 AND (block_height, order_id, predecessor_id, current_account_id) > (?, ?, ?, ?) LIMIT ?",
                "SELECT {ALL_COLS} FROM mv_kv_key WHERE key = ? \
                 AND (block_height, order_id, predecessor_id, current_account_id) < (?, ?, ?, ?) {MV_KEY_DESC} LIMIT ?"
            ),

            scylla_session: session,
        })
    }

    fn collect_rows(
        rows_result: scylla::response::query_result::QueryRowsResult,
    ) -> anyhow::Result<Vec<KvRow>> {
        rows_result
            .rows::<KvRow>()?
            .map(|r| r.map_err(|e| anyhow::anyhow!("{e}")))
            .collect()
    }

    // ---- s_kv (history by predecessor+account) ----

    pub async fn query_kv_by_predecessor(
        &self,
        predecessor_id: &str,
        current_account_id: &str,
        key_filter: KeyFilter<'_>,
        page: Option<SkVPage>,
        limit: i32,
        desc: bool,
    ) -> anyhow::Result<Vec<KvRow>> {
        let rows = match (&key_filter, &page) {
            (KeyFilter::None, None) => {
                self.scylla_session
                    .execute_unpaged(
                        self.skv_no_key.pick(desc),
                        (predecessor_id, current_account_id, limit),
                    )
                    .await?
            }
            (KeyFilter::None, Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        self.skv_no_key_page.pick(desc),
                        (
                            predecessor_id,
                            current_account_id,
                            &p.key,
                            p.block_height,
                            p.order_id,
                            current_account_id,
                            limit,
                        ),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), None) => {
                self.scylla_session
                    .execute_unpaged(
                        self.skv_exact_key.pick(desc),
                        (predecessor_id, current_account_id, *key, limit),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        self.skv_exact_key_page.pick(desc),
                        (
                            predecessor_id,
                            current_account_id,
                            *key,
                            p.block_height,
                            p.order_id,
                            current_account_id,
                            *key,
                            limit,
                        ),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), None) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        self.skv_prefix.pick(desc),
                        (predecessor_id, current_account_id, *prefix, &end, limit),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), Some(p)) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        self.skv_prefix_page.pick(desc),
                        (
                            predecessor_id,
                            current_account_id,
                            &p.key,
                            p.block_height,
                            p.order_id,
                            current_account_id,
                            &end,
                            limit,
                        ),
                    )
                    .await?
            }
        };
        Self::collect_rows(rows.into_rows_result()?)
    }

    // ---- s_kv_last (latest by predecessor+account) ----

    pub async fn query_kv_latest_by_predecessor(
        &self,
        predecessor_id: &str,
        current_account_id: &str,
        key_filter: KeyFilter<'_>,
        page: Option<SkVLastPage>,
        limit: i32,
    ) -> anyhow::Result<Vec<KvRow>> {
        let rows = match (&key_filter, &page) {
            (KeyFilter::None, None) => {
                self.scylla_session
                    .execute_unpaged(
                        &self.skv_last_no_key,
                        (predecessor_id, current_account_id, limit),
                    )
                    .await?
            }
            (KeyFilter::None, Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        &self.skv_last_no_key_page,
                        (predecessor_id, current_account_id, &p.key, limit),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), _) => {
                self.scylla_session
                    .execute_unpaged(
                        &self.skv_last_exact_key,
                        (predecessor_id, current_account_id, *key),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), None) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        &self.skv_last_prefix,
                        (predecessor_id, current_account_id, *prefix, &end, limit),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), Some(p)) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        &self.skv_last_prefix_page,
                        (predecessor_id, current_account_id, &p.key, &end, limit),
                    )
                    .await?
            }
        };
        Self::collect_rows(rows.into_rows_result()?)
    }

    // ---- mv_kv_cur_key (history by account) ----

    pub async fn query_kv_by_account(
        &self,
        current_account_id: &str,
        key_filter: KeyFilter<'_>,
        page: Option<MvCurKeyPage>,
        limit: i32,
        desc: bool,
    ) -> anyhow::Result<Vec<KvRow>> {
        let rows = match (&key_filter, &page) {
            (KeyFilter::None, None) => {
                self.scylla_session
                    .execute_unpaged(self.mv_cur_no_key.pick(desc), (current_account_id, limit))
                    .await?
            }
            (KeyFilter::None, Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        self.mv_cur_no_key_page.pick(desc),
                        (
                            current_account_id,
                            &p.key,
                            p.block_height,
                            p.order_id,
                            &p.predecessor_id,
                            limit,
                        ),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), None) => {
                self.scylla_session
                    .execute_unpaged(
                        self.mv_cur_exact_key.pick(desc),
                        (current_account_id, *key, limit),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        self.mv_cur_exact_key_page.pick(desc),
                        (
                            current_account_id,
                            *key,
                            p.block_height,
                            p.order_id,
                            &p.predecessor_id,
                            *key,
                            limit,
                        ),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), None) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        self.mv_cur_prefix.pick(desc),
                        (current_account_id, *prefix, &end, limit),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), Some(p)) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        self.mv_cur_prefix_page.pick(desc),
                        (
                            current_account_id,
                            &p.key,
                            p.block_height,
                            p.order_id,
                            &p.predecessor_id,
                            &end,
                            limit,
                        ),
                    )
                    .await?
            }
        };
        Self::collect_rows(rows.into_rows_result()?)
    }

    // ---- mv_kv_last_cur_key (latest by account) ----

    pub async fn query_kv_latest_by_account(
        &self,
        current_account_id: &str,
        key_filter: KeyFilter<'_>,
        page: Option<MvLastCurKeyPage>,
        limit: i32,
    ) -> anyhow::Result<Vec<KvRow>> {
        let rows = match (&key_filter, &page) {
            (KeyFilter::None, None) => {
                self.scylla_session
                    .execute_unpaged(&self.mv_last_cur_no_key, (current_account_id, limit))
                    .await?
            }
            (KeyFilter::None, Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        &self.mv_last_cur_no_key_page,
                        (current_account_id, &p.key, &p.predecessor_id, limit),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), None) => {
                self.scylla_session
                    .execute_unpaged(
                        &self.mv_last_cur_exact_key,
                        (current_account_id, *key, limit),
                    )
                    .await?
            }
            (KeyFilter::Exact(key), Some(p)) => {
                self.scylla_session
                    .execute_unpaged(
                        &self.mv_last_cur_exact_key_page,
                        (current_account_id, *key, &p.predecessor_id, limit),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), None) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        &self.mv_last_cur_prefix,
                        (current_account_id, *prefix, &end, limit),
                    )
                    .await?
            }
            (KeyFilter::Prefix(prefix), Some(p)) => {
                let end = prefix_end(prefix);
                self.scylla_session
                    .execute_unpaged(
                        &self.mv_last_cur_prefix_page,
                        (current_account_id, &p.key, &p.predecessor_id, &end, limit),
                    )
                    .await?
            }
        };
        Self::collect_rows(rows.into_rows_result()?)
    }

    // ---- mv_kv_key (global by key) ----

    pub async fn query_kv_by_key(
        &self,
        key: &str,
        page: Option<MvKeyPage>,
        limit: i32,
        desc: bool,
    ) -> anyhow::Result<Vec<KvRow>> {
        let rows = match page {
            None => {
                self.scylla_session
                    .execute_unpaged(self.mv_key.pick(desc), (key, limit))
                    .await?
            }
            Some(p) => {
                self.scylla_session
                    .execute_unpaged(
                        self.mv_key_page.pick(desc),
                        (
                            key,
                            p.block_height,
                            p.order_id,
                            &p.predecessor_id,
                            &p.current_account_id,
                            limit,
                        ),
                    )
                    .await?
            }
        };
        Self::collect_rows(rows.into_rows_result()?)
    }

    // ---- multi-key lookup (latest) ----

    pub async fn query_kv_multi(
        &self,
        keys: &[(&str, &str, &str)],
    ) -> anyhow::Result<Vec<Option<KvRow>>> {
        let futures: Vec<_> = keys
            .iter()
            .map(|(predecessor_id, current_account_id, key)| async move {
                let result = self
                    .scylla_session
                    .execute_unpaged(
                        &self.skv_last_exact_key,
                        (*predecessor_id, *current_account_id, *key),
                    )
                    .await?;
                let rows = result.into_rows_result()?;
                let row = rows.rows::<KvRow>()?.next().transpose()?;
                Ok::<_, anyhow::Error>(row)
            })
            .collect();

        futures::future::join_all(futures)
            .await
            .into_iter()
            .collect()
    }
}

fn prefix_end(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    if let Some(last) = bytes.last_mut() {
        *last += 1;
    } else {
        bytes.push(0x01);
    }
    String::from_utf8(bytes).expect("prefix_end produced invalid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_end() {
        assert_eq!(prefix_end("settings/"), "settings0");
        assert_eq!(prefix_end("a"), "b");
        assert_eq!(prefix_end(""), "\x01");
    }

    /// Integration tests requiring a local ScyllaDB with tables created by kv-sub-indexer.
    ///
    /// Setup:
    ///   docker compose -f ../fastdata-indexer/docker-compose.yml up -d
    ///   cargo test -p kv-sub-indexer -- --ignored   # creates tables in fastdata_testnet
    ///
    /// Run:
    ///   cargo test -- --ignored
    mod integration {
        use super::*;

        async fn setup() -> ScyllaDb {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node("localhost:9042")
                .build()
                .await
                .expect("Failed to connect to ScyllaDB at localhost:9042");

            let db = ScyllaDb::new("testnet", session)
                .await
                .expect("Failed to init ScyllaDb — run kv-sub-indexer tests first to create tables");

            let insert_kv = "INSERT INTO s_kv (receipt_id, action_index, tx_hash, signer_id, \
                predecessor_id, current_account_id, block_height, block_timestamp, \
                shard_id, receipt_index, order_id, key, value) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let insert_last = "INSERT INTO s_kv_last (receipt_id, action_index, tx_hash, signer_id, \
                predecessor_id, current_account_id, block_height, block_timestamp, \
                shard_id, receipt_index, order_id, key, value) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

            let rows: Vec<(
                &str, i32, Option<&str>, &str, &str, &str,
                i64, i64, i32, i32, i64, &str, &str,
            )> = vec![
                ("r1", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 1, "t/settings/volume", "\"80\""),
                ("r2", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 2, "t/settings/lang", "\"en\""),
                ("r3", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 3, "t/score", "\"1500\""),
                ("r4", 0, None, "t-bob.near", "t-bob.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 4, "t/score", "\"2400\""),
                ("r5", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9001, 1_000_000_000, 0, 0, 1, "t/score", "\"1800\""),
            ];

            for r in &rows {
                db.scylla_session
                    .query_unpaged(insert_kv, r.clone())
                    .await
                    .expect("Failed to insert into s_kv");
            }

            let latest: Vec<(
                &str, i32, Option<&str>, &str, &str, &str,
                i64, i64, i32, i32, i64, &str, &str, i64,
            )> = vec![
                ("r1", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 1, "t/settings/volume", "\"80\"",
                 9000 * 1_000_000 + 1),
                ("r2", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 2, "t/settings/lang", "\"en\"",
                 9000 * 1_000_000 + 2),
                ("r5", 0, None, "t-alice.near", "t-alice.near", "t-game.near",
                 9001, 1_000_000_000, 0, 0, 1, "t/score", "\"1800\"",
                 9001 * 1_000_000 + 1),
                ("r4", 0, None, "t-bob.near", "t-bob.near", "t-game.near",
                 9000, 1_000_000_000, 0, 0, 4, "t/score", "\"2400\"",
                 9000 * 1_000_000 + 4),
            ];

            for r in &latest {
                db.scylla_session
                    .query_unpaged(insert_last, r.clone())
                    .await
                    .expect("Failed to insert into s_kv_last");
            }

            db
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_predecessor_no_filter() {
            let db = setup().await;
            let rows = db
                .query_kv_by_predecessor("t-alice.near", "t-game.near", KeyFilter::None, None, 50, false)
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_predecessor_exact_key() {
            let db = setup().await;
            let rows = db
                .query_kv_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::Exact("t/score"), None, 50, false,
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_predecessor_prefix() {
            let db = setup().await;
            let rows = db
                .query_kv_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::Prefix("t/settings/"), None, 50, false,
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_predecessor_desc() {
            let db = setup().await;
            // DESC: newest first
            let rows = db
                .query_kv_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::Exact("t/score"), None, 50, true,
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            // First row should be block 9001 (newest)
            assert_eq!(rows[0].block_height, 9001);
            assert_eq!(rows[1].block_height, 9000);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_latest_by_predecessor() {
            let db = setup().await;
            let rows = db
                .query_kv_latest_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::Exact("t/score"), None, 50,
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].value, "\"1800\"");
            assert_eq!(rows[0].block_height, 9001);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_latest_by_predecessor_all_keys() {
            let db = setup().await;
            let rows = db
                .query_kv_latest_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::None, None, 50,
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_account() {
            let db = setup().await;
            let rows = db
                .query_kv_by_account("t-game.near", KeyFilter::None, None, 50, false)
                .await
                .unwrap();
            assert_eq!(rows.len(), 5);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_account_prefix() {
            let db = setup().await;
            let rows = db
                .query_kv_by_account("t-game.near", KeyFilter::Prefix("t/settings/"), None, 50, false)
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_latest_by_account() {
            let db = setup().await;
            let rows = db
                .query_kv_latest_by_account("t-game.near", KeyFilter::None, None, 50)
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_by_key_global() {
            let db = setup().await;
            let rows = db.query_kv_by_key("t/score", None, 50, false).await.unwrap();
            assert_eq!(rows.len(), 3);
        }

        #[tokio::test]
        #[ignore]
        async fn test_query_multi() {
            let db = setup().await;
            let results = db
                .query_kv_multi(&[
                    ("t-alice.near", "t-game.near", "t/score"),
                    ("t-bob.near", "t-game.near", "t/score"),
                    ("t-alice.near", "t-game.near", "t/nonexistent"),
                ])
                .await
                .unwrap();
            assert_eq!(results.len(), 3);
            assert!(results[0].is_some());
            assert_eq!(results[0].as_ref().unwrap().value, "\"1800\"");
            assert!(results[1].is_some());
            assert_eq!(results[1].as_ref().unwrap().value, "\"2400\"");
            assert!(results[2].is_none());
        }

        #[tokio::test]
        #[ignore]
        async fn test_pagination_by_predecessor() {
            let db = setup().await;
            let page1 = db
                .query_kv_by_predecessor("t-alice.near", "t-game.near", KeyFilter::None, None, 2, false)
                .await
                .unwrap();
            assert_eq!(page1.len(), 2);

            let last = page1.last().unwrap();
            let page2 = db
                .query_kv_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::None,
                    Some(SkVPage {
                        key: last.key.clone(),
                        block_height: last.block_height,
                        order_id: last.order_id,
                    }),
                    2, false,
                )
                .await
                .unwrap();
            assert_eq!(page2.len(), 2);

            let last2 = page2.last().unwrap();
            let page3 = db
                .query_kv_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::None,
                    Some(SkVPage {
                        key: last2.key.clone(),
                        block_height: last2.block_height,
                        order_id: last2.order_id,
                    }),
                    2, false,
                )
                .await
                .unwrap();
            assert!(page3.is_empty());
        }

        #[tokio::test]
        #[ignore]
        async fn test_pagination_latest_by_predecessor() {
            let db = setup().await;
            let page1 = db
                .query_kv_latest_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::None, None, 2,
                )
                .await
                .unwrap();
            assert_eq!(page1.len(), 2);

            let last = page1.last().unwrap();
            let page2 = db
                .query_kv_latest_by_predecessor(
                    "t-alice.near", "t-game.near", KeyFilter::None,
                    Some(SkVLastPage { key: last.key.clone() }),
                    2,
                )
                .await
                .unwrap();
            assert_eq!(page2.len(), 1);
        }
    }
}
