# kv-fastdata-server

Read-only JSON API server for querying Key-Value data indexed by [kv-sub-indexer](https://github.com/fastnear/fastdata-indexer) into ScyllaDB.

## Setup

```bash
cp .env.example .env
# Edit .env with your ScyllaDB connection details
cargo build --release
./target/release/kv-fastdata-server
```

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CHAIN_ID` | Yes | | NEAR chain id (`mainnet`, `testnet`) |
| `PORT` | No | `3010` | HTTP port |
| `WEB_HOSTNAME` | No | | Public hostname for documentation links (e.g. `https://kv.api.fastnear.com`) |
| `SCYLLA_URL` | Yes | | ScyllaDB address (e.g. `localhost:9042`) |
| `SCYLLA_SSL_CA` | No | | Path to CA cert (enables TLS) |
| `SCYLLA_SSL_CERT` | No | | Path to client cert |
| `SCYLLA_SSL_KEY` | No | | Path to client key |
| `SCYLLA_USERNAME` | No | | ScyllaDB username (enables auth) |
| `SCYLLA_PASSWORD` | No | | ScyllaDB password |

## API

### GET - Single Key Lookups

```
GET /v0/latest/{current_account_id}/{predecessor_id}/{key}
GET /v0/history/{current_account_id}/{predecessor_id}/{key}
```

### POST - Filtered Queries

```
POST /v0/history/{current_account_id}/{predecessor_id}   # history by predecessor+account
POST /v0/latest/{current_account_id}/{predecessor_id}    # latest by predecessor+account
POST /v0/history/{current_account_id}                    # history by account (all predecessors)
POST /v0/latest/{current_account_id}                     # latest by account (all predecessors)
POST /v0/history                                         # global key lookup
POST /v0/multi                                           # batch multi-key latest lookup
```

POST body supports: `key`, `key_prefix`, `limit`, `page_token`, `include_metadata`.

### Documentation

```
GET /          # Human-readable docs
GET /skill.md  # Machine-readable API reference
```

## Testing

```bash
# Unit tests
cargo test

# Integration tests (requires ScyllaDB + kv-sub-indexer tables)
docker compose -f ../fastdata-indexer/docker-compose.yml up -d
cargo test -p kv-sub-indexer --manifest-path ../fastdata-indexer/Cargo.toml -- --ignored
cargo test -- --ignored
```
