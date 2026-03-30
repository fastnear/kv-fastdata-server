# KV FastData API

Base URL: `{{HOSTNAME}}`

## GET /v0/latest/{current_account_id}/{predecessor_id}/{key}

Get latest value of a specific key. Key may contain slashes.

```
GET {{HOSTNAME}}/v0/latest/app.near/alice.near/score
```

## GET /v0/history/{current_account_id}/{predecessor_id}/{key}

Get full history of a specific key.

```
GET {{HOSTNAME}}/v0/history/app.near/alice.near/score
```

## POST /v0/all/{predecessor_id}

Latest values of all keys across all contracts for a predecessor. No key filtering. Must use pagination.

```json
{
  "limit": 50,                   // optional, default 50, max 200
  "page_token": "...",            // optional
  "include_metadata": false       // optional
}
```

## POST /v0/history/{current_account_id}/{predecessor_id}

Full history of KV writes by a predecessor to a contract.

```json
{
  "key": "score",              // optional, exact key
  "key_prefix": "settings/",   // optional, mutually exclusive with key
  "limit": 50,                 // optional, default 50, max 200
  "page_token": "...",          // optional
  "include_metadata": false,     // optional, default false
  "asc": false                   // optional, default false (newest first)
}
```

## POST /v0/latest/{current_account_id}/{predecessor_id}

Latest value per key by predecessor + contract. Same body as above.

## POST /v0/history/{current_account_id}

Full history of all KV writes to a contract (all predecessors). Same body.

## POST /v0/latest/{current_account_id}

Latest value per (key, predecessor) for a contract. Same body.

## POST /v0/history

Global lookup: all writes to a key across all accounts.

```json
{
  "key": "score",               // required
  "limit": 50,                  // optional
  "page_token": "...",           // optional
  "include_metadata": false,     // optional
  "asc": false                   // optional, default false (newest first)
}
```

## POST /v0/multi

Batch latest-value lookup for multiple keys (max 100).

```json
{
  "keys": [
    "app.near/alice.near/score",
    "app.near/bob.near/level"
  ],
  "include_metadata": false,
  "asc": false
}
```

Key format: `current_account_id/predecessor_id/key` (key may contain slashes).

## Response

Default (include_metadata=false):
```json
{
  "entries": [
    {
      "predecessor_id": "alice.near",
      "current_account_id": "app.near",
      "block_height": 123456789,
      "block_timestamp": 1700000000000,
      "key": "score",
      "value": 1500
    }
  ],
  "page_token": "eyJr..."
}
```

With include_metadata=true, entries also include: `receipt_id`, `action_index`, `tx_hash`, `signer_id`.

- `value` is raw JSON (number, string, object, array) as stored.
- `tx_hash` is omitted when null.
- `page_token` is omitted when no more results.

Multi response:
```json
{
  "entries": [
    {"predecessor_id": "alice.near", "key": "score", "value": 1500, ...},
    null
  ]
}
```

Entries match input `keys` order. `null` = not found.

## Pagination

1. Send request with optional `limit`.
2. If response has `page_token`, resend same request adding that token.
3. Repeat until `page_token` is absent.

## Errors

- 400: `{"error": "description"}` - invalid request
- 500: `{"error": "Internal error"}` - server error

## Writing Data

Write data by calling `__fastdata_kv` on any NEAR account with a JSON object argument.

- The caller is `predecessor_id`, the target is `current_account_id`
- The target account does NOT need to have a contract or exist on chain
- Root-level keys of the JSON become stored keys; values are stored as-is
- Max 256 keys per call, max 1,024 bytes per key, max 256 KB per value

```bash
# Store a single value
near call my-app.near __fastdata_kv '{"score": 1500}' --accountId alice.near

# Store multiple keys
near call my-app.near __fastdata_kv '{"settings/lang": "en", "settings/volume": 80}' --accountId alice.near

# Use any account as namespace (doesn't need to exist)
near call data-store.alice.near __fastdata_kv '{"hello": "world"}' --accountId alice.near
```
