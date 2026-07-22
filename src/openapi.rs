use std::path::PathBuf;

use anyhow::Result;
use fastnear_openapi_generator::{
    build_service_doc, write_or_check_yaml, AggregateOperationSpec, ApiInfo, ApiServer,
    HttpMethod, ParameterLocation, ParameterSpec, RequestBodySpec, ResponseContent, ResponseSpec,
    SchemaRegistry,
};
use schemars::JsonSchema;
use serde_json::{json, Value};

use crate::types::{
    AllRequest, ByKeyRequest, ErrorResponse, LatestRequest, ListResponse, MultiRequest,
    MultiResponse, QueryRequest,
};

const API_VERSION: &str = "3.0.3";
const SERVICE_INFO: ApiInfo<'static> = ApiInfo {
    title: "KV FastData API",
    version: API_VERSION,
    description: "Read-only key-value queries over FastData records stored in ScyllaDB.",
    servers: &[
        ApiServer {
            url: "https://kv.main.fastnear.com",
            description: "Mainnet",
        },
        ApiServer {
            url: "https://kv.test.fastnear.com",
            description: "Testnet",
        },
    ],
};

pub fn generate(check: bool) -> Result<()> {
    let output_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("openapi");
    let mut registry = SchemaRegistry::openapi3();

    let operations = vec![
        post_operation_spec::<QueryRequest, ListResponse>(
            &mut registry,
            "history_by_predecessor",
            "KV FastData API - History by Predecessor",
            "/v0/history/{current_account_id}/{predecessor_id}",
            "history_by_predecessor",
            "Fetch historical key-value writes for one predecessor and contract",
            "Fetch historical FastData rows for a single predecessor and target account.",
            &["history"],
            vec![
                account_id_param(
                    "current_account_id",
                    "Contract account whose FastData keys were written.",
                    "social.near",
                ),
                account_id_param(
                    "predecessor_id",
                    "Calling account that wrote the FastData keys.",
                    "james.near",
                ),
            ],
            json!({
                "key_prefix": "graph/follow/",
                "limit": 50,
                "include_metadata": true,
                "asc": false
            }),
            "Historical rows for the selected predecessor and account",
            Some(list_response_example()),
        ),
        post_operation_spec::<LatestRequest, ListResponse>(
            &mut registry,
            "latest_by_predecessor",
            "KV FastData API - Latest by Predecessor",
            "/v0/latest/{current_account_id}/{predecessor_id}",
            "latest_by_predecessor",
            "Fetch latest key-value rows for one predecessor and contract",
            "Fetch the latest FastData rows for a single predecessor and target account.",
            &["latest"],
            vec![
                account_id_param(
                    "current_account_id",
                    "Contract account whose FastData keys were written.",
                    "social.near",
                ),
                account_id_param(
                    "predecessor_id",
                    "Calling account that wrote the FastData keys.",
                    "james.near",
                ),
            ],
            json!({
                "key_prefix": "graph/follow/",
                "limit": 50,
                "include_metadata": true
            }),
            "Latest rows for the selected predecessor and account",
            Some(list_response_example()),
        ),
        post_operation_spec::<QueryRequest, ListResponse>(
            &mut registry,
            "history_by_account",
            "KV FastData API - History by Account",
            "/v0/history/{current_account_id}",
            "history_by_account",
            "Fetch historical key-value writes for one contract across all predecessors",
            "Fetch historical FastData rows for one target account across all predecessor accounts.",
            &["history"],
            vec![account_id_param(
                "current_account_id",
                "Contract account whose FastData keys were written.",
                "social.near",
            )],
            json!({
                "key_prefix": "graph/follow/sleet.near",
                "limit": 50,
                "include_metadata": true,
                "asc": false
            }),
            "Historical rows for the selected contract",
            Some(list_response_example()),
        ),
        post_operation_spec::<LatestRequest, ListResponse>(
            &mut registry,
            "latest_by_account",
            "KV FastData API - Latest by Account",
            "/v0/latest/{current_account_id}",
            "latest_by_account",
            "Fetch latest key-value rows for one contract across all predecessors",
            "Fetch the latest FastData rows for one target account across all predecessor accounts.",
            &["latest"],
            vec![account_id_param(
                "current_account_id",
                "Contract account whose FastData keys were written.",
                "social.near",
            )],
            json!({
                "key_prefix": "graph/follow/",
                "limit": 50,
                "include_metadata": true
            }),
            "Latest rows for the selected contract",
            Some(list_response_example()),
        ),
        post_operation_spec::<AllRequest, ListResponse>(
            &mut registry,
            "all_by_predecessor",
            "KV FastData API - All Latest Keys by Predecessor",
            "/v0/all/{predecessor_id}",
            "all_by_predecessor",
            "Fetch latest key-value rows for one predecessor across all contracts",
            "Fetch the latest values for all contracts touched by one predecessor account.",
            &["latest"],
            vec![account_id_param(
                "predecessor_id",
                "Calling account that wrote the FastData keys.",
                "james.near",
            )],
            json!({
                "limit": 50,
                "include_metadata": true
            }),
            "Latest rows for the selected predecessor",
            Some(list_response_example()),
        ),
        post_operation_spec::<ByKeyRequest, ListResponse>(
            &mut registry,
            "history_by_key",
            "KV FastData API - History by Global Key",
            "/v0/history",
            "history_by_key",
            "Fetch historical rows by exact key across all indexed contracts",
            "Fetch the historical values for a selected key across accounts and predecessors.",
            &["history"],
            vec![],
            json!({
                "key": "graph/follow/sleet.near",
                "limit": 50,
                "include_metadata": false,
                "asc": true
            }),
            "Historical rows for the selected key",
            Some(list_response_example()),
        ),
        post_operation_spec::<MultiRequest, MultiResponse>(
            &mut registry,
            "multi",
            "KV FastData API - Multi-Key Lookup",
            "/v0/multi",
            "multi",
            "Fetch the latest rows for multiple fully qualified keys",
            "Fetch the latest FastData row for up to 100 keys in a single request.",
            &["latest"],
            vec![],
            json!({
                "keys": [
                    "social.near/james.near/graph/follow/sleet.near",
                    "social.near/james.near/graph/follow/missing"
                ],
                "include_metadata": true
            }),
            "Latest rows for the requested keys",
            Some(multi_response_example()),
        ),
        get_operation_spec::<ListResponse>(
            &mut registry,
            "get_history_key",
            "KV FastData API - History by Exact Key",
            "/v0/history/{current_account_id}/{predecessor_id}/{key}",
            "get_history_key",
            "Fetch historical rows for one exact key under one predecessor and contract",
            "Fetch every historical write for one exact key under one predecessor and contract.",
            &["history"],
            vec![
                account_id_param(
                    "current_account_id",
                    "Contract account whose FastData keys were written.",
                    "social.near",
                ),
                account_id_param(
                    "predecessor_id",
                    "Calling account that wrote the FastData keys.",
                    "james.near",
                ),
                key_param(),
            ],
            "Historical rows for the selected predecessor, account, and key",
            Some(list_response_example()),
        ),
        get_operation_spec::<ListResponse>(
            &mut registry,
            "get_latest_key",
            "KV FastData API - Latest by Exact Key",
            "/v0/latest/{current_account_id}/{predecessor_id}/{key}",
            "get_latest_key",
            "Fetch the latest row for one exact key under one predecessor and contract",
            "Fetch the latest value for one exact key under one predecessor and contract.",
            &["latest"],
            vec![
                account_id_param(
                    "current_account_id",
                    "Contract account whose FastData keys were written.",
                    "social.near",
                ),
                account_id_param(
                    "predecessor_id",
                    "Calling account that wrote the FastData keys.",
                    "james.near",
                ),
                key_param(),
            ],
            "Latest rows for the selected predecessor, account, and key",
            Some(list_response_example()),
        ),
    ];

    let components = registry.into_components();
    let service_doc = build_service_doc(&SERVICE_INFO, operations, components);
    write_or_check_yaml(output_root.join("openapi.yaml"), &service_doc, check)?;
    Ok(())
}

fn post_operation_spec<Request, Response>(
    registry: &mut SchemaRegistry,
    slug: &'static str,
    title: &'static str,
    path: &'static str,
    operation_id: &'static str,
    summary: &'static str,
    description: &'static str,
    tags: &'static [&'static str],
    parameters: Vec<ParameterSpec<'static>>,
    request_example: Value,
    ok_description: &'static str,
    ok_example: Option<Value>,
) -> AggregateOperationSpec<'static>
where
    Request: JsonSchema,
    Response: JsonSchema,
{
    let request = registry.schema_ref::<Request>();
    let response = registry.schema_ref::<Response>();
    let api_error = registry.schema_ref::<ErrorResponse>();

    AggregateOperationSpec {
        slug,
        title,
        path,
        method: HttpMethod::Post,
        operation_id,
        summary,
        description,
        tags,
        parameters,
        request_body: Some(RequestBodySpec::Json {
            schema: request,
            required: true,
            example: Some(request_example),
            examples: vec![],
        }),
        responses: vec![
            ResponseSpec {
                status: "200",
                description: ok_description,
                content: Some(ResponseContent::Json {
                    schema: response,
                    example: ok_example,
                    examples: vec![],
                }),
            },
            ResponseSpec {
                status: "400",
                description: "Invalid key filter or page token",
                content: Some(ResponseContent::Json {
                    schema: api_error.clone(),
                    example: Some(json!({ "error": "invalid page token" })),
                    examples: vec![],
                }),
            },
            ResponseSpec {
                status: "500",
                description: "Scylla query failure",
                content: Some(ResponseContent::Json {
                    schema: api_error,
                    example: Some(json!({ "error": "internal server error" })),
                    examples: vec![],
                }),
            },
        ],
    }
}

fn get_operation_spec<Response>(
    registry: &mut SchemaRegistry,
    slug: &'static str,
    title: &'static str,
    path: &'static str,
    operation_id: &'static str,
    summary: &'static str,
    description: &'static str,
    tags: &'static [&'static str],
    parameters: Vec<ParameterSpec<'static>>,
    ok_description: &'static str,
    ok_example: Option<Value>,
) -> AggregateOperationSpec<'static>
where
    Response: JsonSchema,
{
    let response = registry.schema_ref::<Response>();
    let api_error = registry.schema_ref::<ErrorResponse>();

    AggregateOperationSpec {
        slug,
        title,
        path,
        method: HttpMethod::Get,
        operation_id,
        summary,
        description,
        tags,
        parameters,
        request_body: None,
        responses: vec![
            ResponseSpec {
                status: "200",
                description: ok_description,
                content: Some(ResponseContent::Json {
                    schema: response,
                    example: ok_example,
                    examples: vec![],
                }),
            },
            ResponseSpec {
                status: "500",
                description: "Scylla query failure",
                content: Some(ResponseContent::Json {
                    schema: api_error,
                    example: Some(json!({ "error": "internal server error" })),
                    examples: vec![],
                }),
            },
        ],
    }
}

fn account_id_param(
    name: &'static str,
    description: &'static str,
    example: &'static str,
) -> ParameterSpec<'static> {
    ParameterSpec {
        name,
        location: ParameterLocation::Path,
        required: true,
        description,
        schema: json!({ "type": "string" }),
        example: Some(json!(example)),
    }
}

fn key_param() -> ParameterSpec<'static> {
    ParameterSpec {
        name: "key",
        location: ParameterLocation::Path,
        required: true,
        description: "Exact FastData key to return.",
        schema: json!({ "type": "string" }),
        example: Some(json!("graph/follow/sleet.near")),
    }
}

fn list_response_example() -> Value {
    json!({
        "entries": [
            {
                "receipt_id": "gaiLdGpaRwaunXUFnnz9VNj8V7cY18ZPAt2QfZazBkk",
                "action_index": 0,
                "tx_hash": "FK7qDhHv4otg2wPGrE3DKFjvmmjHhUKBxQDkcAsdH5k3",
                "signer_id": "james.near",
                "predecessor_id": "james.near",
                "current_account_id": "social.near",
                "block_height": 183302718,
                "block_timestamp": 1769731630602682599_i64,
                "key": "graph/follow/sleet.near",
                "value": ""
            }
        ],
        "page_token": "opaque-next-page-token"
    })
}

fn multi_response_example() -> Value {
    json!({
        "entries": [
            {
                "receipt_id": "gaiLdGpaRwaunXUFnnz9VNj8V7cY18ZPAt2QfZazBkk",
                "action_index": 0,
                "tx_hash": "FK7qDhHv4otg2wPGrE3DKFjvmmjHhUKBxQDkcAsdH5k3",
                "signer_id": "james.near",
                "predecessor_id": "james.near",
                "current_account_id": "social.near",
                "block_height": 183302718,
                "block_timestamp": 1769731630602682599_i64,
                "key": "graph/follow/sleet.near",
                "value": ""
            },
            null
        ]
    })
}

#[cfg(test)]
mod tests {
    use fastnear_openapi_generator::SchemaRegistry;

    use crate::types::{
        AllRequest, KvEntry, LatestRequest, MultiRequest, MultiResponse, QueryRequest,
    };

    #[test]
    fn history_and_latest_request_schemas_only_expose_supported_fields() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<QueryRequest>();
        registry.schema_ref::<LatestRequest>();
        registry.schema_ref::<AllRequest>();
        let components = registry.into_components();

        assert!(components["QueryRequest"]["properties"]["asc"].is_object());
        assert!(components["LatestRequest"]["properties"]["asc"].is_null());
        assert!(components["AllRequest"]["properties"]["key"].is_null());
        assert!(components["AllRequest"]["properties"]["key_prefix"].is_null());
    }

    #[test]
    fn multi_request_schema_limits_keys_to_one_hundred() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<MultiRequest>();
        let components = registry.into_components();
        let keys = &components["MultiRequest"]["properties"]["keys"];

        assert_eq!(keys["type"], "array");
        assert_eq!(keys["maxItems"], 100);
        assert_eq!(keys["items"]["type"], "string");
    }

    #[test]
    fn kv_entry_value_is_modeled_as_free_form_json() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<KvEntry>();
        let components = registry.into_components();
        let value = &components["KvEntry"]["properties"]["value"];

        assert!(value.get("type").is_none());
        assert_eq!(
            value["description"],
            "Raw JSON value as stored in FastData."
        );
    }

    #[test]
    fn multi_response_entries_allow_null_items() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<MultiResponse>();
        let components = registry.into_components();
        let items = &components["MultiResponse"]["properties"]["entries"]["items"];

        assert!(items["oneOf"].is_array());
        assert_eq!(items["oneOf"][0]["type"], "object");
        assert!(items["oneOf"][0]["properties"].is_object());
        assert!(items["oneOf"][1]["enum"][0].is_null());
    }
}
