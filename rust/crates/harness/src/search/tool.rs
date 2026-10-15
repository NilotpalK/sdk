use super::{SearchProvider, SearchRequest, SearchResponse};
use crate::{
    Error, Result,
    runtime::ToolContext,
    tool::{
        BuiltinTool, JsonToolProjection, Tool, ToolDefinition, ToolEffect, ToolExecutor,
        ToolInvocation, ToolResult, ToolSpec, schema,
    },
};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

/// The web-search catalog entry; all schemas come from the actual DTOs.
pub fn spec() -> Result<ToolSpec> {
    Ok(ToolSpec {
        definition: ToolDefinition {
            name: "web_search".into(),
            revision: "1".into(),
            description: "Search the web for current information. Returns ranked source URLs, titles and snippets for citations.".into(),
            input_schema: schema::input::<SearchRequest>()?,
            output_schema: schema::output::<SearchResponse>()?,
            projection_schema: schema::ProjectionMode::Full.schema::<SearchResponse>()?,
        },
        executor: BuiltinTool::WebSearch,
        effects: vec![ToolEffect::Network],
    })
}

/// Binds an explicitly supplied backend, including a credential-free scripted backend.
pub fn web_search(provider: Arc<dyn SearchProvider>) -> Result<Tool> {
    spec()?.bind(
        Arc::new(SearchExecutor(provider)),
        Arc::new(JsonToolProjection),
    )
}

struct SearchExecutor(Arc<dyn SearchProvider>);

impl ToolExecutor for SearchExecutor {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            if invocation.name != "web_search" {
                return Err(Error::Invalid("incorrect web search tool name".into()));
            }
            let request: SearchRequest = serde_json::from_value(invocation.arguments)
                .map_err(|_| Error::Invalid("invalid web search arguments".into()))?;
            request.validate()?;
            let response = self.0.search(&request).await?;
            if response.results.len() > request.max_results as usize {
                return Err(Error::Invalid(
                    "search backend exceeded result limit".into(),
                ));
            }
            Ok(ToolResult {
                value: serde_json::to_value(response)
                    .map_err(|_| Error::Invalid("invalid web search response".into()))?,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        // A lost response cannot be recovered from either search API. Reissuing
        // would consume another request and could return a different result.
        Box::pin(async { Ok(None) })
    }
}
