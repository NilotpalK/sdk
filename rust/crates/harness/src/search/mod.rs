//! Provider-neutral web search, with credentials confined to the host.

use crate::{Error, Result};
use acyclic_stream::BoxProviderFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod tool;
pub use tool::{spec, web_search};
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
mod http;
#[cfg(test)]
mod tests;
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub use http::{Exa, TinyFish};
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
mod config;
#[cfg(all(feature = "search-http", not(target_arch = "wasm32")))]
pub use config::{SearchBackend, SearchConfig};

/// A bounded query, independent of a provider's wire protocol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// Natural-language query, sent verbatim to the selected provider.
    #[schemars(length(min = 1, max = 4096))]
    pub query: String,
    /// Maximum number of ranked results to return.
    #[serde(default = "default_results")]
    #[schemars(range(min = 1, max = 20))]
    pub max_results: u32,
}

fn default_results() -> u32 {
    5
}

impl SearchRequest {
    /// Validates bounds even when called outside the tool registry.
    pub fn validate(&self) -> Result<()> {
        if self.query.trim().is_empty()
            || self.query.len() > 4096
            || !(1..=20).contains(&self.max_results)
        {
            return Err(Error::Invalid(
                "invalid web search query or result limit".into(),
            ));
        }
        Ok(())
    }
}

/// One ranked source. Provider-specific metadata and credentials stay private.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchResult {
    /// Source URL for citations.
    pub url: String,
    /// Source title, empty when the provider has none.
    pub title: String,
    /// Provider-supplied excerpt.
    pub snippet: String,
}

/// Canonical successful search output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchResponse {
    /// Sources in provider relevance order.
    pub results: Vec<SearchResult>,
}

/// Replaceable search capability. Dropping its future cancels the request.
pub trait SearchProvider: acyclic_stream::ProviderPlatform {
    /// Searches once. Durable replay belongs to the harness tool journal.
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>>;
}
