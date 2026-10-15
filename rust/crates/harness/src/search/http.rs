use super::{SearchProvider, SearchRequest, SearchResponse, SearchResult};
use crate::{Error, Result};
use acyclic_stream::BoxProviderFuture;
use reqwest::{Client, Url, header::HeaderValue};
use serde::Deserialize;
use std::time::Duration;

const MAXIMUM_RESPONSE_BYTES: usize = 1_048_576;

/// Exa's typed POST /search adapter.
pub struct Exa(HttpSearch);

/// TinyFish's typed GET / adapter.
pub struct TinyFish(HttpSearch);

struct HttpSearch {
    client: Client,
    endpoint: Url,
    key: HeaderValue,
}

impl HttpSearch {
    fn new(key: &str, endpoint: &str) -> Result<Self> {
        if key.trim().is_empty() {
            return Err(Error::Invalid("search API key is empty".into()));
        }
        let mut key = HeaderValue::from_str(key)
            .map_err(|_| Error::Invalid("invalid search API key".into()))?;
        key.set_sensitive(true);
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Invalid("invalid search endpoint".into()))?;
        if !matches!(endpoint.scheme(), "https" | "http")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Invalid("invalid search endpoint".into()));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| Error::Invalid("cannot create search HTTP client".into()))?;
        Ok(Self {
            client,
            endpoint,
            key,
        })
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Vec<u8>> {
        let mut response = request
            .header("x-api-key", self.key.clone())
            .send()
            .await
            .map_err(|_| Error::Storage("search HTTP request failed".into()))?;
        if !response.status().is_success() {
            // Never expose response bodies or reqwest errors: they can reflect
            // credentials, headers, or a query-bearing URL.
            return Err(Error::Invalid(format!(
                "search HTTP status {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(Error::Invalid("search response exceeds byte limit".into()));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::Storage("cannot read search response".into()))?
        {
            if chunk.len() > MAXIMUM_RESPONSE_BYTES - bytes.len() {
                return Err(Error::Invalid("search response exceeds byte limit".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

impl Exa {
    /// Binds a host-resolved key. It is never included in tool definitions.
    pub fn new(key: &str) -> Result<Self> {
        Self::with_endpoint(key, "https://api.exa.ai/search")
    }

    /// Uses an explicit endpoint, for private gateways and local protocol tests.
    pub fn with_endpoint(key: &str, endpoint: &str) -> Result<Self> {
        HttpSearch::new(key, endpoint).map(Self)
    }
}

impl TinyFish {
    /// Binds a host-resolved key.
    pub fn new(key: &str) -> Result<Self> {
        Self::with_endpoint(key, "https://api.search.tinyfish.ai/")
    }

    /// Uses an explicit endpoint, for private gateways and local protocol tests.
    pub fn with_endpoint(key: &str, endpoint: &str) -> Result<Self> {
        HttpSearch::new(key, endpoint).map(Self)
    }
}

#[derive(Deserialize)]
struct ExaResponse {
    results: Vec<ExaResult>,
}

#[derive(Deserialize)]
struct ExaResult {
    url: String,
    title: Option<String>,
    #[serde(default)]
    highlights: Vec<String>,
}

impl SearchProvider for Exa {
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move {
            request.validate()?;
            let body = serde_json::json!({
                "query": request.query,
                "numResults": request.max_results,
                "type": "auto",
                "contents": {"highlights": true}
            });
            let bytes = self
                .0
                .send(self.0.client.post(self.0.endpoint.clone()).json(&body))
                .await?;
            let response: ExaResponse = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("invalid Exa search response".into()))?;
            let results = response
                .results
                .into_iter()
                .take(request.max_results as usize)
                .map(|item| SearchResult {
                    url: item.url,
                    title: item.title.unwrap_or_default(),
                    snippet: item.highlights.join("\n"),
                })
                .collect();
            Ok(SearchResponse { results })
        })
    }
}

#[derive(Deserialize)]
struct TinyFishResponse {
    results: Vec<TinyFishResult>,
}

#[derive(Deserialize)]
struct TinyFishResult {
    url: String,
    title: String,
    snippet: String,
}

impl SearchProvider for TinyFish {
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move {
            request.validate()?;
            let mut endpoint = self.0.endpoint.clone();
            endpoint
                .query_pairs_mut()
                .append_pair("query", &request.query);
            let bytes = self.0.send(self.0.client.get(endpoint)).await?;
            let response: TinyFishResponse = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("invalid TinyFish search response".into()))?;
            let results = response
                .results
                .into_iter()
                .take(request.max_results as usize)
                .map(|item| SearchResult {
                    url: item.url,
                    title: item.title,
                    snippet: item.snippet,
                })
                .collect();
            Ok(SearchResponse { results })
        })
    }
}

#[cfg(test)]
mod tests;
