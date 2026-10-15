use crate::{
    Error, Result,
    search::{SearchProvider, SearchRequest, SearchResponse},
};
use acyclic_stream::BoxProviderFuture;
use serde::Deserialize;
use std::{collections::VecDeque, sync::Mutex};

/// One exact expected search and its response, loaded from scenario data.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptedSearch {
    /// Expected provider-neutral request.
    pub request: SearchRequest,
    /// Successful result, mutually exclusive with `error`.
    pub response: Option<SearchResponse>,
    /// Failure, mutually exclusive with `response`.
    pub error: Option<String>,
}

/// Deterministic search backend. It never reads credentials or uses the network.
pub struct ScriptedSearchProvider {
    steps: Mutex<VecDeque<ScriptedSearch>>,
}

impl ScriptedSearchProvider {
    /// Validates a finite script before any invocation.
    pub fn new(steps: impl IntoIterator<Item = ScriptedSearch>) -> Result<Self> {
        let steps: VecDeque<_> = steps.into_iter().collect();
        for step in &steps {
            step.request.validate()?;
            if step.response.is_some() == step.error.is_some()
                || step
                    .error
                    .as_ref()
                    .is_some_and(|error| error.trim().is_empty())
            {
                return Err(Error::Invalid(
                    "scripted search requires one response or error".into(),
                ));
            }
        }
        Ok(Self {
            steps: Mutex::new(steps),
        })
    }

    /// Unconsumed required requests; scenario runners must assert zero.
    pub fn remaining(&self) -> usize {
        self.steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

impl SearchProvider for ScriptedSearchProvider {
    fn search<'a>(
        &'a self,
        request: &'a SearchRequest,
    ) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move {
            request.validate()?;
            let mut steps = self
                .steps
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let step = steps
                .front()
                .ok_or_else(|| Error::Invalid("scripted search exhausted".into()))?;
            if &step.request != request {
                return Err(Error::Conflict("scripted search request mismatch".into()));
            }
            let step = steps
                .pop_front()
                .ok_or_else(|| Error::Invalid("scripted search exhausted".into()))?;
            match (step.response, step.error) {
                (Some(response), None) => Ok(response),
                (None, Some(error)) => Err(Error::Invalid(error)),
                _ => Err(Error::Invalid("invalid scripted search step".into())),
            }
        })
    }
}
