//! Data-driven model → search → model scenarios with durable result replay.

use acyclic_harness::{
    AgentId, Capabilities, OperationId,
    context::{CompactionPolicy, ContextPipeline},
    conversation::Limits,
    executor::{ExecutionEvent, Executor, StockExecutor, TurnInput},
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelContent, ModelContentPart, ToolResultContent},
    runtime::RuntimeScope,
    search::{SearchResponse, web_search},
    test_support::{ScriptedModelProvider, ScriptedSearch, ScriptedSearchProvider, ScriptedTurn},
    tool::ToolRegistry,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    configured: bool,
    granted: bool,
    prompt: String,
    turns: Vec<ScriptedTurn>,
    searches: Vec<ScriptedSearch>,
    text: Option<String>,
    error: Option<String>,
    response: Option<SearchResponse>,
    tool_events: Vec<String>,
}

#[tokio::test]
async fn web_search_scenarios() -> TestResult {
    let cases: Vec<Case> = serde_json::from_str(include_str!("web_search/cases.json"))?;
    let results = futures::future::join_all(cases.into_iter().map(|case| async move {
        let name = case.name.clone();
        run(case).await.map_err(|e| format!("{name}: {e}"))
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn run(case: Case) -> TestResult {
    assert_ne!(
        case.text.is_some(),
        case.error.is_some(),
        "exactly one terminal expectation"
    );
    let search = Arc::new(ScriptedSearchProvider::new(case.searches)?);
    let provider = Arc::new(ScriptedModelProvider::new(case.turns));
    let mut tools = ToolRegistry::default();
    if case.configured {
        tools.register((web_search(search.clone())?, None), &mut None, None)?;
    }
    let grants = if case.granted {
        vec!["tool:call:web_search"]
    } else {
        vec![]
    };
    let executor = StockExecutor::new(
        Model::new(
            "scripted".into(),
            "search-scenario".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_tool_authority(
        RuntimeScope::new(Capabilities::new(grants), Limits::default())?,
        None,
    )?;
    let storage = MemoryHarnessStorage::new(AgentId::new(), Limits::default().file_bytes).await?;
    let journal = storage.journal();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text(case.prompt),
        selected_context: None,
        max_steps: 4,
    };
    let result = executor.execute(input.clone(), journal.as_ref()).await;
    match (&case.text, &case.error, result) {
        (Some(text), None, Ok(actual)) => {
            assert_eq!(&actual.text, text);
            let replay = executor.execute(input.clone(), journal.as_ref()).await?;
            assert_eq!(replay, actual);
        }
        (None, Some(error), Err(actual)) => assert!(actual.to_string().contains(error), "{actual}"),
        (_, _, actual) => return Err(format!("unexpected terminal result: {actual:?}").into()),
    }
    assert_eq!(search.remaining(), 0, "unconsumed search steps");
    assert_eq!(provider.remaining(), 0, "unconsumed model turns");
    let requests = provider.requests();
    for request in &requests {
        assert_eq!(
            request.tools.iter().any(|tool| tool.name == "web_search"),
            case.configured && case.granted
        );
    }
    let mut records = Vec::new();
    let mut after = 0;
    loop {
        let page = journal.replay(input.operation_id, after, 64, None)?.await?;
        if page.is_empty() {
            break;
        }
        after = page.last().ok_or("empty page")?.sequence;
        records.extend(page);
    }
    let completed: Vec<_> = records
        .iter()
        .filter_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                result, projection, ..
            } => Some((result, projection)),
            _ => None,
        })
        .collect();
    let lifecycle: Vec<_> = records
        .iter()
        .filter_map(|record| match record.event {
            ExecutionEvent::ToolStarted { .. } => Some("started"),
            ExecutionEvent::ToolCompleted { .. } => Some("completed"),
            ExecutionEvent::ToolFailed { .. } => Some("failed"),
            _ => None,
        })
        .collect();
    assert_eq!(lifecycle, case.tool_events);
    for record in &records {
        assert!(!serde_json::to_string(record)?.contains("scripted search unavailable"));
    }
    if let Some(expected) = case.response {
        assert_eq!(completed.len(), 1);
        let (result, projection) = completed.first().ok_or("missing completed search")?;
        let expected = serde_json::to_value(expected)?;
        let result: Value = serde_json::from_slice(&journal.load(result, None)?.await?)?;
        assert_eq!(result, serde_json::json!({"value": expected}));
        let projection: Value = serde_json::from_slice(&journal.load(projection, None)?.await?)?;
        assert_eq!(
            projection,
            serde_json::json!({"kind":"json", "value":expected})
        );
        let results: Vec<_> = requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolResult {
                    name,
                    content: ToolResultContent::Json { value },
                    ..
                } if name == "web_search" => Some(value),
                _ => None,
            })
            .collect();
        assert_eq!(
            results,
            vec![&expected],
            "the second model turn must see exact search output"
        );
    } else {
        assert!(completed.is_empty());
    }
    Ok(())
}
