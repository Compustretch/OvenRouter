// src/router/task_api.rs
//
// Task-agnostic HTTP entry point. Callers select configured task IDs and any
// non-empty required subset. Required outcomes return early; remaining tasks
// continue in the same cascade and can be fetched by run ID.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::pipeline::{
    cascade::{CascadeGraph, Slot},
    runner::{CascadeRunner, Resolution, RunOutcome, TaskOutcome},
    task::{Decision, Task, TierPolicyMap, TierReading},
};

const RUN_TTL: Duration = Duration::from_secs(900);

#[derive(Clone)]
struct TaskApiState {
    cascade: Arc<CascadeRunner>,
    profiles: Vec<Task>,
    policies: Arc<TierPolicyMap>,
    budget: Duration,
    runs: TaskRunStore,
}

#[derive(Deserialize)]
struct TaskExecutionRequest {
    prompt: String,
    tasks: Vec<String>,
    #[serde(default)]
    required_tasks: Option<Vec<String>>,
}

const TOOL_REQUIREMENT_TASK: &str = "tool_requirement";
const NO_TOOL_REQUIRED: &str = "no_tool_required";
const TOOL_REQUIRED: &str = "tool_required";
const MAX_TOOL_REQUIREMENT_CONTEXT_BYTES: usize = 64 * 1024;
const MAX_TOOL_REQUIREMENT_MESSAGES: usize = 128;
const MAX_TOOL_REQUIREMENT_SKILLS: usize = 16;
const MAX_TOOL_REQUIREMENT_CAPABILITIES: usize = 128;

#[derive(Debug, Deserialize, Serialize)]
struct ToolRequirementRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_id: Option<Uuid>,
    messages: Vec<ToolRequirementMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent: Option<AgentContext>,
    #[serde(default)]
    active_skills: Vec<SkillContext>,
    #[serde(default)]
    capabilities: Vec<CapabilityDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_context: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ToolRequirementMessage {
    role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AgentContext {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SkillContext {
    name: String,
    description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CapabilityDescriptor {
    id: String,
    description: String,
}

#[derive(Clone, Default)]
struct TaskRunStore {
    entries: Arc<Mutex<HashMap<String, TaskRunEntry>>>,
}

struct TaskRunEntry {
    result: watch::Receiver<Option<Result<RunOutcome, String>>>,
    expires_at: Instant,
}

enum TaskRunLookup {
    Missing,
    Pending,
    Complete(RunOutcome),
    Failed(String),
}

impl TaskRunStore {
    fn register(
        &self,
        run_id: String,
        done: JoinHandle<RunOutcome>,
    ) -> Result<(), String> {
        let (sender, receiver) = watch::channel(None);
        {
            let mut entries = self.entries.lock()
                .map_err(|_| "task run store mutex poisoned".to_string())?;
            entries.retain(|_, entry| entry.expires_at > Instant::now());
            entries.insert(run_id, TaskRunEntry {
                result: receiver,
                expires_at: Instant::now() + RUN_TTL,
            });
        }
        tokio::spawn(async move {
            let result = done.await.map_err(|error| error.to_string());
            let _ = sender.send(Some(result));
        });
        Ok(())
    }

    fn poll(&self, run_id: &str) -> Result<TaskRunLookup, String> {
        let mut entries = self.entries.lock()
            .map_err(|_| "task run store mutex poisoned".to_string())?;
        entries.retain(|_, entry| entry.expires_at > Instant::now());
        let Some(entry) = entries.get(run_id) else {
            return Ok(TaskRunLookup::Missing);
        };
        let result = entry.result.borrow().clone();
        Ok(match result {
            None => TaskRunLookup::Pending,
            Some(Ok(outcome)) => TaskRunLookup::Complete(outcome),
            Some(Err(error)) => TaskRunLookup::Failed(error),
        })
    }
}

#[derive(Debug)]
enum ApiError {
    Invalid(String),
    Unavailable(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Invalid(detail) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_task_request", "detail": detail})),
            ).into_response(),
            Self::Unavailable(detail) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "task_unavailable", "detail": detail})),
            ).into_response(),
            Self::Internal(detail) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "task_execution_failed", "detail": detail})),
            ).into_response(),
        }
    }
}

pub fn router(
    cascade: Arc<CascadeRunner>,
    profiles: Vec<Task>,
    policies: Arc<TierPolicyMap>,
    budget: Duration,
) -> Router {
    let state = TaskApiState {
        cascade,
        profiles,
        policies,
        budget,
        runs: TaskRunStore::default(),
    };
    Router::new()
        .route("/", post(execute_tasks))
        .route("/tool-requirement", post(tool_requirement))
        .route("/:run_id", get(get_task_run))
        .with_state(state)
}

async fn tool_requirement(
    State(state): State<TaskApiState>,
    Json(request): Json<ToolRequirementRequest>,
) -> Response {
    match evaluate_tool_requirement(&state, request).await {
        Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn evaluate_tool_requirement(
    state: &TaskApiState,
    request: ToolRequirementRequest,
) -> Result<Value, ApiError> {
    let request_id = request.request_id.unwrap_or_else(Uuid::new_v4);
    let prompt = build_tool_requirement_input(&request)?;

    let profile = state.profiles.iter()
        .find(|profile| profile.name.as_str() == TOOL_REQUIREMENT_TASK)
        .cloned()
        .ok_or_else(|| ApiError::Unavailable(
            "task.tool_requirement is not configured".into()
        ))?;

    let labels = profile.labels.as_deref().unwrap_or_default();
    if profile.output_kind != crate::pipeline::task::OutputKind::Binary
        || labels.len() != 2
        || !labels.iter().any(|label| label == NO_TOOL_REQUIRED)
        || !labels.iter().any(|label| label == TOOL_REQUIRED)
    {
        return Err(ApiError::Unavailable(
            "task.tool_requirement must be binary with labels no_tool_required and tool_required".into()
        ));
    }

    if !state.policies.values().any(|by_task| by_task.contains_key(TOOL_REQUIREMENT_TASK)) {
        return Err(ApiError::Unavailable(
            "task.tool_requirement has no enabled tier policy".into()
        ));
    }

    let started = Instant::now();
    let outcome = state.cascade.run(
        &prompt,
        &[profile],
        &state.policies,
        state.budget,
    ).await;
    let task_outcome = outcome.tasks.get(TOOL_REQUIREMENT_TASK)
        .ok_or_else(|| ApiError::Internal(
            "tool_requirement cascade returned no task outcome".into()
        ))?;

    let payload = tool_requirement_result(request_id, task_outcome, &state.cascade.graph());
    tracing::info!(
        request_id = %request_id,
        status = payload["status"].as_str().unwrap_or("unknown"),
        decision = payload["decision"].as_str().unwrap_or("deferred"),
        confidence = ?payload["confidence"].as_f64(),
        tier_calls = outcome.tier_calls.len(),
        elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
        "tool requirement evaluated"
    );
    Ok(payload)
}

fn build_tool_requirement_input(request: &ToolRequirementRequest) -> Result<String, ApiError> {
    if request.messages.is_empty() || request.messages.len() > MAX_TOOL_REQUIREMENT_MESSAGES {
        return Err(ApiError::Invalid(format!(
            "messages must contain between 1 and {MAX_TOOL_REQUIREMENT_MESSAGES} entries"
        )));
    }
    if request.active_skills.len() > MAX_TOOL_REQUIREMENT_SKILLS {
        return Err(ApiError::Invalid(format!(
            "active_skills may contain at most {MAX_TOOL_REQUIREMENT_SKILLS} entries"
        )));
    }
    if request.capabilities.len() > MAX_TOOL_REQUIREMENT_CAPABILITIES {
        return Err(ApiError::Invalid(format!(
            "capabilities may contain at most {MAX_TOOL_REQUIREMENT_CAPABILITIES} entries"
        )));
    }

    let mut has_user_message = false;
    for (index, message) in request.messages.iter().enumerate() {
        if !matches!(message.role.as_str(), "system" | "developer" | "user" | "assistant" | "tool") {
            return Err(ApiError::Invalid(format!(
                "messages[{index}].role must be system, developer, user, assistant, or tool"
            )));
        }
        has_user_message |= message.role == "user";
        if message.content.is_none() && message.tool_calls.is_none() {
            return Err(ApiError::Invalid(format!(
                "messages[{index}] must include content or tool_calls"
            )));
        }
    }
    if !has_user_message {
        return Err(ApiError::Invalid("messages must include at least one user message".into()));
    }

    if let Some(agent) = &request.agent {
        validate_context_string(&agent.id, "agent.id", 128)?;
        if let Some(description) = &agent.description {
            validate_context_string(description, "agent.description", 2048)?;
        }
    }
    for (index, skill) in request.active_skills.iter().enumerate() {
        validate_context_string(&skill.name, &format!("active_skills[{index}].name"), 128)?;
        validate_context_string(&skill.description, &format!("active_skills[{index}].description"), 2048)?;
        if let Some(summary) = &skill.summary {
            validate_context_string(summary, &format!("active_skills[{index}].summary"), 8192)?;
        }
    }
    for (index, capability) in request.capabilities.iter().enumerate() {
        validate_context_string(&capability.id, &format!("capabilities[{index}].id"), 256)?;
        validate_context_string(&capability.description, &format!("capabilities[{index}].description"), 1024)?;
    }

    let current_user_index = request.messages.iter()
        .rposition(|message| message.role == "user")
        .expect("validated above: at least one user message");
    let current_user_message = &request.messages[current_user_index];
    let conversation_history: Vec<_> = request.messages.iter()
        .enumerate()
        .filter_map(|(index, message)| (index != current_user_index).then_some(message))
        .collect();

    // This versioned envelope is the classifier's input contract. The current
    // user message comes first because the encoder has a finite token window;
    // history follows it. It carries compact capability descriptions, never
    // executable tool schemas or authorization grants.
    let envelope = json!({
        "schema_version": "ovenrouter.tool_requirement.v1",
        "current_user_message": current_user_message,
        "agent": &request.agent,
        "active_skills": &request.active_skills,
        "capabilities": &request.capabilities,
        "task_context": &request.task_context,
        "conversation_history": conversation_history,
    });
    let serialized = serde_json::to_string(&envelope)
        .map_err(|error| ApiError::Internal(format!("failed to serialize routing context: {error}")))?;
    if serialized.len() > MAX_TOOL_REQUIREMENT_CONTEXT_BYTES {
        return Err(ApiError::Invalid(format!(
            "serialized routing context exceeds {MAX_TOOL_REQUIREMENT_CONTEXT_BYTES} bytes"
        )));
    }
    Ok(serialized)
}

fn validate_context_string(value: &str, field: &str, max_chars: usize) -> Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::Invalid(format!("{field} must not be empty")));
    }
    if value.chars().count() > max_chars {
        return Err(ApiError::Invalid(format!("{field} exceeds {max_chars} characters")));
    }
    Ok(())
}

fn tool_requirement_result(
    request_id: Uuid,
    outcome: &TaskOutcome,
    tiers: &CascadeGraph,
) -> Value {
    let details = task_outcome_json(outcome, tiers);
    let resolution = &details["resolution"];
    let (status, decision, confidence, scores, source) =
        match resolution["kind"].as_str().unwrap_or("deferred") {
            "tier" => (
                "resolved",
                resolution.get("label").cloned().unwrap_or(Value::Null),
                resolution.get("confidence").cloned().unwrap_or(Value::Null),
                resolution.get("scores").cloned().unwrap_or_else(|| json!([])),
                json!({
                    "tier": resolution.get("tier"),
                    "tier_id": resolution.get("tier_id"),
                    "model_id": resolution.get("model_id"),
                }),
            ),
            "fallback" => (
                "fallback",
                resolution.get("label").cloned().unwrap_or(Value::Null),
                Value::Null,
                json!([]),
                Value::Null,
            ),
            _ => ("deferred", Value::Null, Value::Null, json!([]), Value::Null),
        };

    json!({
        "request_id": request_id,
        "task": TOOL_REQUIREMENT_TASK,
        "decision": decision,
        "status": status,
        "confidence": confidence,
        "scores": scores,
        "source": source,
        "trace": details["trace"],
    })
}

async fn execute_tasks(
    State(state): State<TaskApiState>,
    Json(request): Json<TaskExecutionRequest>,
) -> Response {
    match execute_task_request(&state, request).await {
        Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn get_task_run(
    State(state): State<TaskApiState>,
    Path(run_id): Path<String>,
) -> Response {
    match state.runs.poll(&run_id) {
        Ok(TaskRunLookup::Missing) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "task_run_not_found", "run_id": run_id})),
        ).into_response(),
        Ok(TaskRunLookup::Pending) => (
            StatusCode::ACCEPTED,
            Json(json!({"run_id": run_id, "status": "pending"})),
        ).into_response(),
        Ok(TaskRunLookup::Complete(outcome)) => (
            StatusCode::OK,
            Json(json!({
                "run_id": run_id,
                "status": "complete",
                "tasks": all_task_results(&outcome, &state.cascade.graph()),
            })),
        ).into_response(),
        Ok(TaskRunLookup::Failed(error)) | Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": "task_execution_failed",
                "run_id": run_id,
                "detail": error,
            })),
        ).into_response(),
    }
}

async fn execute_task_request(
    state: &TaskApiState,
    request: TaskExecutionRequest,
) -> Result<Value, ApiError> {
    if request.prompt.trim().is_empty() {
        return Err(ApiError::Invalid("prompt must not be empty".into()));
    }
    if request.tasks.is_empty() {
        return Err(ApiError::Invalid(
            "tasks must contain at least one configured task ID".into(),
        ));
    }

    let mut selected = BTreeSet::new();
    let mut profiles = Vec::with_capacity(request.tasks.len());
    for name in &request.tasks {
        if name.trim().is_empty() {
            return Err(ApiError::Invalid("task IDs must not be empty".into()));
        }
        if !selected.insert(name.clone()) {
            return Err(ApiError::Invalid(format!("duplicate task ID {name:?}")));
        }
        let profile = state.profiles.iter()
            .find(|profile| profile.name.as_str() == name)
            .ok_or_else(|| ApiError::Invalid(format!("task {name:?} is not configured")))?;
        profiles.push(profile.clone());
    }

    let required_names = request.required_tasks
        .unwrap_or_else(|| request.tasks.clone());
    if required_names.is_empty() {
        return Err(ApiError::Invalid(
            "required_tasks must contain at least one task ID".into(),
        ));
    }

    let mut required_ids = BTreeSet::new();
    for name in &required_names {
        let id = name.clone();
        if !required_ids.insert(id) {
            return Err(ApiError::Invalid(format!(
                "duplicate required task ID {name:?}"
            )));
        }
        if !selected.contains(name) {
            return Err(ApiError::Invalid(format!(
                "required task {name:?} is not in tasks"
            )));
        }
    }

    let run_id = Uuid::new_v4().to_string();
    if required_ids.len() == profiles.len() {
        let outcome = state.cascade
            .run(&request.prompt, &profiles, &state.policies, state.budget)
            .await;
        return Ok(json!({
            "run_id": run_id,
            "status": "complete",
            "tasks": all_task_results(&outcome, &state.cascade.graph()),
        }));
    }

    let pending_names: Vec<String> = request.tasks.iter()
        .filter(|name| !required_ids.contains(&String::from((*name).clone())))
        .cloned()
        .collect();
    let split = Arc::clone(&state.cascade).run_split(
        request.prompt,
        profiles,
        required_ids.clone(),
        Arc::clone(&state.policies),
        state.budget,
    );
    state.runs.register(run_id.clone(), split.done)
        .map_err(ApiError::Internal)?;
    let required_outcome = split.required.await.map_err(|error| {
        ApiError::Internal(format!("required task result channel closed: {error}"))
    })?;

    Ok(json!({
        "run_id": run_id,
        "status": "partial",
        "required_tasks": required_names,
        "tasks": selected_task_results(&required_outcome, &required_ids, &state.cascade.graph()),
        "pending_tasks": pending_names,
    }))
}

fn all_task_results(outcome: &RunOutcome, tiers: &CascadeGraph) -> Value {
    let mut tasks = Map::new();
    for (task_id, task_outcome) in &outcome.tasks {
        tasks.insert(task_id.as_str().to_string(), task_outcome_json(task_outcome, tiers));
    }
    Value::Object(tasks)
}

fn selected_task_results(
    outcome: &RunOutcome,
    selected: &BTreeSet<String>,
    tiers: &CascadeGraph,
) -> Value {
    let mut tasks = Map::new();
    for task_id in selected {
        if let Some(task_outcome) = outcome.tasks.get(task_id) {
            tasks.insert(task_id.as_str().to_string(), task_outcome_json(task_outcome, tiers));
        }
    }
    Value::Object(tasks)
}

/// `tier` is the tier's name; `position` is its zero-based place in the
/// execution order, for clients that want to show the order.
fn task_outcome_json(outcome: &TaskOutcome, tiers: &CascadeGraph) -> Value {
    let resolution = match &outcome.resolution {
        Resolution::Tier { tier_id, tier, model_id, reading } => json!({
            "kind": "tier",
            "tier": tier.as_str(),
            "tier_id": tier_id.to_string(),
            "position": tiers.slot(tier_id).map(Slot::index),
            "model_id": model_id,
            "label": reading.label,
            "confidence": reading.confidence,
            "scores": reading.scores.iter().map(|(label, score)| {
                json!({"label": label, "score": score})
            }).collect::<Vec<_>>(),
        }),
        Resolution::Fallback { label } => json!({
            "kind": "fallback",
            "label": label,
        }),
        Resolution::Deferred => json!({"kind": "deferred"}),
    };

    let trace = outcome.trace.iter().map(|entry| {
        let reading = match &entry.reading {
            TierReading::Scored(reading) => json!({
                "kind": "scored",
                "label": reading.label,
                "confidence": reading.confidence,
            }),
            TierReading::Abstain(reason) => json!({
                "kind": "abstain",
                "reason": reason,
            }),
            TierReading::Unsupported => json!({"kind": "unsupported"}),
        };
        let decision = match &entry.decision {
            Decision::Resolve(reading) => json!({
                "kind": "resolve",
                "label": reading.label,
                "confidence": reading.confidence,
            }),
            Decision::Pass { reason, top_guess, score } => json!({
                "kind": "pass",
                "reason": reason,
                "top_guess": top_guess,
                "score": score,
            }),
        };
        json!({
            "tier": entry.tier.as_str(),
            "tier_id": entry.tier_id.to_string(),
            "position": tiers.slot(&entry.tier_id).map(Slot::index),
            "model_id": entry.model_id,
            "reading": reading,
            "decision": decision,
        })
    }).collect::<Vec<_>>();
    json!({"resolution": resolution, "trace": trace})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_requirement_input_preserves_conversation_skills_and_capabilities() {
        let request: ToolRequirementRequest = serde_json::from_value(json!({
            "messages": [
                {"role": "user", "content": "Run the workload."},
                {"role": "assistant", "tool_calls": [{"id": "call-1", "type": "function"}]},
                {"role": "tool", "name": "workload.run", "tool_call_id": "call-1", "content": "run_id=42"}
            ],
            "agent": {"id": "coda", "description": "Coding and operations agent"},
            "active_skills": [{
                "name": "workload-investigation",
                "description": "Investigate workload results",
                "summary": "Inspect execution metadata and query ClickHouse."
            }],
            "capabilities": [{
                "id": "clickhouse.query",
                "description": "Run authorized analytical queries."
            }]
        })).unwrap();

        let input = build_tool_requirement_input(&request).unwrap();
        let envelope: Value = serde_json::from_str(&input).unwrap();
        assert_eq!(envelope["schema_version"], "ovenrouter.tool_requirement.v1");
        assert_eq!(envelope["agent"]["id"], "coda");
        assert_eq!(envelope["active_skills"][0]["name"], "workload-investigation");
        assert_eq!(envelope["capabilities"][0]["id"], "clickhouse.query");
        assert_eq!(envelope["messages"][2]["role"], "tool");
        assert_eq!(envelope["messages"][2]["content"], "run_id=42");
    }

    #[test]
    fn tool_requirement_input_rejects_missing_user_context_and_invalid_roles() {
        let no_user: ToolRequirementRequest = serde_json::from_value(json!({
            "messages": [{"role": "assistant", "content": "Hello"}]
        })).unwrap();
        assert!(build_tool_requirement_input(&no_user).is_err());

        let invalid_role: ToolRequirementRequest = serde_json::from_value(json!({
            "messages": [{"role": "function", "content": "result"}, {"role": "user", "content": "Hi"}]
        })).unwrap();
        assert!(build_tool_requirement_input(&invalid_role).is_err());
    }

    #[test]
    fn tool_requirement_fallback_is_explicit_and_has_no_model_confidence() {
        let outcome = TaskOutcome {
            resolution: Resolution::Fallback { label: TOOL_REQUIRED.into() },
            trace: Vec::new(),
        };
        let result = tool_requirement_result(Uuid::nil(), &outcome, &CascadeGraph::default());
        assert_eq!(result["task"], TOOL_REQUIREMENT_TASK);
        assert_eq!(result["decision"], TOOL_REQUIRED);
        assert_eq!(result["status"], "fallback");
        assert!(result["confidence"].is_null());
        assert_eq!(result["source"], Value::Null);
    }
    use crate::pipeline::{
        cascade::{Cascade, CascadeGraph},
        registry::TierRegistry,
        task::{FallbackPolicy, OutputKind, Reading, TierPolicy},
        tier::{tname, FakeEvaluate, ModelFormat, Params, RequestShape, Strategy, Tier, TierContext, TierName, TierOutput, Transport},
    };
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use tokio::time::{sleep, timeout};

    type CallLog = Arc<Mutex<Vec<(String, Vec<String>)>>>;

    struct FixtureTier {
        name: String,
        delay: Duration,
        readings: BTreeMap<String, TierReading>,
        calls: CallLog,
    }
    #[async_trait]
    impl FakeEvaluate for FixtureTier {
        async fn evaluate(&self, context: &TierContext<'_>) -> TierOutput {
            self.calls.lock().unwrap().push((self.name.clone(), context.tasks.to_vec()));
            sleep(self.delay).await;
            let readings = context.tasks.iter().map(|task| (
                task.clone(),
                self.readings.get(task).cloned().unwrap_or(TierReading::Unsupported),
            )).collect();
            TierOutput { model_id: format!("fixture-tier-{}", self.name), readings }
        }
    }

    fn profile(task: &str, kind: OutputKind, labels: &[&str], path: &[&str]) -> Task {
        Task {
            name: task.to_string(),
            cascade: Cascade::from_names(&path.iter().map(|name| tname(name)).collect::<Vec<_>>()).unwrap(),
            output_kind: kind,
            labels: Some(labels.iter().map(|label| (*label).to_string()).collect()),
            label_metadata: Vec::new(),
            taxonomy_version: None,
            fallback: FallbackPolicy::Defer,
        }
    }

    fn fake_entry(
        name: &str,
        strategy: Strategy,
        fake: Arc<dyn FakeEvaluate>,
    ) -> (TierName, Tier, Params, Arc<dyn FakeEvaluate>) {
        let (transport, request_shape, model_format, model, config, endpoint, timeout_ms) =
            match strategy {
                Strategy::Rules => (Transport::InProcess, None, None, None, Some("test-rules".into()), None, None),
                Strategy::FastText => (Transport::InProcess, None, None, Some("test-fasttext".into()), None, None, None),
                Strategy::Encoder => (Transport::Grpc, Some(RequestShape::Score), Some(ModelFormat::Onnx),
                    Some(name.into()), None, Some("http://127.0.0.1:9".into()), Some(250)),
                Strategy::Decision | Strategy::Generative => {
                    unreachable!("terminal strategies cannot be task-cascade tiers")
                }
            };
        (
            tname(name),
            Tier { strategy: strategy, transport, request_shape },
            Params { model_format, model, config, endpoint, timeout_ms, enabled: None },
            fake,
        )
    }

    #[tokio::test]
    async fn arbitrary_tasks_follow_independent_paths_and_return_required_subset_first() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let registry = TierRegistry::of(vec![
            fake_entry("rules", Strategy::Rules, Arc::new(FixtureTier {
                name: "rules".into(),
                delay: Duration::from_millis(5),
                readings: [("customer_intent".into(),
                    TierReading::Scored(Reading::new("billing", 0.97)))].into(),
                calls: calls.clone(),
            })),
            fake_entry("modernbert", Strategy::Encoder, Arc::new(FixtureTier {
                name: "modernbert".into(),
                delay: Duration::from_millis(300),
                readings: [
                    ("urgency".into(), TierReading::Scored(Reading::new("high", 0.91))),
                    ("fraud_risk".into(), TierReading::Scored(Reading::new("payment_abuse", 0.88))),
                ].into(),
                calls: calls.clone(),
            })),
        ]);

        let profiles = vec![
            profile("customer_intent", OutputKind::Multiclass, &["billing", "account"], &["rules"]),
            profile("urgency", OutputKind::Ordinal, &["low", "medium", "high"], &["modernbert"]),
            profile("fraud_risk", OutputKind::Multilabel, &["account_takeover", "payment_abuse"], &["modernbert"]),
        ];
        let graph = CascadeGraph::from_tasks(&profiles, &registry).unwrap();
        let rules_id = graph.id_for_name(&tname("rules")).unwrap();
        let modernbert_id = graph.id_for_name(&tname("modernbert")).unwrap();
        let cascade = Arc::new(CascadeRunner::new(graph));
        let policies: TierPolicyMap = [
            (rules_id, [("customer_intent".into(), TierPolicy::new(0.90))].into()),
            (modernbert_id, [
                ("urgency".into(), TierPolicy::new(0.80)),
                ("fraud_risk".into(), TierPolicy::new(0.80)),
            ].into()),
        ].into();
        let state = TaskApiState {
            cascade, profiles, policies: Arc::new(policies),
            budget: Duration::from_secs(2), runs: TaskRunStore::default(),
        };
        let request = TaskExecutionRequest {
            prompt: "Evaluate the support request.".into(),
            tasks: vec!["customer_intent".into(), "urgency".into(), "fraud_risk".into()],
            required_tasks: Some(vec!["customer_intent".into()]),
        };

        let response = execute_task_request(&state, request).await.unwrap();
        assert_eq!(response["status"], "partial");
        assert_eq!(response["tasks"]["customer_intent"]["resolution"]["label"], "billing");
        assert_eq!(response["tasks"]["customer_intent"]["resolution"]["position"], 0);
        assert_eq!(response["pending_tasks"], json!(["urgency", "fraud_risk"]));
        let run_id = response["run_id"].as_str().unwrap().to_string();
        assert!(matches!(state.runs.poll(&run_id).unwrap(), TaskRunLookup::Pending));

        let complete = timeout(Duration::from_secs(2), async {
            loop {
                match state.runs.poll(&run_id).unwrap() {
                    TaskRunLookup::Complete(outcome) => break outcome,
                    TaskRunLookup::Failed(error) => panic!("task run failed: {error}"),
                    TaskRunLookup::Missing => panic!("task run expired before completion"),
                    TaskRunLookup::Pending => sleep(Duration::from_millis(5)).await,
                }
            }
        }).await.expect("best-effort tasks should finish within the test budget");

        assert_eq!(complete.tasks.len(), 3);
        assert!(matches!(complete.tasks[&String::from("urgency")].resolution,
            Resolution::Tier { ref tier, .. } if tier.as_str() == "modernbert"));
        assert!(matches!(complete.tasks[&String::from("fraud_risk")].resolution,
            Resolution::Tier { ref tier, .. } if tier.as_str() == "modernbert"));
        let mut calls = calls.lock().unwrap().clone();
        calls.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(calls, [
            ("modernbert".to_string(), vec!["urgency".into(), "fraud_risk".into()]),
            ("rules".to_string(), vec!["customer_intent".into()]),
        ]);
    }
}
