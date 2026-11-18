# OvenRouter

![OvenRouter](/assets/oven_router_logo.png)

<p align="center">
<i>"Not all prompts need to be baked at the same temperature."</i>
</p>

## Summary

OvenRouter is a configurable, multi-task inference and routing engine written in Rust. Organizations can define and add task heads to a shared cascade, giving each task its own output contract, ordered evaluation path, confidence policies, and fallback behavior.

Rather than building a separate inference pipeline for every classification problem, OvenRouter compiles the configured task paths into a shared execution graph. Tasks reuse available tier instances, progress independently through their own cascades, and can execute compatible model-backed heads together through a single encoder representation.

This makes OvenRouter a general-purpose foundation for organizational AI inference. A single deployment can evaluate distinct tasks such as tool requirements, help-desk categories, urgency, and other domain-specific classifications without requiring a separate routing system for each.

Its first major application is organizational tool routing. A binary `tool_requirement` task determines whether a request needs an external tool. Requests that do not proceed directly to generative inference; requests that do enter Jev's tool-selection path, followed by generative tool execution.
i
If we were to reduce it to a mantra it would be: **Define the tasks. Share the cascade. Route the work.**




An in-process, high-throughput request-routing engine written in Rust. It
classifies incoming prompts and determines **whether the prompt requires a tool
call**, then routes accordingly: prompts that need tools enter a tool-executing
agent loop, and prompts that do not go directly to the LLM. The classification
decision has a **p99 latency under 5 ms**.

**Whether a request needs a tool call determines its execution shape.**
Tool-needing requests run as a stateful loop — generate, dispatch, observe,
repeat — with an unbounded number of turns and, for mutating tools, irreversible
side effects. Requests that don't need tools are a single completion. Deciding
which shape applies before generation starts keeps the loop, and its
irreversibility, off the path of requests that don't require it.

> **Implementation status:** `master` is in transition. The existing `/v1/completions` and `/v1/chat/completions` paths still use the legacy `jev_capable` / `needs_llm` task and typed-decision / generative dispatch. A separate binary `tool_requirement` task and context-aware `POST /v1/tasks/tool-requirement` decision endpoint are implemented. A trained `tool_requirement` encoder head is still a deployment artifact; until it is installed, the configured conservative fallback returns `tool_required`. Ollaya tool selection, structured tool-call generation, and MCP execution remain future work.







## Table of Contents

- [Routing model](#routing-model)
  - [Target tool-routing behavior](#target-tool-routing-behavior)
  - [Current `master` behavior](#current-master-behavior)
- [Cascade engine](#cascade-engine)
- [Current configuration](#current-configuration)
- [Build and run](#build-and-run)
  - [Prerequisites](#prerequisites)
  - [Principal runtime settings](#principal-runtime-settings)
- [HTTP API](#http-api)
  - [Prompt completion](#prompt-completion)
  - [Chat completion](#chat-completion)
  - [Typed decision request (current compatibility path)](#typed-decision-request-current-compatibility-path)
  - [Generic task execution](#generic-task-execution)
  - [Tool-requirement decision](#tool-requirement-decision)
  - [Request correlation](#request-correlation)
- [Testing and validation](#testing-and-validation)
- [Observability](#observability)
- [Known limitations and transition work](#known-limitations-and-transition-work)
- [Contributing](#contributing)
- [License](#license)







## Routing model

### Target tool-routing behavior

The primary routing task is a binary classification:

- `no_tool_required`: handle the request through direct LLM inference.
- `tool_required`: send the request through tool selection and tool-mediated inference.

The classifier decides which path the request should enter. It does not predict a tool identity, generate arguments, or execute an invocation.

```text
Request + conversation context
              |
              v
     OvenRouter task cascade
       tool-requirement head
              |
        +-----+------+
        |            |
 no_tool_required  tool_required
        |            |
        v            v
   Direct LLM     Ollaya selects a tool
                       |
                       v
                LLM generates the
                structured tool call
                       |
                       v
                Tool/MCP executor
                       |
                       v
                 Tool result
                       |
                       v
                LLM produces the
                  final response
```

OvenRouter coordinates the request-routing and tool-mediated execution paths. Ollaya determines which available tool is appropriate. The LLM receives that selected tool and its schema, then generates the actual structured call and arguments. The executor invokes the selected tool and returns its result to the LLM.

A positive classifier result means **enter the tool-selection path**. It does not guarantee that a matching tool exists or that Ollaya will select one. “No applicable tool” must remain a distinct downstream outcome; it must not be silently reinterpreted as `no_tool_required` by the classifier.

The `tool_requirement` task is exposed independently at `POST /v1/tasks/tool-requirement`. It accepts the conversation plus selected-agent context, active skill summaries, and compact capability descriptions. It returns a routing decision only: it does not select a tool, generate arguments, invoke a provider, or authorize an operation.

The tool-selection result and the LLM-generated call are separate contracts. The selected tool should be identified by a stable tool identity. The LLM should generate arguments against the selected tool's authoritative schema; it should not independently substitute another tool. Tool execution, result handling, and any approval policy remain explicit orchestration steps. The tool interface should preserve standard MCP tool identity and input-schema semantics so the router can work with canonical MCP servers rather than depending on a server-specific tool format.

An illustrative task contract for the target classifier is:

```toml
[task.tool_requirement]
cascade = ["encoder"]
output_kind = "binary"
labels = ["no_tool_required", "tool_required"]
fallback_label = "tool_required"
```

This task is configured separately from the legacy `route` task and is invoked through `/v1/tasks/tool-requirement`, not through the existing completion dispatcher. It currently uses the shared encoder tier only: the rules and FastText implementations still emit legacy Jev labels and must not be reused for this task without a task-generic head implementation. The conservative `tool_required` fallback favors avoiding missed tool invocations; its effect on unnecessary tool discovery must be measured.

### Current `master` behavior

The current implementation has not completed that transition. Its completion path currently does the following:

1. Runs the configured `route` task through the `rules`, `fasttext`, and `encoder` cascade.
2. Resolves the legacy labels `jev_capable` or `needs_llm`.
3. Dispatches `jev_capable` requests to the `decision` strategy, which calls Ollaya's typed `/api/decide` endpoint.
4. Dispatches `needs_llm` requests to the `generative` strategy, which calls Ollama's `/api/generate` endpoint.
5. Falls back from a rejected or failed Decision attempt to Generative when Generative is enabled.

The current `DecisionRunner` evaluates supplied typed questions and injects a separate Jev-eligibility question. It does **not** select a tool. The current `GenerativeRunner` sends a prompt to `/api/generate`; it does **not** generate native structured tool calls. The current runtime has no Ollaya tool-selector stage or MCP/tool executor.

The current route task and dispatch code are implementation state, not the target tool-routing contract. The redesign must replace the meaning and training labels of the route task and implement the tool-selection and tool-call path without conflating their responsibilities.

## Cascade engine

OvenRouter's cascade engine is task-oriented. A task defines the question being answered, its output contract, accepted labels, fallback behavior, and its ordered cascade. Tiers provide reusable readings; task-specific policies determine whether a reading resolves that task.

The available strategy types are:

- `rules`: deterministic, in-process evaluation.
- `fasttext`: learned subword classification through the native FastText binding.
- `encoder`: model-backed evaluation through the configured encoder runtime and transport.

A task's `cascade` is an ordered list of `TierName` values. `TierRegistry` catalogs available tier definitions; it does not own tier instances or assign instance IDs. `CascadeGraph` compiles the union of configured task paths and materializes the tier instances they reference. Each task still advances along its own ordered path. The graph's topological order is not a global execution barrier.

A `TierInstance` returns raw readings. A task-specific `TierPolicy` applies confidence thresholds, per-label thresholds, `never_resolve`, strict-boundary behavior, or observe-only authority. A low-confidence reading normally passes the task to its next policy-bearing tier. When a task exhausts its path, its configured fallback applies.

Tasks that are ready at the same shared encoder instance can be evaluated together through the `ScoreTasks` request shape. The checked-in ModernBERT sidecar implements the shared `Score` / `ScoreTasks` service contract, so compatible task heads can share one encoder representation pass. Tasks that reach the same instance at different times can invoke it in separate cohorts. Definitions that no task references remain available in the registry but are not materialized into the execution graph.

The identity layers are distinct:

- `TierName` identifies a configured tier definition and appears in task cascades.
- `TierId` identifies a concrete materialized tier instance.
- `Slot` is the instance's position in the compiled graph and an execution/telemetry address, not an identity.

The older numbered tier settings and telemetry fields remain compatibility surfaces. They do not define the cascade's current identity or ordering model.

## Current configuration

The runtime loads `config/router.toml` and `config/tasks.toml` by default. Override their paths with `ROUTER_CONFIG` and `TASKS_CONFIG` respectively.

The current route task is still configured with the legacy Jev labels:

```toml
[task.route]
cascade = ["rules", "fasttext", "encoder"]
labels = ["jev_capable", "needs_llm"]
fallback_label = "needs_llm"

[task.tag]
cascade = ["encoder"]
output_kind = "multiclass"
taxonomy = "../taskonomies/help_desk.json"
fallback = "defer"

[task.tool_requirement]
cascade = ["encoder"]
output_kind = "binary"
labels = ["no_tool_required", "tool_required"]
fallback_label = "tool_required"
```

Available tiers are declared separately. The order of `[[tier]]` declarations has no execution meaning; task cascades define the path.

```toml
[[tier]]
name = "rules"
strategy = "rules"
transport = "in_process"

[[tier]]
name = "fasttext"
strategy = "fasttext"
transport = "in_process"

[[tier]]
name = "encoder"
strategy = "encoder"
model = "modernbert-onnx"
model_format = "onnx"
transport = "grpc"
request_shape = "score_tasks"
```

Calibration belongs to a tier-name/task pair:

```toml
[tier_policy.rules.route]
min_confidence = 0.78
strict = true
never_resolve = ["needs_llm"]

[tier_policy.fasttext.route]
min_confidence = 0.95
never_resolve = ["needs_llm"]

[tier_policy.encoder.route]
min_confidence = 0.95
label_min_confidence = { jev_capable = 0.95, needs_llm = 0.70 }

[tier_policy.encoder.tag]
min_confidence = 0.85

[tier_policy.encoder.tool_requirement]
min_confidence = 0.70
label_min_confidence = { no_tool_required = 0.95 }
```

`config/router.toml` contains deployment settings for the legacy tier compatibility blocks and the current `decision` and `generative` terminal strategies. Provider identity and strategy identity are separate: `decision` is currently executed by Ollaya, and `generative` by Ollama. Model identifiers such as `laya:en` and `gemma3:latest` are configuration values, not strategy names.

`tool_requirement` is intentionally a separate task from the legacy `route` task. The existing `/v1/completions` dispatch path remains unchanged during this transition; harnesses can call the dedicated tool-requirement endpoint independently. The checked-in configuration uses an encoder-only cascade until task-generic rule and FastText heads are available. The initial confidence bar is provisional and must be calibrated against a held-out tool-requirement dataset.

## Build and run

### Prerequisites

- A stable Rust toolchain.
- A C/C++ build toolchain and `clang` for the native FastText dependency.
- `pkg-config`.

On macOS:

```bash
xcode-select --install
```

On Debian or Ubuntu:

```bash
sudo apt-get install build-essential clang pkg-config
```

Build and test:

```bash
cargo build --release
cargo test --all-targets
```

Run the gateway:

```bash
cargo run --release
```

The default bind address is `127.0.0.1:8080`. Set `ROUTER_BIND` to change it. The default configuration paths are `config/router.toml` and `config/tasks.toml`.

The encoder tier is a separate gRPC service. The checked-in ModernBERT sidecar is started separately and listens on port `50052` by default:

```bash
python3 -m pip install -r sidecars/modernbert/requirements.txt
python3 sidecars/modernbert/server.py
```

Configure its endpoint, model format, model identifier, and timeout on the tier definition or through the supported tier environment overrides. Ensure the sidecar has loaded the same model identifier declared by the router; the encoder request carries model identity to the sidecar for validation. Model files are deployment artifacts and must match the model and tokenizer expected by the sidecar.

### Principal runtime settings

| Setting | Purpose | Default / source |
| :--- | :--- | :--- |
| `ROUTER_CONFIG` | Router deployment configuration | `config/router.toml` |
| `TASKS_CONFIG` | Task, tier, and policy configuration | `config/tasks.toml` |
| `ROUTER_BIND` | HTTP listen address | `127.0.0.1:8080` |
| `FASTTEXT_MODEL` | FastText model override | Existing model-path fallback in `main.rs` |
| `TIER1_CONFIG` | Rules configuration override | `config/tier1.toml` |
| `DECISION_ENDPOINT` | Current Decision provider endpoint | `http://127.0.0.1:11435/api/decide` |
| `DECISION_MODEL` | Current Decision model | `laya:en` |
| `GENERATIVE_ENDPOINT` | Current Generative provider endpoint | `http://127.0.0.1:11434/api/generate` |
| `GENERATIVE_MODEL` | Current Generative model | `gemma3:latest` |
| `CLICKHOUSE_URL` | ClickHouse HTTP endpoint | `http://127.0.0.1:8123` |
| `RUST_LOG` | Tracing verbosity | `info` |

Tier endpoint and timeout overrides depend on the tier's configured model format. Prefer named tier settings in `config/tasks.toml`; the numbered `[router.tierN]` and environment settings exist for backward compatibility.

## HTTP API

The gateway exposes the following endpoints:

| Method | Path | Purpose |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Liveness check |
| `POST` | `/v1/completions` | Prompt or typed-decision completion |
| `POST` | `/v1/chat/completions` | Chat-message completion; returns an OpenAI-style chat response |
| `POST` | `/v1/decisions` | Compatibility alias to the completion handler |
| `POST` | `/v1/tasks` | Run selected configured task heads |
| `POST` | `/v1/tasks/tool-requirement` | Context-aware binary tool-necessity decision |
| `GET` | `/v1/tasks/{run_id}` | Poll the remaining task results |

### Prompt completion

```bash
curl -s http://127.0.0.1:8080/v1/completions \
  -H 'content-type: application/json' \
  -d '{"prompt":"Summarize the incident report.","stream":false}'
```

### Chat completion

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"messages":[{"role":"user","content":"Summarize the incident report."}],"stream":false}'
```

The existing completion implementation rejects `stream: true`. Its legacy route classifier uses the latest user message, while its generative fallback receives the serialized conversation. This limitation remains in the legacy completion path; the separate `/v1/tasks/tool-requirement` endpoint accepts structured conversation history for the new tool-necessity task.

### Typed decision request (current compatibility path)

A typed request must provide both `state` and `questions`:

```bash
curl -s http://127.0.0.1:8080/v1/decisions \
  -H 'content-type: application/json' \
  -d '{
    "state":"Is 2 + 2 equal to 4?",
    "questions":{
      "answer":{
        "type":"choice",
        "instructions":"Does the statement hold?",
        "criteria":{
          "yes":"The statement is true.",
          "no":"The statement is false."
        }
      }
    }
  }'
```

This endpoint currently uses the legacy Decision/Generative behavior. It is not the future tool-selection endpoint.

### Generic task execution

`POST /v1/tasks` runs any configured task IDs without invoking the completion endpoint's Decision/Generative dispatcher:

```bash
curl -s http://127.0.0.1:8080/v1/tasks \
  -H 'content-type: application/json' \
  -d '{
    "prompt":"Evaluate this support request.",
    "tasks":["tag"],
    "required_tasks":["tag"]
  }'
```

Every task ID must correspond to a `[task.<id>]` profile in `config/tasks.toml`. `required_tasks` must be a non-empty subset of `tasks`; when omitted, all selected tasks are required. When all selected tasks are required, the endpoint returns the complete result immediately. When only a subset is required, it returns that subset with a `run_id` and `pending_tasks`; poll `GET /v1/tasks/{run_id}` for the remaining results. Polling returns HTTP `202` while the run is pending and HTTP `200` when complete. Stored task runs expire after 15 minutes.

### Tool-requirement decision

`POST /v1/tasks/tool-requirement` evaluates the configured binary `tool_requirement` task without invoking a generative model or downstream tool-selection strategy. An agent harness calls it before each model inference.

The request preserves the conversation, including assistant tool-call messages and tool results. The harness may also send the selected agent, active skill summaries, and compact capability descriptions. Do not send executable tool schemas or treat these descriptions as authorization grants. Supply `X-Request-ID` or the JSON `request_id` field for correlation; when both are present they must match. If neither is supplied, OvenRouter generates a UUID. The response echoes the resolved ID in its `X-Request-ID` header and `request_id` field.

```json
{
  "messages": [
    {"role": "user", "content": "Run the workload."},
    {"role": "assistant", "tool_calls": [{"id": "call-1", "type": "function"}]},
    {"role": "tool", "name": "workload.run", "tool_call_id": "call-1", "content": "run_id=42"},
    {"role": "user", "content": "Now investigate that execution."}
  ],
  "agent": {"id": "coda"},
  "active_skills": [
    {
      "name": "workload-investigation",
      "description": "Investigate workload execution outcomes.",
      "summary": "Inspect execution metadata and query ClickHouse."
    }
  ],
  "capabilities": [
    {
      "id": "clickhouse.query",
      "description": "Run authorized analytical queries."
    }
  ]
}
```

The versioned `ovenrouter.tool_requirement.v1` envelope uses an ordered context array: current user message first, followed by agent metadata, active skill summaries, capability descriptions, task context, and prior conversation history. The API accepts up to 128 messages, 16 skills, and 128 capabilities and bounds the serialized request to 64 KiB. The shared ModernBERT encoder currently truncates its tokenized input to 512 tokens, so the harness should send a compact, relevant conversation window. The 64 KiB API limit is a transport bound, not a guarantee that the entire request fits the model context.

A model-resolved response contains the predicted label and confidence:

```json
{
  "request_id": "7a4d35a2-3983-4d90-8f6d-0cbbda9a6c72",
  "task": "tool_requirement",
  "decision": "tool_required",
  "status": "resolved",
  "confidence": 0.97,
  "scores": [
    {"label": "no_tool_required", "score": 0.03},
    {"label": "tool_required", "score": 0.97}
  ],
  "source": {
    "tier": "encoder",
    "tier_id": "…",
    "model_id": "modernbert-onnx"
  },
  "trace": []
}
```

The configured fallback is deliberately conservative: `tool_required`. If the encoder has no trained head for this task, the sidecar reports the task as unsupported and the router returns `status: "fallback"`, `decision: "tool_required"`, and a null confidence. The response therefore does not present a fallback as a model prediction.

The head artifact belongs at `models/modernbert-base/heads/tool_requirement/head.json`. Train it from labeled JSONL examples containing `prompt` and `label` fields (`tool_required` or `no_tool_required`) using `calibration/train_heads.py` with `--task tool_requirement`, `--output-kind binary`, the shared encoder and tokenizer, and a held-out dataset for temperature calibration. The training input must use the same `ovenrouter.tool_requirement.v1` context serialization used by the endpoint. The threshold in `config/tasks.toml` is an initial conservative value, not a production calibration result.

This endpoint decides **whether** to enter capability discovery. It does not select a specific capability, generate a tool call, execute MCP, or enforce permissions. The caller retains responsibility for those stages.

### Request correlation

The completion endpoints accept `X-Request-ID` as a UUID header and an optional JSON `request_id` for compatibility. If both are supplied, they must match. If neither is supplied, the router generates a UUIDv4. Malformed, duplicated, or conflicting IDs receive HTTP `400`. The resolved ID is echoed in the response's `X-Request-ID` header and propagated to current downstream calls and telemetry.

## Testing and validation

Run the deterministic Rust suite first:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

The repository also contains integration and acceptance checks:

```bash
bash tests/clickhouse/migration-smoke.sh
bash tests/e2e/it-support-live.sh
bash scripts/smoke-jev-ch.sh
```

The ClickHouse migration smoke uses a scratch database. The live IT-support smoke requires the router, configured Ollaya and Ollama models, and ClickHouse to be available. The Jev/ClickHouse smoke is a separate arithmetic acceptance path. These tests exercise the current implementation; they do not demonstrate the not-yet-implemented tool-selection workflow.

For performance work, measure the cascade tiers and complete request path separately on the intended deployment hardware. Report latency percentiles and the fraction of requests resolved at each tier against a representative workload. Do not treat historical target percentages or an unmeasured latency target as a production guarantee.

## Observability

The router uses `tracing` for local diagnostics. Completed route records are sent through a bounded background channel, appended as JSON Lines to `logs/routing_capture.jsonl`, and inserted into ClickHouse's `route_records_raw` table. The capture includes the request ID, input prompt, task decisions, per-tier outcomes, ordered downstream dispatch attempts, and final result. ClickHouse insert failures do not block the completion response; the local JSONL spool is retained.

Tagging writes its own local JSONL spool under `logs/` and can insert tag records into ClickHouse. Cascade tagging and joint route/tag encoder scoring are configurable through the pipeline settings in `config/tasks.toml`.

**Privacy and retention:** captured prompts and responses are written verbatim. Restrict access to the log directory and ClickHouse tables, define retention and cleanup, and avoid sending secrets or regulated data unless the deployment's data-handling controls permit it. The local capture writer appends to its spool; configure external rotation and retention.

## Known limitations and transition work

- The existing completion endpoints still predict `jev_capable` versus `needs_llm`; they have not been migrated to the binary tool-requirement decision.
- The separate `tool_requirement` task API is implemented, but a trained encoder head must be installed before it produces model-resolved decisions; its default fallback is `tool_required`.
- Current Ollaya integration performs typed decision evaluation and Jev-eligibility checking. It does not select tools.
- Current generative integration uses Ollama's `/api/generate`; it does not emit structured tool calls.
- There is no current MCP/tool executor in the completion path.
- The chat completion classifier currently receives only the latest user message, not the full conversation state.
- Streaming responses are rejected.
- The current model and threshold settings are workload-dependent. Retraining or changing a model requires recalibration and renewed evaluation.
- Capture spools contain raw request/response content and need an explicit operational retention policy.

The remaining tool-routing implementation needs to route positive `tool_requirement` results through capability discovery and Ollaya tool selection, pass the selected tool schema to an LLM capable of structured tool-call generation, execute that call through the configured tool transport, and return the result to the LLM. The classifier, selector, call generator, and executor must retain distinct request/response contracts and observable outcomes.

## Contributing

1. Keep task contracts separate from tier implementations and confidence policies.
2. Keep tier definitions separate from materialized tier instances and task execution order.
3. Add tests for threshold boundaries, pass-through, fallback, context handling, and downstream failure paths.
4. For hot-path changes, include before/after benchmarks on representative hardware.
5. Run formatting, linting, and the relevant integration smoke tests before opening a pull request.

## License

See the `LICENSE` file in the repository root.
