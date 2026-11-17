# OvenRouter

![OvenRouter](/assets/oven_router_logo.png)

<p align="center">
<i>"Not all prompts need to be baked at the same temperature."</i>
</p>

## Summary

OvenRouter is a configurable, multi-task inference and routing engine written in Rust. Its central extension point is the **task head**: an independently configured inference task with its own output contract, ordered cascade, resolution policies, and fallback behavior.

Organizations can add task heads to a shared inference substrate instead of building a separate pipeline for every classification problem. OvenRouter compiles the configured task paths into a shared execution graph, reuses materialized tier instances, and advances each task along its own path. Compatible model-backed heads can be evaluated together through one encoder representation, avoiding redundant encoding work.

A single deployment can therefore support distinct tasks such as tool-requirement classification, help-desk categorization, urgency assessment, and other domain-specific inference. Each task remains independently configurable; shared infrastructure does not force tasks to share labels, thresholds, or execution paths.

```text
                    Shared tier registry
                            |
                      CascadeGraph
                  _________|_________
                 /         |         \
          tool_requirement  tag (help-desk)  other task heads
             head             head          (as configured)
                 \_________  |  _________/
                           shared tiers
                                |
                      compatible encoder pass
```

Each head keeps its own output contract and resolution policy. The graph shares reusable tier instances; it does not turn the tasks into one combined classifier.

The first major application is **organizational tool routing**. A binary `tool_requirement` head determines whether a request requires an external tool. Requests that do not require tools proceed directly to generative inference. Requests that do enter Jev's tool-selection path, after which a generative model constructs the tool call and the MCP client executes it.

> **Implementation status:** the multi-task cascade and generic task-execution API are part of the current runtime. The new binary `tool_requirement` completion path, Jev tool-selection workflow, and MCP tool executor are the target architecture; they are **not yet implemented by the current runtime**. The current completion path still uses the legacy `jev_capable` / `needs_llm` route task and typed-decision / generative dispatch. This README distinguishes implemented behavior from the intended tool-routing design.

**Define the tasks. Share the cascade. Route the work.**

## Table of Contents

- [Task heads and the multi-task cascade](#task-heads-and-the-multi-task-cascade)
  - [What a task head defines](#what-a-task-head-defines)
  - [Shared tier registry and execution graph](#shared-tier-registry-and-execution-graph)
  - [Shared encoder execution](#shared-encoder-execution)
  - [Adding a task head](#adding-a-task-head)
- [Tool routing](#tool-routing)
  - [Tool-requirement classification](#tool-requirement-classification)
  - [Tool selection and execution](#tool-selection-and-execution)
  - [MCP compatibility](#mcp-compatibility)
  - [Current completion behavior](#current-completion-behavior)
- [Current configuration](#current-configuration)
- [Tier strategies](#tier-strategies)
- [HTTP API](#http-api)
  - [Prompt completion](#prompt-completion)
  - [Chat completion](#chat-completion)
  - [Typed decision request](#typed-decision-request)
  - [Generic task execution](#generic-task-execution)
  - [Request correlation](#request-correlation)
- [Build and run](#build-and-run)
  - [Prerequisites](#prerequisites)
  - [Principal runtime settings](#principal-runtime-settings)
- [Testing and validation](#testing-and-validation)
- [Observability](#observability)
- [Known limitations and transition work](#known-limitations-and-transition-work)
- [Contributing](#contributing)
- [License](#license)

---

## Task heads and the multi-task cascade

The multi-task cascade is OvenRouter's core architecture. A task head defines **what the system should infer**. The cascade defines **how that task obtains an answer**. Tiers provide reusable readings; task-specific contracts and policies decide whether those readings resolve a task.

This separation lets an organization add inference tasks without creating a new router or a separate copy of every tier implementation. It also lets tasks use different paths and different resolution criteria while sharing the execution resources that make sense to share.

### What a task head defines

A task head defines:

- A stable task ID and the question the task answers.
- Its output contract, such as a set of labels or a taxonomy-backed classification.
- Its ordered list of `TierName` values.
- Task-specific resolution policies for the tiers on its path.
- Its fallback behavior when no tier resolves the task.

The tasks do not need to share an output type or label space. One task can classify a prompt into a help-desk taxonomy while another makes a binary routing decision. A further task can assess urgency or another operational attribute, provided the configured scorer supports that task's output contract.

A task head is a configured inference contract, not a guarantee that a model already knows how to produce its output. Adding a model-backed task also requires a compatible scoring head and model-side support, plus appropriate data, calibration, and evaluation.

### Shared tier registry and execution graph

The available tier strategies are reusable definitions. `TierRegistry` catalogs those definitions by `TierName`; it does not own tier instances and does not assign instance IDs.

Each task owns an ordered `Cascade` of `TierName` values. `CascadeGraph` compiles the union of configured task paths and materializes the `TierInstance` objects those paths reference. Shared tier definitions can therefore resolve to shared instances, while each task retains its own progression and policies. The graph's topological order supports stable graph construction and slot assignment; it is not a global execution barrier.

A `TierInstance` returns raw readings. The task-specific `TierPolicy` determines whether a reading resolves that task. Policies can apply confidence thresholds, per-label thresholds, `never_resolve`, strict-boundary behavior, or observe-only authority. A low-confidence reading normally passes the task to its next policy-bearing tier. When a task exhausts its path, its configured fallback applies.

The identity layers are distinct:

- `TierName` identifies an available tier definition and appears in task cascades.
- `TierId` identifies a concrete materialized tier instance.
- `Slot` identifies the instance's position in the compiled graph for execution and telemetry; it is not a tier identity.

Definitions that no configured task references remain available in the registry but are not materialized into the execution graph.

### Shared encoder execution

Tasks that are ready at the same shared encoder instance can be evaluated together through the `ScoreTasks` request shape. The checked-in ModernBERT sidecar implements the shared `Score` / `ScoreTasks` service contract, allowing compatible task heads to share one encoder representation pass.

Each task still resolves independently according to its own output contract and policy. If tasks reach the shared encoder at different times, the same `TierInstance` can be invoked in separate cohorts. Sharing the encoder representation does not merge the task heads or make one task's resolution policy apply to another.

### Adding a task head

Task heads are declared in `config/tasks.toml`. The current configuration demonstrates two different tasks:

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
```

`route` uses a multi-stage cascade for its legacy routing classification. `tag` uses the encoder for taxonomy-backed help-desk classification. The two tasks have different contracts and paths, but can share the same encoder invocation when they are ready together and the configured sidecar supports both scoring heads.

To add another task, define its task contract, select its ordered cascade, and add policies for the tier/task pairs that should be allowed to resolve it. If the task uses a model-backed tier, implement or configure the corresponding scoring head in the encoder service. Then validate the output contract and calibrate its policies against held-out examples. Merely adding a task ID to configuration does not train or create a model head.

The generic task API can execute configured task IDs independently of the completion endpoint's legacy Decision/Generative dispatcher; see [Generic task execution](#generic-task-execution).

## Tool routing

Tool routing is the first major application of the multi-task cascade. OvenRouter answers the routing question; Jev selects the appropriate tool; a generative model constructs the call; and a host-side MCP client performs the protocol interaction.

### Tool-requirement classification

The target routing task is a binary classification:

- `no_tool_required`: answer through direct generative inference.
- `tool_required`: enter the tool-selection and tool-execution path.

The classifier decides which execution path a request should enter. It does not identify a tool, produce tool arguments, or execute an invocation. A request that requires company-specific information can be `tool_required` even when the eventual answer is short or straightforward.

The target task contract is:

```toml
[task.tool_requirement]
cascade = ["rules", "fasttext", "encoder"]
output_kind = "binary"
labels = ["no_tool_required", "tool_required"]
fallback_label = "tool_required"
```

This is an illustrative target contract, **not a configuration accepted by the current `master` completion path**. The current completion handler still looks up the task named `route` and expects the legacy labels. The fallback shown here is conservative: it favors avoiding missed tool invocations at the cost of more tool-selection attempts. Production calibration must measure that tradeoff against the organization's request distribution.

A positive classification means **enter the tool-selection path**. It does not guarantee that a suitable tool exists or that Jev will select one. “No applicable tool” is a distinct downstream outcome and must not be silently reclassified as `no_tool_required`.

### Tool selection and execution

The target execution path is:

```text
Request + conversation context
              |
              v
       OvenRouter task cascade
        tool_requirement head
              |
        +-----+------+
        |            |
 no_tool_required  tool_required
        |            |
        v            v
   Direct LLM       Jev selects a tool
                         |
                         v
                 Generative model creates
                  the structured tool call
                         |
                         v
                  MCP client executes it
                         |
                         v
                     Tool result
                         |
                         v
                 Generative model forms
                    the final response
```

The selected tool and the generated invocation are separate contracts. Jev should return a stable tool identity. The generative model receives that tool's authoritative description and input schema, then generates the structured call and arguments. The host-side executor invokes the selected tool and returns its result to the model. Tool selection, call generation, execution, result handling, and any required approval remain distinct and observable steps.

### MCP compatibility

MCP is the intended integration contract for the organizational tool ecosystem. The tool catalog should preserve each server's identity, native tool name, description, and input schema. The executor should invoke tools through standard MCP protocol interactions rather than translating every integration into a vendor-specific OvenRouter API.

Docker MCP Catalog images are an important deployment target, alongside remotely hosted MCP servers and organization-built servers. Docker packaging and gateway components should remain deployment options; they should not become a proprietary replacement for MCP itself.

**This compatibility is a design requirement, not a claim about the current runtime.** The current completion path has no MCP server discovery, MCP client transport, or tool executor. Those pieces must be implemented and tested against real MCP servers before OvenRouter can claim working MCP compatibility.

### Current completion behavior

The current `master` completion path has not completed the tool-routing transition. It currently:

1. Runs the configured `route` task through the `rules`, `fasttext`, and `encoder` cascade.
2. Resolves the legacy labels `jev_capable` or `needs_llm`.
3. Dispatches `jev_capable` requests to the `decision` strategy, which calls Ollaya's typed `/api/decide` endpoint.
4. Dispatches `needs_llm` requests to the `generative` strategy, which calls Ollama's `/api/generate` endpoint.
5. Falls back from a rejected or failed Decision attempt to Generative when Generative is enabled.

The current `DecisionRunner` evaluates supplied typed questions and injects a separate Jev-eligibility question. It does **not** select a tool. The current `GenerativeRunner` sends a prompt to `/api/generate`; it does **not** generate native structured tool calls. The current runtime has no Ollaya tool-selector stage or MCP/tool executor.

## Current configuration

The runtime loads `config/router.toml` and `config/tasks.toml` by default. Override their paths with `ROUTER_CONFIG` and `TASKS_CONFIG` respectively.

The current route and tag task declarations are shown in [Adding a task head](#adding-a-task-head). Available tiers are declared separately. The order of `[[tier]]` declarations has no execution meaning; each task's cascade defines its own order.

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
```

`config/router.toml` contains deployment settings for legacy numbered tier compatibility blocks and the current `decision` and `generative` terminal strategies. Provider identity and strategy identity are separate: `decision` is currently executed by Ollaya, and `generative` by Ollama. Model identifiers such as `laya:en` and `gemma3:latest` are configuration values, not strategy names.

The numbered `[router.tierN]` settings, environment variables, and numbered telemetry fields are compatibility surfaces. Named tier definitions and task-owned cascades are the current configuration model.

## Tier strategies

The cascade currently exposes three reusable tier strategies:

- **`rules`:** deterministic, in-process evaluation.
- **`fasttext`:** learned subword classification through the native FastText binding.
- **`encoder`:** model-backed evaluation through the configured encoder runtime and transport.

A task chooses the strategy instances it needs by naming tiers in its own cascade. `decision` and `generative` are current completion-dispatch strategies; they are not cascade tiers and have no `TierId` in the multi-task graph.

## HTTP API

The gateway exposes the following endpoints:

| Method | Path | Purpose |
| :--- | :--- | :--- |
| `GET` | `/healthz` | Liveness check |
| `POST` | `/v1/completions` | Prompt or typed-decision completion |
| `POST` | `/v1/chat/completions` | Chat-message completion; returns an OpenAI-style chat response |
| `POST` | `/v1/decisions` | Compatibility alias to the completion handler |
| `POST` | `/v1/tasks` | Run selected configured task heads |
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

The current implementation rejects `stream: true`. For chat requests, it currently classifies using the latest user message, while the generative fallback receives the serialized conversation. This context mismatch is a known limitation and must be addressed as part of context-aware tool-requirement classification.

### Typed decision request

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

`POST /v1/tasks` runs configured task IDs without invoking the completion endpoint's Decision/Generative dispatcher:

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

### Request correlation

The completion endpoints accept `X-Request-ID` as a UUID header and an optional JSON `request_id` for compatibility. If both are supplied, they must match. If neither is supplied, the router generates a UUIDv4. Malformed, duplicated, or conflicting IDs receive HTTP `400`. The resolved ID is echoed in the response's `X-Request-ID` header and propagated to current downstream calls and telemetry.

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

- The current route task still predicts `jev_capable` versus `needs_llm`; the binary tool-requirement task is not yet wired into the completion runtime.
- Current Ollaya integration performs typed decision evaluation and Jev-eligibility checking. It does not select tools.
- Current generative integration uses Ollama's `/api/generate`; it does not emit structured tool calls.
- There is no current MCP server discovery, MCP client transport, or tool executor in the completion path.
- The chat completion classifier currently receives only the latest user message, not the full conversation state.
- Streaming responses are rejected.
- The current model and threshold settings are workload-dependent. Retraining or changing a model requires recalibration and renewed evaluation.
- Capture spools contain raw request/response content and need an explicit operational retention policy.

The tool-routing implementation needs to add the binary tool-requirement task, route positive results through Jev tool selection, pass the selected tool schema to a generative model capable of structured tool-call generation, execute that call through the configured MCP client, and return the result to the model. The classifier, selector, call generator, and executor must retain distinct request/response contracts and observable outcomes.

## Contributing

1. Keep task contracts separate from tier implementations and confidence policies.
2. Keep tier definitions separate from materialized tier instances and task execution order.
3. Add tests for threshold boundaries, pass-through, fallback, context handling, and downstream failure paths.
4. For hot-path changes, include before/after benchmarks on representative hardware.
5. Run formatting, linting, and the relevant integration smoke tests before opening a pull request.

## License

See the `LICENSE` file in the repository root.
