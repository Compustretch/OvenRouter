
![OvenRouter](/assets/oven_router_logo.png)

<p align="center">
<i>"Not all prompts need to be baked at the same temperature."</i>
</p>

## Cascade Router Engine

An in-process, high-throughput request routing engine written in Rust. It classifies incoming prompts and routes them to discriminative (Jev) or generative (LLM) execution pathways with a **p99 decision latency under 5 ms** for the overwhelming majority of traffic.

The router uses five canonical strategies: `rules`, `fasttext`, `encoder`, `decision`, and `generative`. The first three are cascade strategies compiled into the task-owned DAG. `decision` and `generative` are terminal execution strategies selected after route classification; they are never nodes in `CascadeGraph`.

<table width="100%"><tr><td>
 <h3>Most LLM requests do not need a generative model.</h3>
  <h3>Route the ones that don't to a much cheaper decision engine, and escalate only when necessary.</h3>
</td></tr></table>


---

## Table of Contents

1. [Design Goals](#design-goals)
2. [Cascade Semantics](#cascade-semantics)
3. [Tier Reference](#tier-reference)
4. [Latency Budget and Target Coverage](#latency-budget-and-target-coverage)
5. [Repository Layout](#repository-layout)
6. [Runtime and Performance Model](#runtime-and-performance-model)
7. [Configuration](#configuration)
8. [Building](#building)
9. [Model Artifacts](#model-artifacts)
10. [Testing](#testing)
11. [Benchmarking](#benchmarking)
12. [Observability](#observability)
13. [Extending the Router](#extending-the-router)
14. [Failure Modes and Fallback Behavior](#failure-modes-and-fallback-behavior)
15. [Known Constraints](#known-constraints)
16. [Contributing](#contributing)
17. [License](#license)

---

## Design Goals

- **Bounded latency.** Every tier has an explicit p99 budget. The 5 ms target is for requests resolved by the early cascade stages.
- **Early exit.** A request leaves the cascade at the first tier whose confidence meets its configured threshold.
- **No GC pauses, minimal allocation.** The hot path is native Rust with C++ bindings, a replaced global allocator, and zero-copy ingress for the cheapest tiers.
- **Deterministic behavior.** Given the same input, model artifacts, and configuration, the router returns the same decision.
- **One encoder stage.** ModernBERT and Harrier are concrete model choices behind the encoder strategy, not separate cascade tiers. A task path contains one encoder tier, whose declaration selects the model.

---

## Cascade Semantics

A request is a prompt payload. The dispatcher in `src/router/cascade.rs` runs tiers in order and stops at the first tier that returns a resolved outcome.

Each tier returns one of two outcomes:

- **Resolved:** the tier produced a routing decision whose confidence meets its threshold. The cascade terminates and the decision is returned.
- **PassThrough:** the tier could not decide with enough confidence (or is disabled). The payload moves to the next tier.

The cascade contains only the cascade strategies `rules`, `fasttext`, and `encoder`. Once route classification completes, the terminal dispatcher selects `decision` or `generative`. Strategy and provider are independent identities: `decision` currently executes through Ollaya, and `generative` currently executes through Ollama. The configured model identity (for example, `laya:en` or `gemma3:latest`) is recorded separately.

The `TierRegistry` catalogs available `Tier` definitions and their declarative parameters by `TierName`; it stores no `TierInstance` and assigns no `TierId`. Each `Task` owns an ordered `Cascade` of `TierName` values. `CascadeGraph` compiles the paths into a global union of materialized tier instances, assigning each instance a hash-derived `TierId`. Shared names produce shared graph nodes; each union transition retains the task paths that contributed it. Unused definitions remain in the registry without acquiring an instance ID. A cycle in the union of task-path transitions fails validation. Disabled definitions are omitted from each task's Cascade while preserving the remaining relative order.

```toml
[task.route]
cascade = ["rules", "fasttext", "encoder"]
labels = ["jev_capable", "needs_llm"]
fallback_label = "needs_llm"

[task.tag]
cascade = ["encoder"]
output_kind = "multiclass"
fallback = "defer"

[tier_policy.rules.route]
min_confidence = 0.78

[tier_policy.encoder.route]
min_confidence = 0.95

[tier_policy.encoder.tag]
min_confidence = 0.85
```

A task skips nodes absent from its own Cascade and nodes without a task-specific policy. After a pass, it continues to the next policy-bearing node along its ordered path. Resolution removes that task from the run; exhaustion applies its fallback. Tasks that are concurrently ready at the same graph node are batched into one invocation, allowing a single `ScoreTasks` request to evaluate multiple heads against one shared representation. If tasks reach that node at different times, the same `TierInstance` can be invoked again for the later cohort; an unrelated or optional predecessor never blocks a task that is already ready.

A new task adds its output contract, ordered `cascade` of TierNames, and policies for the tiers it uses. A new `[[tier]]` adds an available Tier definition to the registry; referencing its name in a task's Cascade causes `CascadeGraph` to materialize its TierInstance.

The identity layers stay distinct. `TierName` identifies an authored tier definition and appears in task cascades. `TierId` is generated only when `CascadeGraph` materializes a concrete instance; the hash includes its name and behavior-determining configuration, including inference runtime, model-related configuration and loaded artifact content. Endpoint and timeout are deployment details, not identity. `Slot` is the instance's position in the compiled graph, an execution/telemetry address rather than an identity.

`model` in the named tier declaration identifies the concrete model; `model_format` identifies its inference format. `Tier` remains an abstract strategy/transport/request-shape descriptor. `TierInstance` resolves the declaration and passes the model identifier to the selected runner. The encoder request formatter carries that identifier through gRPC metadata, and the sidecar rejects a request when its loaded model does not match. `TierInstance` is owned by `CascadeGraph`, not by `TierRegistry`.

The original four cascade positions remain only at the compatibility boundary: numbered telemetry fields, `[router.tierN]` settings, numbered environment variables, and `tierN` aliases in `[tier_policy.*]`. The `[legacy_tiers]` table in `tasks.toml` is the only place a cascade name and one of those old ordinals meet. The terminal strategies `decision` and `generative` have no cascade ordinal and no `TierId`.

`CascadeGraph` computes a deterministic topological projection of the global union of task-path transitions by resolving adjacent TierName pairs to hash-derived TierIds. That order supports stable graph presentation and `Slot` assignment; it is not a global runtime barrier. The runner advances each task along its own ordered path and concurrently evaluates independent ready nodes. Shared TierNames resolve to one shared `TierInstance`; tasks ready together at that node are batched, while later cohorts can invoke the same instance independently. Definitions absent from every task path remain available in the registry but do not materialize. The complete `tier_outcomes` array remains name-keyed in per-task progression order; existing numbered fields remain compatibility projections. Neither `decision` nor `generative` is a Cascade node.

---

## Tier Reference

### Tier 1: Static (Fuzzy) Rules and Regex Guardrails

- **Low-Latency Deterministic Pipeline:** Replaces conventional regex guardrails with Aho-Corasick automata and Damerau-Levenshtein fuzzy matching, scanning the immutable payload through exact positional, fuzzy-token, imperative-zone, prefix, and polarity checks.
- **Ambiguity as a First-Class Output:** The policy explicitly models competing intent. When strong LLM generative cues conflict with Jev discriminative signals, Tier 1 enters an explicit conflict state and defers to Tier 2 rather than forcing a premature guess.
- **Deterministic Query Fast Paths:** Recognizes requests whose answers can be resolved through closed-form computation or other deterministic operations, bypassing semantic model inference entirely. Math dependent requests are the simplest case; the same layer is designed to absorb quantitative and other structured queries as additional fast paths.

### Tier 2: Learned Decision Layer

- **Subword Route Classification:** FastText provides the first learned routing layer, using subword features to classify prompts into Jev or generative execution paths. The classifier exposes the full probability distribution rather than discarding the model's raw evidence after selecting the top class. n-gram embeddings evaluated via in-process static C++ bindings (`libfasttext`).
- **Confidence-Gated Escalation:** Tier 2 does not treat the top prediction as sufficient by itself. Its calibrated score is compared against a configurable confidence threshold; high-confidence Jev predictions terminate the cascade, while uncertain cases pass through to the next tier. The threshold therefore controls the tradeoff between Jev coverage and false-Jev commits. The classifier produces the top-1 probability P1 and top-2 probability P2. A margin-scaled confidence score is computed as:

  `S = P1 * (1.0 + (P1 - P2))`

  This tier resolves when `S >= confidence_threshold`; otherwise it returns PassThrough. A wide margin between the top two classes raises confidence, and a narrow margin lowers it, even when P1 alone is high.
- **Empirical Threshold Calibration:** Thresholds are evaluated against held-out labeled data rather than chosen by hand. The evaluation harness sweeps the actual score boundaries produced by the model, reports Jev precision and coverage with 95% Wilson confidence intervals, and rejects evaluation sets that overlap normalized training prompts. Source-held-out experiments provide an additional test of cross-source generalization.
- **Robust** to typos, inflections, and fuzzy keyword variation, because subword features generalize across spellings.
- **Artifact:** quantized binary `router_fastpath.ftz`.

### Tier 3: Encoder Strategy

- **One execution stage:** The encoder strategy selects the model-backed evaluation path for a given tier instance. ModernBERT and Harrier are model implementations, not separate cascade stages.
- **Model selection:** The tier declaration names the concrete model and its format. The instance passes that selection through the selected runner and gRPC transport to the sidecar, which validates that the request targets the model it loaded.
- **Joint inference:** With `request_shape = "score_tasks"`, the sidecar encodes the prompt once and evaluates each requested task head against the shared representation. Route and tag therefore share one encoder invocation when both tasks are ready in the same cohort.
- **Alternative model:** Harrier can replace ModernBERT by changing the encoder tier's `model`, `model_format`, and endpoint configuration. It does not appear as another element in the task Cascade.

### Terminal strategy: `decision`

- **Typed decision execution:** The terminal dispatcher sends the request's typed `state` and `questions` to the configured Decision provider.
- **Provider and model identity:** The current provider is Ollaya. The model is configured independently (default `laya:en`).
- **Eligibility:** Ollaya evaluates the caller's typed questions and an injected Jev-eligibility question in one request. A rejected or failed Decision attempt proceeds to `generative` when that strategy is enabled.

### Terminal strategy: `generative`

- **Open-ended generation:** The Generative strategy handles requests that require open-ended language generation, synthesis, or reasoning, as well as requests that the Decision strategy rejects or cannot serve.
- **Provider and model identity:** The current provider is Ollama. The model is configured independently (default `gemma3:latest`).
- **Dispatch semantics:** Every attempted terminal strategy is recorded in order. The separate final-result record identifies the strategy, provider, model and status that ultimately produced the caller's response, or the terminal failure if no response was produced.

### Strategy and provider ontology

The complete Strategy enum contains exactly five values: `rules`, `fasttext`, `encoder`, `decision`, and `generative`. Ollaya and Ollama are provider identities, not strategies. Model IDs such as `laya:en` and `gemma3:latest` are neither strategies nor provider names.
---

## Latency Budget and Target Coverage

Latency figures are per-tier p99 budgets. Coverage figures are **design targets** for the share of traffic each tier should resolve. Actual values depend on your traffic mix and must be measured (see [Benchmarking](#benchmarking) and [Observability](#observability)).

| Strategy | Execution | p99 Budget | Target Coverage |
| :--- | :--- | :--- | :--- |
| `rules` | `regex::RegexSet` and hash maps | < 0.1 ms | 25% to 35% |
| `fasttext` | FastText subword (`libfasttext`) | 0.2 ms to 0.4 ms | 40% to 50% |
| `encoder` | Configured encoder model via gRPC | TBD | TBD |

The 5 ms target applies to requests resolved by the cascade-scoring strategies within their measured budgets. Terminal Decision and Generative invocation latencies are reported separately and depend on the selected provider and model.

---

## Repository Layout

```text
src/
  config/
    mod.rs            Configuration loader with environment overrides
    schema.rs         Serde schemas for tier thresholds, model paths, flags
  logging/
    mod.rs            Low-overhead tracing and telemetry initialization
  pipeline/           Task-agnostic cascade engine (see "Multi-task cascade" below)
    task.rs           Task contracts, raw readings and tier/task resolution policies
    registry.rs       Unordered TierRegistry of available Tier definitions, keyed by TierName
    cascade.rs        Task-owned TierName paths; CascadeGraph materializes instances and compiles the DAG
    tier.rs           Tier definitions, hash identities, TierInstances and evaluator contract
    runner.rs         CascadeRunner: concurrent task-path frontiers over shared tier instances
    config.rs         config/tasks.toml loader
    tagger.rs         CascadeTagger: category tagging on the shared tiers
  router/
    cascade.rs        Pipeline dispatcher and lifecycle orchestration
    tier1.rs          Static hash, length bound, and regex evaluator
    tier2.rs          Learned Decision Tier (FastText subword classifier)
    tier3_runner.rs   (ModernBERT ONNX runtime stub)
    tier4.rs          Micro-Transformer / Embedding Evaluators (gRPC client using GGUF) 
    decision_runner.rs   Typed Jev decision execution
    generative_runner.rs Ollama-backed generative execution
  tier3/
    calibration.rs    Margin calculation and probability calibration
    trait.rs          Core evaluator trait and contract definitions
    inprocess/
      fasttext.rs     FFI wrapper for libfasttext
      modernbert.rs   ONNX Runtime implementation for ModernBERT
      harrier.rs      ONNX Runtime implementation for Harrier
      oryn.rs         Oryn in-process engine
    remote/
      client.rs       Asynchronous gRPC client
      proto.rs        Generated Protocol Buffer bindings
```

---

## Runtime and Performance Model

- **Global allocator:** the system allocator is replaced with `mimalloc` via `#[global_allocator]` to avoid heap lock contention across worker threads.
- **Threading:** a fixed worker pool sized by `router.workers`. Pinning workers to physical cores is recommended for stable tail latency.
- **Zero-copy ingress:** tiers 1 and 2 receive the payload as `&str`, so no copy or allocation is made during pre-filtering.
- **Quantization:** the ModernBERT model artifact is Int8 ONNX; the current Tier 3 stub does not load it yet.
- **Static linking:** `libfasttext` is linked statically to avoid dynamic loading overhead and deployment drift.

---

## Configuration

At startup, the router reads `config/router.toml`. The `tier1` through `tier4` blocks configure the historical cascade executors. The named `decision` and `generative` blocks configure terminal execution strategies. Omitted strategies default to disabled. Override the config file path with `ROUTER_CONFIG`.

```toml
[router.tier1]
enabled = true

[router.tier2]
enabled = true

[router.tier3]
enabled = true

[router.tier4]
enabled = true

[router.decision]
enabled = true
endpoint = "http://127.0.0.1:11435/api/decide"
model = "laya:en"
timeout_ms = 120000

[router.generative]
enabled = true
endpoint = "http://127.0.0.1:11434/api/generate"
model = "gemma3:latest"
timeout_ms = 120000
```

Rules and model/backend locations continue to use their existing environment variables and configuration. The `decision` and `generative` blocks independently enable their terminal strategy. A Decision failure may fall back to Generative when it is enabled. If the required terminal strategy is disabled or both providers fail, the router returns an explicit gateway error.

When Tier 3 is enabled, run the ModernBERT sidecar separately. It loads `models/modernbert-base/model_int8.onnx` and `models/modernbert-base/tokenizer.json` at startup and listens on port `50052` by default:

```bash
pip install -r sidecars/modernbert/requirements.txt
python sidecars/modernbert/server.py
```

Set `MODERNBERT_ENDPOINT` on the router to change its gRPC address. The model must be a two-class ONNX sequence classifier; set `MODERNBERT_JEV_LABEL_INDEX` to `0` or `1` if Jev is not class index `1`.

---

## Building

### Prerequisites

- A stable Rust toolchain
- A C++ toolchain and `clang`, required to build the static FastText library
- `pkg-config`
- Python 3 (for model training, exemplar dataset loading/serving, and calibration)

```bash
# Debian / Ubuntu
sudo apt-get install build-essential clang pkg-config

# macOS
xcode-select --install
```

### Compile

```bash
cargo build --release
```

For best SIMD performance, build with native CPU features enabled:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

Binaries built with `target-cpu=native` are not portable across CPU generations. Build on, or for, the deployment hardware.

---

## Model Artifacts

ModernBERT requires a valid ONNX sequence-classification model and matching tokenizer at the paths below. The checked-in files may be placeholders; the sidecar fails at startup if either file is empty.

| Artifact | Used by | Produced by |
| :--- | :--- | :--- |
| `router_fastpath.ftz` | Tier 2 | `scripts/train_fasttext.py` |
| `models/modernbert-base/model_int8.onnx` | Tier 3 | ONNX export and Int8 quantization pipeline |
| `models/modernbert-base/tokenizer.json` | Tier 3 | Tokenizer matching the ModernBERT ONNX model |

### Training the Tier 2 model

```bash
python3 scripts/train_fasttext.py \
  --input data/training_prompts.txt \
  --output models/router_fastpath.ftz
```

After training, recalibrate `tier2.confidence_threshold` against a held-out set. A retrained model invalidates the previous threshold.

---

## Testing

```bash
cargo test --all-targets
cargo run --example eval_tier1
cargo run --example eval_tier2
```

The Tier 1 eval reads `evals/tier1/cases.jsonl` and reports Jev precision, coverage,
LLM-verdict precision, and deferred cases. Review false Jev commits before changing
the Tier 1 thresholds or pattern set.

The Tier 2 eval reads `calibration/ft_valid.txt`, the held-out split produced from
the labeled calibration exemplars, and sweeps Jev precision and coverage across
confidence thresholds. It checks for normalized prompt overlap with
`calibration/ft_train.txt`, reports Wilson confidence intervals for precision,
evaluates each distinct Jev score boundary as well as `0` and `1`, and lists false
Jev commits at the current `0.95` threshold. To evaluate an independent labeled set,
pass it with `--cases` and provide its training source with `--training-cases`. Use a
separate representative validation set before changing the production threshold.

For a source-held-out stress test, train without one exemplar file and write all
generated artifacts outside the repository:

```bash
python3 calibration/make_fasttext_train.py \
  --holdout-source calibration/exemplars_ac-ct.jsonl \
  --output-dir /tmp/tier2-source-holdout
cargo run --example eval_tier2 -- \
  --model /tmp/tier2-source-holdout/tier2.bin \
  --cases /tmp/tier2-source-holdout/ft_holdout.txt \
  --training-cases /tmp/tier2-source-holdout/ft_train.txt
```

This checks cross-source generalization, not independent production performance;
the held-out prompts come from the same labeled exemplar collection.

To see tier-level output while debugging:

```bash
cargo test --all-targets -- --nocapture
```

The suite should cover, at minimum:

- Per-tier resolve and pass-through behavior at threshold boundaries
- End-to-end cascade ordering and short-circuiting
- Disabled-tier skipping
- Payload bound rejection at tier 1
- Calibration math in `calibration.rs`


### End-to-end validation

The test suites prove different contracts:

- `cargo test --all-targets` validates deterministic router behavior using local HTTP stubs, including Decision success, rejection, transport failure, timeout, Generative success/failure, and disabled strategies.
- `bash tests/clickhouse/migration-smoke.sh` applies migrations 001-006 to a scratch database and verifies historical strategy/provider normalization, including `cascade_decision_tier = 6` remaining `fallback` and unavailable historical model IDs remaining NULL.
- `bash tests/e2e/it-support-live.sh` exercises the configured real `laya:en` and `gemma3:latest` models, the account/password and VPN scenarios, the exact 20-leaf taxonomy, and request-correlated ClickHouse persistence.
- `scripts/smoke-jev-ch.sh` remains the separate arithmetic/ClickHouse acceptance test. The IT-support scenarios are additive and do not replace it.

The live smoke requires the router, Ollaya, Ollama, and ClickHouse to be running locally, with migration 006 applied. Its expected leaf categories are `account_pw` and `nw_vpn`; both are checked against the taxonomy file rather than a broad-domain label.

---

## Benchmarking

Micro-benchmarks use Criterion.

```bash
cargo bench
```

When validating the SLA:

- Benchmark each tier in isolation first, then the full cascade.
- Report p50, p99, and p99.9, not means.
- Run on the target deployment hardware with the production allocator and build flags.
- Use a representative traffic mix. Coverage targets in this document are only meaningful against realistic input distributions.

---

## Observability

Runtime observability records are written to ClickHouse. `tracing` provides local console diagnostics; the gateway does not export logs or traces to Loki, Tempo, or another remote telemetry service.

### Span Attributes and Evaluation Tracking
The Tier1Engine::classify method instruments spans with individual feature metrics and routing decisions:

- Exact & Fuzzy Feature Counters: matches_llm_prefix, starts_polar, jev_strong_hits, jev_mod_hits, llm_strong_hits
- Fast-Path Markers: is_binary_question
- Decision State: confidence, routing_reason

Tracking resolved-at-tier distribution over time is the primary health signal. A shift of traffic toward Tiers 4 through 6 indicates threshold, model, or prompt-distribution drift, causing tail latency and operational cost to rise.

### Adjusting Log Verbosity
Verbosity is controlled via RUST_LOG. The default level is info.
```RUST_LOG=info cargo run --release```
Recommended production configuration:
```RUST_LOG=info,cascade_router=debug```

Tracking **resolved-at-tier distribution over time** is the most important signal. A shift of traffic toward the encoder stage and terminal execution means thresholds, models, or the input distribution have drifted, and both cost and tail latency will rise.

### Request Capture

Each completed route is sent through a bounded, non-blocking Tokio channel to a background writer, which appends one JSON object per line to `logs/routing_capture.jsonl` and inserts it into ClickHouse's `route_records_raw` table. Each record contains the request ID, timestamp, prompt, cascade tier outcomes, ordered `dispatch_attempts`, and a distinct `final_result`. An attempt records strategy, provider, model ID, outcome, latency, and failure reason. The final result records the strategy/provider/model that produced the response, or the terminal failure. The legacy `cascade_decision_tier` value 6 means `fallback`; it is not the Generative strategy. Migration 006 projects these semantics into `route_execution_results`. Historical records without model identity retain a NULL model ID. ClickHouse insert failures are logged and the local spool is retained for replay.

The raw prompt is stored verbatim and may contain sensitive data. Restrict access to the capture file and define appropriate retention and cleanup; the writer appends indefinitely and does not rotate the file.

### Running the Gateway

`cargo run --release` starts the HTTP gateway on `127.0.0.1:8080` by default. It exposes
`GET /healthz`, `POST /v1/completions`, and `POST /v1/decisions`. Prompt-only requests are classified and can route directly to the Generative strategy. Structured requests may include `state` and `questions`; when classification selects Decision, Ollaya/Laya receives those typed questions and an injected eligibility question in one request. An ineligible or failed Decision attempt can fall back to the configured Generative strategy. Each attempt and the final result record provider and model identity independently.

```bash
docker run -d --name ollaya -p 11435:11435 ghcr.io/ollaya-dev/ollaya
ollaya run laya:en
ollama pull gemma3:latest
ollama pull qwen3:1.7b
cargo run --release
```

Ollaya publishes multi-architecture images (including `linux/amd64` and `linux/arm64`); Docker
selects the host architecture. Run ClickHouse with the migrations in `platform/clickhouse/init`
applied to enable typed route-decision and task-tag ingestion. The route and tag pipelines retain
JSONL spool files under `logs/`; ClickHouse insert failures are logged and do not block gateway
responses. Spool lines remain available for manual replay if ClickHouse is unavailable.

The principal runtime overrides are `ROUTER_BIND`, `MODERNBERT_ENDPOINT` (default
`http://[::1]:50052`), `TIER4_HARRIER_ENDPOINT` (falls back to
`TIER3_ENDPOINT` for compatibility), `DECISION_ENDPOINT` (default
`http://127.0.0.1:11435/api/decide`), `DECISION_MODEL` (default `laya:en`),
`GENERATIVE_ENDPOINT` (default `http://127.0.0.1:11434/api/generate`),
`GENERATIVE_MODEL` (default `gemma3:latest`),
`TAGGER_OLLAMA_URL` (default `http://127.0.0.1:11434`), `TAGGER_MODEL` (default
`qwen3:1.7b`), and `CLICKHOUSE_URL` (default `http://127.0.0.1:8123`).

Example structured request:

```bash
curl -s http://127.0.0.1:8080/v1/decisions \
  -H 'content-type: application/json' \
  -d '{"state":"Is 2 + 2 equal to 4?","questions":{"answer":{"type":"choice","instructions":"Does the statement hold?","criteria":{"yes":"It is true.","no":"It is false."}}}}'
```

### Adjusting the Log Level

Log verbosity is controlled with the `RUST_LOG` environment variable. The default level is `info`. Invalid `RUST_LOG` values cause startup to fail with a configuration error.

Set `RUST_LOG=debug` for more detail:

```bash
RUST_LOG=info cargo run --release
```

Common settings:

| Value | Effect |
| :--- | :--- |
| `RUST_LOG=info` | Recommended for normal operation and production |
| `RUST_LOG=debug` | Router decision detail without frame-level transport output |
| `RUST_LOG=warn` | Warnings and errors only |
| `RUST_LOG=trace` | Everything, including frame-level transport output. Use only for short debugging sessions |
| `RUST_LOG=trace,h2=warn,hyper=warn` | Full trace output for the router, with `h2` and `hyper` silenced |

Trace-level logging adds overhead and can distort the latency figures in this document. Benchmark and deploy with `info` or higher.


---

## Extending the Router

Tier 2 and tier 3 engines implement a common evaluator contract defined in `src/tier2/trait.rs`. To add an engine:

1. Add an implementation under `src/tier2/inprocess/` (or a remote one under `src/tier2/remote/`).
2. Implement the evaluator trait, returning a resolved decision with a confidence score, or a pass-through.
3. Register the engine name in the configuration schema in `src/config/schema.rs`.
4. Wire selection in the corresponding tier runner in `src/router/`.
5. Add threshold-boundary tests and a Criterion benchmark.

Engines must not allocate on the per-request hot path where avoidable, and must keep their documented p99 within the tier's budget.

---

## Failure Modes and Fallback Behavior

- **Tier pass-through:** low confidence is not an error. It is the normal escalation path.
- **Model load failure at startup:** the router should fail fast rather than silently run with a tier missing. Disabling a tier must be an explicit configuration choice.
- **Encoder timeout:** encoder timeouts and RPC failures pass through to route fallback.
- **Decision rejection or failure:** a failed Decision attempt is recorded and may fall through to Generative when enabled.
- **Generative unavailable:** provider failures return an explicit gateway error; the record preserves all attempted strategies and the terminal failure.
- **Oversize or malformed payloads:** rejected at tier 1 based on `max_payload_bytes`.

---

## Known Constraints

- The 5 ms p99 target applies to traffic resolved by the three cascade strategies. Terminal Decision and Generative latency is measured separately.
- Coverage percentages are targets, not guarantees.
- AVX-512 and NEON acceleration depend on the host CPU. Throughput and latency on other hardware will differ.
- Thresholds are coupled to specific model artifacts. Swapping or retraining a model requires recalibration.

---

## Contributing

1. Open an issue describing the change before starting large work.
2. Keep changes to tiers 1 to 3 allocation-free on the hot path.
3. Include tests and, for any hot-path change, Criterion benchmark results before and after.
4. Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test --all-targets` before opening a pull request.

---

## License

See the `LICENSE` file in the repository root.

---

## Multi-task cascade

Tiers are decoupled from the questions they answer. A `TierInstance` returns raw per-task
readings and applies no thresholds. Each `Task` owns its ordered `Cascade`, alongside its
output contract, allowed labels and fallback. `CascadeGraph` is compiled from all task paths.
The `CascadeRunner` advances each task along its own policy-bearing path. Independent ready
nodes execute concurrently; tasks that reach the same shared node together are batched, while
a later-arriving cohort can invoke that same `TierInstance` separately. Each task leaves when
it resolves or exhausts the policy-bearing tiers on its path.

* `route` (jev_capable / needs_llm): the policies in `config/tasks.toml` set the
  confidence bars for each eligible tier. The legacy `router::cascade::route`
  path remains separate from the generic task runner.
* `tag` (domain categories): `CascadeTagger` implements `tagging::Tagger`, falling back
  to the Ollama tagger when no tier resolves. Off by default (`tag_via_cascade` in
  `config/tasks.toml`) and needs a `[tier_policy.<tier>.tag]` entry for a tier in
  `[task.tag].cascade`.
* Joint execution: the additive `ScoreTasks` RPC scores several tasks from one shared
  encoding. A sidecar that answers UNIMPLEMENTED is served route-only via `Score`.
  No shipped sidecar implements it yet.

### Generic task execution API

The gateway also exposes a task-generic inference endpoint, separate from the
route-driven completion endpoints:

~~~http
POST /v1/tasks
Content-Type: application/json

{
  "prompt": "Evaluate the support request.",
  "tasks": ["customer_intent", "urgency", "fraud_risk"],
  "required_tasks": ["customer_intent"]
}
~~~

Every ID in `tasks` must have a corresponding `[task.<id>]` profile in
`config/tasks.toml`. The `required_tasks` field may name any non-empty subset
of the selected tasks; when omitted, all selected tasks are required. The
required subset is returned as soon as it resolves, while the remaining tasks
continue in the same cascade run. The response contains a `run_id` and the
IDs still pending.

Poll `GET /v1/tasks/{run_id}` for the final outcome. It returns HTTP 202 while
the cascade is running and HTTP 200 with every selected task result when the
run completes. Results include the resolving tier, model ID, label,
confidence, score distribution, and per-tier trace. This endpoint performs
task inference only; it does not invoke the route endpoint's Jev/LLM downstream
dispatcher.
