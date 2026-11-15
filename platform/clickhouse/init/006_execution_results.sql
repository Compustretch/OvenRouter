-- 006_execution_results.sql
--
-- Canonical terminal execution telemetry.
--
-- Strategy describes the operation: decision or generative.
-- Provider describes the implementation: ollaya or ollama.
-- Model identity is recorded only when the source event recorded it.
--
-- This is a compatibility migration for the current Will-It-Jev database. The
-- OvenRouter fork is expected to establish its own ClickHouse baseline; it does
-- not need to carry this historical bridge forward.
--
-- Historical rules:
--   cascade_decision_tier = 6 means the cascade exhausted its path: fallback.
--   downstream tier5 / jev means strategy=decision, provider=ollaya.
--   downstream tier6 / llm means strategy=generative, provider=ollama.
--   historical modernbert / harrier cascade names normalize to encoder.
--   the old records do not contain per-request model IDs, so model_id stays NULL.
--
-- Backfill is sourced from the append-only raw landing table. No model identity
-- is inferred from deployment defaults, current configuration, or provider names.

SELECT throwIf(count() > 0, 'migration 006 refused: route_execution_results already exists')
FROM system.tables
WHERE database = 'llm_observability'
  AND name IN ('route_execution_results', 'route_execution_results_mv')
FORMAT Null;

CREATE TABLE llm_observability.route_execution_results
(
    timestamp              DateTime64(6, 'UTC'),
    request_id             UUID,
    cascade_decision_tier  UInt8,
    decided_by_tier_name   LowCardinality(String),
    dispatch_attempts_json String CODEC(ZSTD(3)),
    final_strategy         LowCardinality(String),
    final_provider         Nullable(String),
    final_model_id         Nullable(String),
    final_status           LowCardinality(String),
    final_failure_reason   Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, request_id);

-- Replay the historical raw records once into the canonical projection. Old
-- payloads lack dispatch_attempts/final_result, so the SELECT below translates
-- only identities that the old record actually supports. Model IDs remain NULL.
INSERT INTO llm_observability.route_execution_results
WITH
    toUInt8(JSONExtractUInt(line, 'cascade_decision_tier')) AS legacy_ordinal,
    JSONExtractString(line, 'decided_by_tier_name') AS legacy_tier_name,
    JSONExtractString(line, 'final_routing', 'backend') AS legacy_backend,
    JSONExtractBool(line, 'is_failover') AS legacy_failover,
    JSONExtractString(line, 'failure_reason') AS legacy_failure,
    JSONExtractString(line, 'downstream_outcome') AS legacy_downstream_outcome,
    if(
        JSONHas(line, 'final_result'),
        JSONExtractString(line, 'final_result', 'strategy'),
        multiIf(legacy_backend = 'jev', 'decision', legacy_backend = 'llm', 'generative', '')
    ) AS canonical_strategy,
    if(
        JSONHas(line, 'final_result'),
        nullIf(JSONExtractString(line, 'final_result', 'provider'), ''),
        multiIf(legacy_backend = 'jev', 'ollaya', legacy_backend = 'llm', 'ollama', NULL)
    ) AS canonical_provider,
    if(
        JSONHas(line, 'final_result'),
        nullIf(JSONExtractString(line, 'final_result', 'model_id'), ''),
        CAST(NULL, 'Nullable(String)')
    ) AS canonical_model_id,
    if(
        JSONHas(line, 'final_result'),
        JSONExtractString(line, 'final_result', 'status'),
        if(empty(legacy_downstream_outcome), 'unknown', legacy_downstream_outcome)
    ) AS canonical_status,
    if(
        JSONHas(line, 'final_result'),
        nullIf(JSONExtractString(line, 'final_result', 'failure_reason'), ''),
        if(canonical_status = 'success', CAST(NULL, 'Nullable(String)'), nullIf(legacy_failure, ''))
    ) AS canonical_failure,
    if(
        legacy_ordinal = 6,
        'fallback',
        multiIf(
            legacy_ordinal = 1, 'rules',
            legacy_ordinal = 2, 'fasttext',
            legacy_ordinal IN (3, 4), 'encoder',
            legacy_tier_name IN ('modernbert', 'harrier'), 'encoder',
            legacy_tier_name
        )
    ) AS canonical_cascade_name,
    if(
        JSONHas(line, 'dispatch_attempts'),
        JSONExtractRaw(line, 'dispatch_attempts'),
        if(
            legacy_failover,
            concat(
                '[{"strategy":"decision","provider":"ollaya","model_id":null,"outcome":"',
                multiIf(
                    legacy_failure = 'jev_not_eligible', 'rejected',
                    legacy_failure = 'timeout', 'timeout',
                    legacy_failure = 'transport_error', 'connection_error',
                    legacy_failure = 'circuit_open', 'connection_error',
                    'failure'
                ),
                '","failure_reason":', toJSONString(nullIf(legacy_failure, '')),
                '},{"strategy":"generative","provider":"ollama","model_id":null,"outcome":"',
                if(legacy_downstream_outcome = 'success', 'success', 'failure'),
                '","failure_reason":null}]'
            ),
            if(
                legacy_backend = 'jev',
                '[{"strategy":"decision","provider":"ollaya","model_id":null,"outcome":"success","failure_reason":null}]',
                if(
                    legacy_backend = 'llm',
                    concat(
                        '[{"strategy":"generative","provider":"ollama","model_id":null,"outcome":"',
                        if(empty(legacy_downstream_outcome), 'unknown', legacy_downstream_outcome),
                        '","failure_reason":null}]'
                    ),
                    '[]'
                )
            )
        )
    ) AS canonical_attempts
SELECT
    parseDateTime64BestEffort(JSONExtractString(line, 'timestamp'), 6, 'UTC') AS timestamp,
    toUUID(JSONExtractString(line, 'request_id')) AS request_id,
    legacy_ordinal AS cascade_decision_tier,
    canonical_cascade_name AS decided_by_tier_name,
    canonical_attempts AS dispatch_attempts_json,
    canonical_strategy AS final_strategy,
    canonical_provider AS final_provider,
    canonical_model_id AS final_model_id,
    canonical_status AS final_status,
    canonical_failure AS final_failure_reason
FROM llm_observability.route_records_raw
WHERE isValidJSON(line)
  AND toUUIDOrNull(JSONExtractString(line, 'request_id')) IS NOT NULL
  AND parseDateTime64BestEffortOrNull(JSONExtractString(line, 'timestamp'), 6, 'UTC') IS NOT NULL;

-- The materialized view also understands replayed historical raw records. If a
-- spool is replayed after migration 006, it receives the same canonical mapping
-- as the one-time backfill above; missing historical model IDs remain NULL.
CREATE MATERIALIZED VIEW llm_observability.route_execution_results_mv
TO llm_observability.route_execution_results AS
WITH
    toUInt8(JSONExtractUInt(line, 'cascade_decision_tier')) AS legacy_ordinal,
    JSONExtractString(line, 'decided_by_tier_name') AS legacy_tier_name,
    JSONExtractString(line, 'final_routing', 'backend') AS legacy_backend,
    JSONExtractBool(line, 'is_failover') AS legacy_failover,
    JSONExtractString(line, 'failure_reason') AS legacy_failure,
    JSONExtractString(line, 'downstream_outcome') AS legacy_downstream_outcome,
    if(
        JSONHas(line, 'final_result'),
        JSONExtractString(line, 'final_result', 'strategy'),
        multiIf(legacy_backend = 'jev', 'decision', legacy_backend = 'llm', 'generative', '')
    ) AS canonical_strategy,
    if(
        JSONHas(line, 'final_result'),
        nullIf(JSONExtractString(line, 'final_result', 'provider'), ''),
        multiIf(legacy_backend = 'jev', 'ollaya', legacy_backend = 'llm', 'ollama', NULL)
    ) AS canonical_provider,
    if(
        JSONHas(line, 'final_result'),
        nullIf(JSONExtractString(line, 'final_result', 'model_id'), ''),
        CAST(NULL, 'Nullable(String)')
    ) AS canonical_model_id,
    if(
        JSONHas(line, 'final_result'),
        JSONExtractString(line, 'final_result', 'status'),
        if(empty(legacy_downstream_outcome), 'unknown', legacy_downstream_outcome)
    ) AS canonical_status,
    if(
        JSONHas(line, 'final_result'),
        nullIf(JSONExtractString(line, 'final_result', 'failure_reason'), ''),
        if(canonical_status = 'success', CAST(NULL, 'Nullable(String)'), nullIf(legacy_failure, ''))
    ) AS canonical_failure,
    if(
        legacy_ordinal = 6,
        'fallback',
        multiIf(
            legacy_ordinal = 1, 'rules',
            legacy_ordinal = 2, 'fasttext',
            legacy_ordinal IN (3, 4), 'encoder',
            legacy_tier_name IN ('modernbert', 'harrier'), 'encoder',
            legacy_tier_name
        )
    ) AS canonical_cascade_name,
    if(
        JSONHas(line, 'dispatch_attempts'),
        JSONExtractRaw(line, 'dispatch_attempts'),
        if(
            legacy_failover,
            concat(
                '[{"strategy":"decision","provider":"ollaya","model_id":null,"outcome":"',
                multiIf(
                    legacy_failure = 'jev_not_eligible', 'rejected',
                    legacy_failure = 'timeout', 'timeout',
                    legacy_failure = 'transport_error', 'connection_error',
                    legacy_failure = 'circuit_open', 'connection_error',
                    'failure'
                ),
                '","failure_reason":', toJSONString(nullIf(legacy_failure, '')),
                '},{"strategy":"generative","provider":"ollama","model_id":null,"outcome":"',
                if(legacy_downstream_outcome = 'success', 'success', 'failure'),
                '","failure_reason":null}]'
            ),
            if(
                legacy_backend = 'jev',
                '[{"strategy":"decision","provider":"ollaya","model_id":null,"outcome":"success","failure_reason":null}]',
                if(
                    legacy_backend = 'llm',
                    concat(
                        '[{"strategy":"generative","provider":"ollama","model_id":null,"outcome":"',
                        if(empty(legacy_downstream_outcome), 'unknown', legacy_downstream_outcome),
                        '","failure_reason":null}]'
                    ),
                    '[]'
                )
            )
        )
    ) AS canonical_attempts
SELECT
    parseDateTime64BestEffort(JSONExtractString(line, 'timestamp'), 6, 'UTC') AS timestamp,
    toUUID(JSONExtractString(line, 'request_id')) AS request_id,
    legacy_ordinal AS cascade_decision_tier,
    canonical_cascade_name AS decided_by_tier_name,
    canonical_attempts AS dispatch_attempts_json,
    canonical_strategy AS final_strategy,
    canonical_provider AS final_provider,
    canonical_model_id AS final_model_id,
    canonical_status AS final_status,
    canonical_failure AS final_failure_reason
FROM llm_observability.route_records_raw
WHERE isValidJSON(line)
  AND toUUIDOrNull(JSONExtractString(line, 'request_id')) IS NOT NULL
  AND parseDateTime64BestEffortOrNull(JSONExtractString(line, 'timestamp'), 6, 'UTC') IS NOT NULL;
