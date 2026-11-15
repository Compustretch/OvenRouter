#!/usr/bin/env bash
#
# Arithmetic smoke test for router ingress, request correlation and ClickHouse.
# The prompt/state/questions remain semantically clean. Correlation is carried
# only by X-Request-ID, and all primary assertions use typed ClickHouse tables.

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ROUTER_URL="${ROUTER_URL:-http://127.0.0.1:8080/v1/decisions}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-llm_observability}"
ROUTE_WAIT_SECONDS="${ROUTE_WAIT_SECONDS:-60}"
TAG_WAIT_SECONDS="${TAG_WAIT_SECONDS:-45}"
ROUTER_TIMEOUT_SECONDS="${ROUTER_TIMEOUT_SECONDS:-60}"

for cmd in curl jq uuidgen awk; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "$cmd is required" >&2; exit 1; }
done

AUTH_FILE="${PROJECT_ROOT}/.auth"
RESPONSE_FILE="$(mktemp)"
HEADER_FILE="$(mktemp)"
cleanup() { rm -f "$RESPONSE_FILE" "$HEADER_FILE"; }
trap cleanup EXIT

fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }
info() { printf '[INFO] %s\n' "$*"; }
pass() { printf '[PASS] %s\n' "$*"; }
section() { printf '\n============================================================\n  %s\n============================================================\n' "$1"; }

[[ -f "$AUTH_FILE" ]] || fail "Missing credentials file: $AUTH_FILE"
set -a
# shellcheck disable=SC1090
source "$AUTH_FILE"
set +a
: "${CLICKHOUSE_USER:?CLICKHOUSE_USER missing from .auth}"
: "${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD missing from .auth}"
CH_AUTH=(--user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}")

ch_query() {
    curl -sS --fail-with-body "${CH_AUTH[$]}" \
        --get --data-urlencode "query=$1" "${CLICKHOUSE_URL}/"
}

# The UUID is generated independently and never appears in request semantics.
SMOKE_UUID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
[[ "$SMOKE_UUID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ ]] \
    || fail "uuidgen did not produce a canonical UUIDv4: $SMOKE_UUID"

REQUEST_BODY='{
  "prompt": "What is 18% of 450?",
  "state": "What is 18% of 450?",
  "questions": {
    "answer": {
      "type": "choice",
      "instructions": "What is 18 percent of 450? Select the exact result.",
      "criteria": {
        "81": "The exact arithmetic result is 81.",
        "other": "The arithmetic result is any value other than 81."
      }
    }
  }
}'

section "PREFLIGHT"
ch_query "SELECT 1 FORMAT TabSeparated" >/dev/null || fail "ClickHouse unreachable or authentication failed"
for table in route_decisions task_tags route_execution_results; do
    count="$(ch_query "SELECT count() FROM system.tables WHERE database='${CLICKHOUSE_DATABASE}' AND name='${table}' FORMAT TabSeparated")" \
        || fail "Could not inspect ClickHouse table $table"
    [[ "${count//[[:space:]]/}" == "1" ]] || fail "Required typed table missing: ${CLICKHOUSE_DATABASE}.${table}"
done
curl -sS --fail-with-body --connect-timeout 3 --max-time 5 "${ROUTER_URL%/v1/decisions}/healthz" >/dev/null \
    || fail "Router health check failed"
pass "Router and typed ClickHouse projections are reachable"
info "Request ID: $SMOKE_UUID"

section "ROUTER REQUEST"
HTTP_STATUS="$(curl -sS --connect-timeout 3 --max-time "$ROUTER_TIMEOUT_SECONDS" \
    --dump-header "$HEADER_FILE" --output "$RESPONSE_FILE" --write-out '%{http_code}' \
    --request POST \
    --header 'Content-Type: application/json' \
    --header "X-Request-ID: ${SMOKE_UUID}" \
    --data-binary "$REQUEST_BODY" "$ROUTER_URL")" || fail "Router HTTP request failed"
[[ "$HTTP_STATUS" == "200" ]] || fail "Router returned HTTP $HTTP_STATUS: $(cat "$RESPONSE_FILE")"

RESPONSE_REQUEST_ID="$(awk 'tolower($1) == "x-request-id:" { gsub("\r", "", $2); value=$2 } END { print value }' "$HEADER_FILE")"
[[ "$RESPONSE_REQUEST_ID" == "$SMOKE_UUID" ]] \
    || fail "Response X-Request-ID mismatch: expected $SMOKE_UUID, received ${RESPONSE_REQUEST_ID:-missing}"
pass "Router response echoed X-Request-ID unchanged"
info "Response body:"
jq . "$RESPONSE_FILE" || cat "$RESPONSE_FILE"

assert_one_typed_row() {
    local table="$1"
    local count
    count="$(ch_query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.${table} FINAL WHERE request_id = toUUID('${SMOKE_UUID}') FORMAT TabSeparated")" \
        || fail "Could not count typed rows for ${table}"
    [[ "${count//[[:space:]]/}" == "1" ]] \
        || fail "Expected exactly one ${table} row for request_id=${SMOKE_UUID}; found ${count}"
    printf '[PASS] %s has exactly one row for request_id=%s\n' "$table" "$SMOKE_UUID"
}

wait_typed_row() {
    local table="$1" request_id="$2" wait_seconds="$3" query="$4" result="" attempt
    for ((attempt=1; attempt<=wait_seconds; attempt++)); do
        result="$(ch_query "$query")" || fail "ClickHouse query failed for ${table} request_id=${request_id}"
        [[ -n "$result" ]] && { printf '%s' "$result"; return 0; }
        sleep 1
    done
    fail "No typed ${table} row for request_id=${request_id} within ${wait_seconds}s"
}

section "TYPED ROUTE CORRELATION"
ROUTE_SQL="
SELECT toString(request_id) AS request_id, toString(timestamp) AS timestamp,
       prompt_raw, decided_by_tier, decided_backend, final_backend,
       downstream_tier, downstream_outcome, is_failover, failure_reason,
       total_latency_us
FROM ${CLICKHOUSE_DATABASE}.route_decisions FINAL
WHERE request_id = toUUID('${SMOKE_UUID}')
LIMIT 1
FORMAT JSONEachRow
"
ROUTE_ROW="$(wait_typed_row route_decisions "$SMOKE_UUID" "$ROUTE_WAIT_SECONDS" "$ROUTE_SQL")"
assert_one_typed_row route_decisions
jq -e --arg id "$SMOKE_UUID" '.request_id == $id and .prompt_raw == "What is 18% of 450?"' \
    <<<"$ROUTE_ROW" >/dev/null || fail "Typed route row has a mismatched ID or contaminated prompt"
pass "route_decisions contains the same UUID and the exact clean prompt"
printf '%s\n' "$ROUTE_ROW" | jq .

section "TYPED EXECUTION CORRELATION"
EXECUTION_SQL="
SELECT toString(request_id) AS request_id, cascade_decision_tier,
       decided_by_tier_name, dispatch_attempts_json, final_strategy,
       final_provider, final_model_id, final_status, final_failure_reason
FROM ${CLICKHOUSE_DATABASE}.route_execution_results FINAL
WHERE request_id = toUUID('${SMOKE_UUID}')
LIMIT 1
FORMAT JSONEachRow
"
EXECUTION_ROW="$(wait_typed_row route_execution_results "$SMOKE_UUID" "$ROUTE_WAIT_SECONDS" "$EXECUTION_SQL")"
assert_one_typed_row route_execution_results
jq -e --arg id "$SMOKE_UUID" '.request_id == $id and (.dispatch_attempts_json | fromjson | length >= 1)' \
    <<<"$EXECUTION_ROW" >/dev/null || fail "Typed execution projection is not correlated"
pass "route_execution_results contains the same UUID and terminal attempt history"
printf '%s\n' "$EXECUTION_ROW" | jq .

section "TYPED TASK-TAG CORRELATION"
TAG_SQL="
SELECT toString(request_id) AS request_id, task_id, output_kind,
       taxonomy_version, tagger_model, tagger_version, tag_status,
       primary_category, primary_score
FROM ${CLICKHOUSE_DATABASE}.task_tags FINAL
WHERE request_id = toUUID('${SMOKE_UUID}')
LIMIT 1
FORMAT JSONEachRow
"
TAG_ROW="$(wait_typed_row task_tags "$SMOKE_UUID" "$TAG_WAIT_SECONDS" "$TAG_SQL")"
assert_one_typed_row task_tags
jq -e --arg id "$SMOKE_UUID" '.request_id == $id' <<<"$TAG_ROW" >/dev/null \
    || fail "Typed task-tag projection has a mismatched request ID"
pass "task_tags contains the same UUID"
printf '%s\n' "$TAG_ROW" | jq .

# Raw rows are diagnostics only. They are never the primary correlation join.
RAW_ROUTE_COUNT="$(ch_query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.route_records_raw WHERE JSONExtractString(line, 'request_id')='${SMOKE_UUID}' FORMAT TabSeparated")"
RAW_TAG_COUNT="$(ch_query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.task_tags_raw WHERE JSONExtractString(line, 'request_id')='${SMOKE_UUID}' FORMAT TabSeparated")"
info "Raw diagnostic rows: route_records_raw=${RAW_ROUTE_COUNT}, task_tags_raw=${RAW_TAG_COUNT}"
pass "Header-only request correlation verified across router response and all typed telemetry projections"
