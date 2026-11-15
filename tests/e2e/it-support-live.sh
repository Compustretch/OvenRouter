#!/usr/bin/env bash
#
# Live IT-support E2E acceptance test.
#
# Exercises the real configured Decision/Ollaya and Generative/Ollama paths,
# the configured 20-leaf help-desk taxonomy, and typed ClickHouse correlation.
# All correlation IDs are generated independently and sent only in X-Request-ID.
#
# Requirements: Bash 3.2+, curl, jq, uuidgen
# Router, Ollaya, Ollama, ClickHouse and migrations 001-006 must be running.
#
# Optional environment:
#   ROUTER_URL, CLICKHOUSE_URL, AUTH_FILE, TASK_TAXONOMY
#   DECISION_MODEL (default laya:en), GENERATIVE_MODEL (default gemma3:latest)
#   ROUTER_TIMEOUT_SECONDS, ROUTE_WAIT_SECONDS, TAG_WAIT_SECONDS
#
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
ROUTER_URL="${ROUTER_URL:-http://127.0.0.1:8080/v1/decisions}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
CLICKHOUSE_URL="${CLICKHOUSE_URL%/}"
AUTH_FILE="${AUTH_FILE:-${PROJECT_ROOT}/.auth}"
TAXONOMY_FILE="${TASK_TAXONOMY:-${PROJECT_ROOT}/taskonomies/help_desk.json}"
DECISION_MODEL="${DECISION_MODEL:-laya:en}"
GENERATIVE_MODEL="${GENERATIVE_MODEL:-gemma3:latest}"
ROUTER_TIMEOUT_SECONDS="${ROUTER_TIMEOUT_SECONDS:-150}"
ROUTE_WAIT_SECONDS="${ROUTE_WAIT_SECONDS:-60}"
TAG_WAIT_SECONDS="${TAG_WAIT_SECONDS:-60}"
CLICKHOUSE_DATABASE="llm_observability"
RESPONSE_FILE="$(mktemp)"
HEADER_FILE="$(mktemp)"
FAILURES=0

cleanup() { rm -f "$RESPONSE_FILE" "$HEADER_FILE"; }
trap cleanup EXIT
fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }
pass() { printf '[PASS] %s\n' "$*"; }
info() { printf '[INFO] %s\n' "$*"; }
record_failure() { printf '[FAIL] %s\n' "$*" >&2; FAILURES=$((FAILURES + 1)); }
section() { printf '\n================================================================\n  %s\n================================================================\n' "$1"; }

for cmd in curl jq uuidgen awk; do
    command -v "$cmd" >/dev/null 2>&1 || fail "Required command not found: $cmd"
done
[[ -f "$TAXONOMY_FILE" ]] || fail "Taxonomy file not found: $TAXONOMY_FILE"

if [[ -f "$AUTH_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$AUTH_FILE"
    set +a
fi

TAXONOMY_LEAVES="$(jq -c '[.categories[].name] | sort' "$TAXONOMY_FILE")"
LEAF_COUNT="$(jq -r 'length' <<<"$TAXONOMY_LEAVES")"
UNIQUE_LEAF_COUNT="$(jq -r 'unique | length' <<<"$TAXONOMY_LEAVES")"
[[ "$LEAF_COUNT" == "20" && "$UNIQUE_LEAF_COUNT" == "20" ]] \
    || fail "Expected exactly 20 unique taxonomy leaves; found $LEAF_COUNT ($UNIQUE_LEAF_COUNT unique)"
TAXONOMY_CRITERIA="$(jq -c '[.categories[] | {key: .name, value: .classifier_criterion}] | from_entries' "$TAXONOMY_FILE")"
pass "Loaded configured taxonomy with exactly 20 unique leaf categories"

ch_query() {
    local query="$1"
    if [[ -n "${CLICKHOUSE_USER:-}" ]]; then
        curl -sS --fail-with-body --connect-timeout 3 --max-time 15 \
            --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD:-}" \
            --get --data-urlencode "query=${query}" "${CLICKHOUSE_URL}/"
    else
        curl -sS --fail-with-body --connect-timeout 3 --max-time 15 \
            --get --data-urlencode "query=${query}" "${CLICKHOUSE_URL}/"
    fi
}

new_request_id() {
    local request_id
    request_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
    [[ "$request_id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$ ]] \
        || fail "uuidgen did not produce a canonical UUIDv4: $request_id"
    printf '%s' "$request_id"
}

assert_response_request_id() {
    local expected="$1"
    local actual
    actual="$(awk 'tolower($1) == "x-request-id:" { gsub("\r", "", $2); value=$2 } END { print value }' "$HEADER_FILE")"
    [[ "$actual" == "$expected" ]] \
        || fail "Response X-Request-ID mismatch: expected $expected, received ${actual:-missing}"
    pass "Response X-Request-ID echoed $expected"
}

assert_leaf_category() {
    local category="$1"
    local expected="$2"
    jq -e --arg category "$category" --argjson leaves "$TAXONOMY_LEAVES" \
        '($leaves | index($category)) != null' <<<"null" >/dev/null \
        || record_failure "Category '$category' is not a leaf in the configured taxonomy"
    [[ "$category" == "$expected" ]] \
        || record_failure "Expected taxonomy leaf '$expected', received '${category:-empty}'"
}

wait_route_record() {
    local request_id="$1"
    local expected_prompt="$2"
    local result="" sql="
SELECT toString(request_id) AS request_id, prompt_raw,
       decided_by_tier, decided_by_tier_name, decided_backend, final_backend,
       downstream_tier, downstream_outcome, is_failover, failure_reason,
       total_latency_us
FROM ${CLICKHOUSE_DATABASE}.route_decisions FINAL
WHERE request_id = toUUID('${request_id}')
LIMIT 1
FORMAT JSONEachRow
"
    local i
    for ((i = 1; i <= ROUTE_WAIT_SECONDS; i++)); do
        result="$(ch_query "$sql")" || fail "Typed route_decisions query failed for $request_id"
        [[ -n "$result" ]] && break
        sleep 1
    done
    [[ -n "$result" ]] || fail "No typed route_decisions row appeared for $request_id"
    jq -e --arg id "$request_id" --arg prompt "$expected_prompt" \
        '.request_id == $id and .prompt_raw == $prompt' <<<"$result" >/dev/null \
        || fail "Typed route_decisions request ID or clean prompt mismatch for $request_id"
    printf '%s' "$result"
}

wait_tag_record() {
    local request_id="$1"
    local result="" sql="
SELECT toString(request_id) AS request_id, task_id, output_kind,
       taxonomy_version, tagger_model, tagger_version, tag_status,
       primary_category, primary_score
FROM ${CLICKHOUSE_DATABASE}.task_tags FINAL
WHERE request_id = toUUID('${request_id}')
LIMIT 1
FORMAT JSONEachRow
"
    local i
    for ((i = 1; i <= TAG_WAIT_SECONDS; i++)); do
        result="$(ch_query "$sql")" || fail "Typed task_tags query failed for $request_id"
        [[ -n "$result" ]] && break
        sleep 1
    done
    [[ -n "$result" ]] || fail "No typed task_tags row appeared for $request_id"
    jq -e --arg id "$request_id" '.request_id == $id' <<<"$result" >/dev/null \
        || fail "Typed task_tags request ID mismatch for $request_id"
    printf '%s' "$result"
}

wait_execution_projection() {
    local request_id="$1"
    local result="" sql="
SELECT toString(request_id) AS request_id, cascade_decision_tier,
       decided_by_tier_name, dispatch_attempts_json, final_strategy,
       final_provider, final_model_id, final_status, final_failure_reason
FROM ${CLICKHOUSE_DATABASE}.route_execution_results FINAL
WHERE request_id = toUUID('${request_id}')
LIMIT 1
FORMAT JSONEachRow
"
    local i
    for ((i = 1; i <= ROUTE_WAIT_SECONDS; i++)); do
        result="$(ch_query "$sql")" || fail "Typed execution projection query failed for $request_id"
        [[ -n "$result" ]] && break
        sleep 1
    done
    [[ -n "$result" ]] || fail "No route_execution_results row appeared for $request_id"
    jq -e --arg id "$request_id" '.request_id == $id' <<<"$result" >/dev/null \
        || fail "Typed route_execution_results request ID mismatch for $request_id"
    printf '%s' "$result"
}

assert_common_tag() {
    local request_id="$1"
    local expected="$2"
    local tag_row status category
    tag_row="$(wait_tag_record "$request_id")"
    status="$(jq -r '.tag_status // empty' <<<"$tag_row")"
    category="$(jq -r '.primary_category // empty' <<<"$tag_row")"
    [[ "$status" == "tagged" ]] || record_failure "Tagger did not return tagged status for $request_id (status=$status)"
    assert_leaf_category "$category" "$expected"
    pass "Typed task_tags correlated to $request_id and tagged as leaf $expected"
    printf '%s\n' "$tag_row" | jq .
}

assert_clickhouse_strategy() {
    local request_id="$1"
    local expected_strategy="$2"
    local expected_provider="$3"
    local expected_model="$4"
    local row
    row="$(wait_execution_projection "$request_id")"
    jq -e --arg strategy "$expected_strategy" --arg provider "$expected_provider" \
        --arg model "$expected_model" '
        .final_strategy == $strategy and
        .final_provider == $provider and
        .final_model_id == $model and
        .final_status == "success" and
        (.dispatch_attempts_json | fromjson | length >= 1)
    ' <<<"$row" >/dev/null \
        || record_failure "Typed execution projection mismatch for $request_id (expected $expected_strategy/$expected_provider/$expected_model)"
    pass "Typed execution result persisted: $expected_strategy / $expected_provider / $expected_model"
    printf '%s\n' "$row" | jq .
}

section "PREFLIGHT"
curl -sS --fail-with-body --connect-timeout 3 --max-time 5 "$CLICKHOUSE_URL/ping" >/dev/null \
    || fail "ClickHouse is unreachable at $CLICKHOUSE_URL"
ch_query "SELECT 1 FORMAT TabSeparated" >/dev/null || fail "ClickHouse authentication failed"
for table in route_decisions task_tags route_execution_results; do
    ch_query "SELECT 1 FROM system.tables WHERE database='${CLICKHOUSE_DATABASE}' AND name='${table}' FORMAT TabSeparated" \
        | grep -q '^1$' || fail "Required ClickHouse projection is missing: $table"
done
ROUTER_HEALTH_URL="${ROUTER_HEALTH_URL:-${ROUTER_URL%/v1/decisions}/healthz}"
curl -sS --fail-with-body --connect-timeout 3 --max-time 5 "$ROUTER_HEALTH_URL" >/dev/null 2>&1 \
    || fail "Router health check failed at $ROUTER_HEALTH_URL"
info "Router URL: $ROUTER_URL"
info "Expected Decision model: $DECISION_MODEL"
info "Expected Generative model: $GENERATIVE_MODEL"
pass "Router dependencies and typed ClickHouse projections are reachable"

section "ACCOUNT / PASSWORD — REAL DECISION PATH"
ACCOUNT_REQUEST_ID="$(new_request_id)"
ACCOUNT_STATE="I forgot my work password and my account is locked. Please help me reset my password so I can sign in."
ACCOUNT_BODY="$(jq -cn --arg prompt "$ACCOUNT_STATE" --arg state "$ACCOUNT_STATE" \
    --argjson criteria "$TAXONOMY_CRITERIA" '
    {
      prompt: $prompt,
      state: $state,
      questions: {
        category: {
          type: "choice",
          instructions: "Classify this internal IT support request using exactly one leaf from the configured help-desk taxonomy. Choose the most specific defensible category.",
          criteria: $criteria
        }
      }
    }
')"
: > "$HEADER_FILE"
HTTP_STATUS="$(curl -sS --connect-timeout 3 --max-time "$ROUTER_TIMEOUT_SECONDS" \
    --dump-header "$HEADER_FILE" --output "$RESPONSE_FILE" --write-out '%{http_code}' \
    --request POST --header 'Content-Type: application/json' \
    --header "X-Request-ID: ${ACCOUNT_REQUEST_ID}" \
    --data-binary "$ACCOUNT_BODY" "$ROUTER_URL")" || fail "Account/password request did not complete"
[[ "$HTTP_STATUS" == "200" ]] || fail "Account/password request returned HTTP $HTTP_STATUS"
assert_response_request_id "$ACCOUNT_REQUEST_ID"
jq -e '.done_reason == "decide" and .state_truncated == false and .answers.category.type == "choice"' \
    "$RESPONSE_FILE" >/dev/null || fail "Account/password request did not return a completed typed Ollaya decision"
[[ "$(jq -r '.model // empty' "$RESPONSE_FILE")" == "$DECISION_MODEL" ]] \
    || fail "Decision model mismatch: expected $DECISION_MODEL, got $(jq -r '.model // "missing"' "$RESPONSE_FILE")"
ACCOUNT_CATEGORY="$(jq -r '.answers.category.choice // empty' "$RESPONSE_FILE")"
assert_leaf_category "$ACCOUNT_CATEGORY" "account_pw"
jq -e 'has("answers") and ([.answers | keys[] | startswith("__router_")] | any | not)' "$RESPONSE_FILE" >/dev/null \
    || fail "Internal eligibility answer leaked into the caller response"

ACCOUNT_ROUTE="$(wait_route_record "$ACCOUNT_REQUEST_ID" "$ACCOUNT_STATE")"
pass "route_decisions contains the header UUID and clean account/password prompt"
assert_clickhouse_strategy "$ACCOUNT_REQUEST_ID" "decision" "ollaya" "$DECISION_MODEL"
assert_common_tag "$ACCOUNT_REQUEST_ID" "account_pw"

section "VPN / CONNECTIVITY — REAL GENERATIVE PATH"
VPN_REQUEST_ID="$(new_request_id)"
VPN_PROMPT="I cannot connect to the corporate VPN from home. The connection fails every time, so I cannot reach internal systems. Please help me troubleshoot."
VPN_BODY="$(jq -cn --arg prompt "$VPN_PROMPT" '{prompt:$prompt}')"
: > "$HEADER_FILE"
HTTP_STATUS="$(curl -sS --connect-timeout 3 --max-time "$ROUTER_TIMEOUT_SECONDS" \
    --dump-header "$HEADER_FILE" --output "$RESPONSE_FILE" --write-out '%{http_code}' \
    --request POST --header 'Content-Type: application/json' \
    --header "X-Request-ID: ${VPN_REQUEST_ID}" \
    --data-binary "$VPN_BODY" "$ROUTER_URL")" || fail "VPN request did not complete"
[[ "$HTTP_STATUS" == "200" ]] || fail "VPN request returned HTTP $HTTP_STATUS"
assert_response_request_id "$VPN_REQUEST_ID"
jq -e '(.response | type == "string" and length > 0) or (.choices[0].message.content | type == "string" and length > 0)' \
    "$RESPONSE_FILE" >/dev/null || fail "VPN request did not return a generative response"
[[ "$(jq -r '.model // empty' "$RESPONSE_FILE")" == "$GENERATIVE_MODEL" ]] \
    || fail "Generative model mismatch: expected $GENERATIVE_MODEL, got $(jq -r '.model // "missing"' "$RESPONSE_FILE")"

VPN_ROUTE="$(wait_route_record "$VPN_REQUEST_ID" "$VPN_PROMPT")"
pass "route_decisions contains the header UUID and clean VPN prompt"
assert_clickhouse_strategy "$VPN_REQUEST_ID" "generative" "ollama" "$GENERATIVE_MODEL"
assert_common_tag "$VPN_REQUEST_ID" "nw_vpn"

section "RESULT"
[[ "$FAILURES" -eq 0 ]] || fail "Live IT-support E2E completed with $FAILURES assertion failure(s)"
pass "Both real downstream strategies, the 20-leaf taxonomy, response headers, and typed ClickHouse projections passed"
