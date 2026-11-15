#!/usr/bin/env bash
#
# Live IT-support E2E acceptance test.
#
# Exercises the real configured Decision/Ollaya and Generative/Ollama paths,
# the configured 20-leaf help-desk taxonomy, and correlated ClickHouse records.
# This is additive: scripts/smoke-jev-ch.sh remains the arithmetic/ClickHouse
# smoke test and is deliberately not replaced by this scenario suite.
#
# Requirements: Bash 3.2+, curl, jq
# The router, Ollaya and Ollama must already be running. ClickHouse migrations
# 001-006 must be applied.
#
# Optional environment:
#   ROUTER_URL, CLICKHOUSE_URL, AUTH_FILE, TASK_TAXONOMY
#   DECISION_MODEL (default laya:en), GENERATIVE_MODEL (default gemma3:latest)
#   ROUTER_TIMEOUT_SECONDS, ROUTE_WAIT_SECONDS, TAG_WAIT_SECONDS
#
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
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
FAILURES=0

cleanup() { rm -f "$RESPONSE_FILE"; }
trap cleanup EXIT
fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }
pass() { printf '[PASS] %s\n' "$*"; }
info() { printf '[INFO] %s\n' "$*"; }
record_failure() { printf '[FAIL] %s\n' "$*" >&2; FAILURES=$((FAILURES + 1)); }

for cmd in curl jq; do
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
    local key="$1"
    local result=""
    local sql="
SELECT line
FROM ${CLICKHOUSE_DATABASE}.route_records_raw
WHERE position(JSONExtractString(line, 'prompt_raw'), '${key}') > 0
ORDER BY _part DESC, _part_offset DESC
LIMIT 1
FORMAT TabSeparatedRaw
"
    local i
    for ((i = 1; i <= ROUTE_WAIT_SECONDS; i++)); do
        result="$(ch_query "$sql")" || fail "ClickHouse route-record query failed for $key"
        [[ -n "$result" ]] && { printf '%s' "$result"; return 0; }
        sleep 1
    done
    fail "No route record appeared for $key within ${ROUTE_WAIT_SECONDS}s"
}

wait_tag_record() {
    local request_id="$1"
    local result=""
    local sql="
SELECT line
FROM ${CLICKHOUSE_DATABASE}.task_tags_raw
WHERE JSONExtractString(line, 'request_id') = '${request_id}'
ORDER BY _part DESC, _part_offset DESC
LIMIT 1
FORMAT TabSeparatedRaw
"
    local i
    for ((i = 1; i <= TAG_WAIT_SECONDS; i++)); do
        result="$(ch_query "$sql")" || fail "ClickHouse task-tag query failed for $request_id"
        [[ -n "$result" ]] && { printf '%s' "$result"; return 0; }
        sleep 1
    done
    fail "No task-tag record appeared for $request_id within ${TAG_WAIT_SECONDS}s"
}

wait_execution_projection() {
    local request_id="$1"
    local result=""
    local sql="
SELECT toString(request_id) AS request_id, cascade_decision_tier,
       decided_by_tier_name, dispatch_attempts_json, final_strategy,
       final_provider, final_model_id, final_status
FROM ${CLICKHOUSE_DATABASE}.route_execution_results FINAL
WHERE request_id = toUUID('${request_id}')
FORMAT JSONEachRow
"
    local i
    for ((i = 1; i <= ROUTE_WAIT_SECONDS; i++)); do
        result="$(ch_query "$sql")" || fail "ClickHouse execution projection query failed for $request_id"
        [[ -n "$result" ]] && { printf '%s' "$result"; return 0; }
        sleep 1
    done
    fail "No route_execution_results row appeared for $request_id within ${ROUTE_WAIT_SECONDS}s"
}

assert_common_tag() {
    local key="$1"
    local request_id="$2"
    local expected="$3"
    local tag_line
    tag_line="$(wait_tag_record "$request_id")"
    jq -e . <<<"$tag_line" >/dev/null || fail "Invalid task_tags_raw JSON for $key"
    local category status
    category="$(jq -r '.result.primary.category_id // empty' <<<"$tag_line")"
    status="$(jq -r '.result.status // empty' <<<"$tag_line")"
    [[ "$(jq -r '.request_id' <<<"$tag_line")" == "$request_id" ]] \
        || record_failure "Task-tag request ID does not correlate for $key"
    [[ "$status" == "tagged" ]] || record_failure "Tagger did not return tagged status for $key (status=$status)"
    assert_leaf_category "$category" "$expected"
    pass "$key tagged as taxonomy leaf $expected"
}

assert_clickhouse_strategy() {
    local route_json="$1"
    local request_id cascade_tier cascade_name expected_strategy expected_provider expected_model
    request_id="$(jq -r '.request_id' <<<"$route_json")"
    cascade_tier="$(jq -r '.cascade_decision_tier' <<<"$route_json")"
    cascade_name="$(jq -r '.decided_by_tier_name' <<<"$route_json")"
    expected_strategy="$(jq -r '.final_result.strategy' <<<"$route_json")"
    expected_provider="$(jq -r '.final_result.provider' <<<"$route_json")"
    expected_model="$(jq -r '.final_result.model_id' <<<"$route_json")"
    local row attempts
    row="$(wait_execution_projection "$request_id")"
    jq -e --arg strategy "$expected_strategy" --arg provider "$expected_provider" \
        --arg model "$expected_model" --arg cascade_name "$cascade_name" \
        --argjson cascade_tier "$cascade_tier" '
        .final_strategy == $strategy and
        .final_provider == $provider and
        .final_model_id == $model and
        .decided_by_tier_name == $cascade_name and
        .cascade_decision_tier == $cascade_tier
    ' <<<"$row" >/dev/null || record_failure "ClickHouse execution projection mismatch for $request_id"
    attempts="$(jq -c '.dispatch_attempts' <<<"$route_json")"
    jq -e --argjson expected "$attempts" '.dispatch_attempts_json | fromjson == $expected' \
        <<<"$row" >/dev/null || record_failure "ClickHouse dispatch attempts differ from route capture for $request_id"
    pass "ClickHouse execution result persisted and correlated: $expected_strategy / $expected_provider / $expected_model"
}

section() { printf '\n================================================================\n  %s\n================================================================\n' "$1"; }

section "PREFLIGHT"
curl -sS --fail-with-body --connect-timeout 3 --max-time 5 "$CLICKHOUSE_URL/ping" >/dev/null \
    || fail "ClickHouse is unreachable at $CLICKHOUSE_URL"
ch_query "SELECT 1 FORMAT TabSeparated" >/dev/null || fail "ClickHouse authentication failed"
ch_query "SELECT 1 FROM system.tables WHERE database='${CLICKHOUSE_DATABASE}' AND name='route_execution_results' FORMAT TabSeparated" \
    | grep -q '^1$' || fail "Migration 006 is not applied (route_execution_results missing)"
ROUTER_HEALTH_URL="${ROUTER_HEALTH_URL:-${ROUTER_URL%/v1/decisions}/healthz}"
curl -sS --fail-with-body --connect-timeout 3 --max-time 5 "$ROUTER_HEALTH_URL" >/dev/null 2>&1 \
    || fail "Router health check failed at $ROUTER_HEALTH_URL"
info "Router URL: $ROUTER_URL"
info "Expected Decision model: $DECISION_MODEL"
info "Expected Generative model: $GENERATIVE_MODEL"
pass "Router dependencies and ClickHouse execution projection are reachable"

if command -v uuidgen >/dev/null 2>&1; then
    SMOKE_KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')"
else
    SMOKE_KEY="$(date +%s)-$$-${RANDOM}${RANDOM}"
fi

section "ACCOUNT / PASSWORD — REAL DECISION PATH"
ACCOUNT_KEY="it-account-${SMOKE_KEY}"
ACCOUNT_STATE="I forgot my work password and my account is locked. Please help me reset my password so I can sign in. [${ACCOUNT_KEY}]"
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
HTTP_STATUS="$(curl -sS --connect-timeout 3 --max-time "$ROUTER_TIMEOUT_SECONDS" \
    --output "$RESPONSE_FILE" --write-out '%{http_code}' \
    --request POST --header 'Content-Type: application/json' \
    --data-binary "$ACCOUNT_BODY" "$ROUTER_URL")" || fail "Account/password request did not complete"
[[ "$HTTP_STATUS" == "200" ]] || fail "Account/password request returned HTTP $HTTP_STATUS"
jq -e '.done_reason == "decide" and .state_truncated == false and .answers.category.type == "choice"' \
    "$RESPONSE_FILE" >/dev/null || fail "Account/password request did not return a completed typed Ollaya decision"
[[ "$(jq -r '.model // empty' "$RESPONSE_FILE")" == "$DECISION_MODEL" ]] \
    || fail "Decision model mismatch: expected $DECISION_MODEL, got $(jq -r '.model // "missing"' "$RESPONSE_FILE")"
ACCOUNT_CATEGORY="$(jq -r '.answers.category.choice // empty' "$RESPONSE_FILE")"
assert_leaf_category "$ACCOUNT_CATEGORY" "account_pw"
jq -e 'has("answers") and ([.answers | keys[] | startswith("__router_")] | any | not)' "$RESPONSE_FILE" >/dev/null \
    || fail "Internal eligibility answer leaked into the caller response"

ACCOUNT_ROUTE="$(wait_route_record "$ACCOUNT_KEY")"
jq -e . <<<"$ACCOUNT_ROUTE" >/dev/null || fail "Invalid account/password route record"
ACCOUNT_REQUEST_ID="$(jq -r '.request_id' <<<"$ACCOUNT_ROUTE")"
jq -e --arg model "$DECISION_MODEL" '
    .dispatch_attempts | length == 1 and
    .[0].strategy == "decision" and .[0].provider == "ollaya" and
    .[0].model_id == $model and .[0].outcome == "success"
' <<<"$ACCOUNT_ROUTE" >/dev/null || fail "Account/password did not complete through one successful Decision attempt"
jq -e --arg model "$DECISION_MODEL" '
    .final_result.strategy == "decision" and
    .final_result.provider == "ollaya" and
    .final_result.model_id == $model and
    .final_result.status == "success"
' <<<"$ACCOUNT_ROUTE" >/dev/null || fail "Account/password final_result does not identify the real Ollaya Decision result"
pass "Account/password completed through Decision / Ollaya using $DECISION_MODEL"
assert_clickhouse_strategy "$ACCOUNT_ROUTE"
assert_common_tag "$ACCOUNT_KEY" "$ACCOUNT_REQUEST_ID" "account_pw"

section "VPN / CONNECTIVITY — REAL GENERATIVE PATH"
VPN_KEY="it-vpn-${SMOKE_KEY}"
VPN_PROMPT="I cannot connect to the corporate VPN from home. The connection fails every time, so I cannot reach internal systems. Please help me troubleshoot. [${VPN_KEY}]"
VPN_BODY="$(jq -cn --arg prompt "$VPN_PROMPT" '{prompt:$prompt}')"
HTTP_STATUS="$(curl -sS --connect-timeout 3 --max-time "$ROUTER_TIMEOUT_SECONDS" \
    --output "$RESPONSE_FILE" --write-out '%{http_code}' \
    --request POST --header 'Content-Type: application/json' \
    --data-binary "$VPN_BODY" "$ROUTER_URL")" || fail "VPN request did not complete"
[[ "$HTTP_STATUS" == "200" ]] || fail "VPN request returned HTTP $HTTP_STATUS"
jq -e '(.response | type == "string" and length > 0) or (.choices[0].message.content | type == "string" and length > 0)' \
    "$RESPONSE_FILE" >/dev/null || fail "VPN request did not return a generative response"
[[ "$(jq -r '.model // empty' "$RESPONSE_FILE")" == "$GENERATIVE_MODEL" ]] \
    || fail "Generative model mismatch: expected $GENERATIVE_MODEL, got $(jq -r '.model // "missing"' "$RESPONSE_FILE")"

VPN_ROUTE="$(wait_route_record "$VPN_KEY")"
jq -e . <<<"$VPN_ROUTE" >/dev/null || fail "Invalid VPN route record"
VPN_REQUEST_ID="$(jq -r '.request_id' <<<"$VPN_ROUTE")"
jq -e --arg model "$GENERATIVE_MODEL" '
    .dispatch_attempts | length == 1 and
    .[0].strategy == "generative" and .[0].provider == "ollama" and
    .[0].model_id == $model and .[0].outcome == "success"
' <<<"$VPN_ROUTE" >/dev/null || fail "VPN did not complete through one direct Generative attempt"
jq -e --arg model "$GENERATIVE_MODEL" '
    .final_result.strategy == "generative" and
    .final_result.provider == "ollama" and
    .final_result.model_id == $model and
    .final_result.status == "success"
' <<<"$VPN_ROUTE" >/dev/null || fail "VPN final_result does not identify the real Ollama Generative result"
pass "VPN request completed through Generative / Ollama using $GENERATIVE_MODEL"
assert_clickhouse_strategy "$VPN_ROUTE"
assert_common_tag "$VPN_KEY" "$VPN_REQUEST_ID" "nw_vpn"

section "RESULT"
[[ "$FAILURES" -eq 0 ]] || fail "Live IT-support E2E completed with $FAILURES assertion failure(s)"
pass "Both real downstream strategies, the 20-leaf taxonomy, and correlated ClickHouse projections passed"
pass "The arithmetic smoke test scripts/smoke-jev-ch.sh remains a separate, unchanged acceptance test"
