#!/usr/bin/env bash
#
# Apply the complete ClickHouse migration chain to an isolated scratch database,
# then prove that migration 006 translates historical execution telemetry.
#
# Requirements: Bash 3.2+, curl, jq, sed
# Optional: CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, AUTH_FILE
#
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
CLICKHOUSE_URL="${CLICKHOUSE_URL%/}"
AUTH_FILE="${AUTH_FILE:-${PROJECT_ROOT}/.auth}"
DATABASE="will_it_jev_migration_smoke_$$_${RANDOM}"

fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }
pass() { printf '[PASS] %s\n' "$*"; }

for cmd in curl jq sed; do
    command -v "$cmd" >/dev/null 2>&1 || fail "Required command not found: $cmd"
done

if [[ -f "$AUTH_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$AUTH_FILE"
    set +a
fi

curl_ch() {
    if [[ -n "${CLICKHOUSE_USER:-}" ]]; then
        curl -sS --fail-with-body --connect-timeout 3 --max-time 120 \
            --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD:-}" "$@"
    else
        curl -sS --fail-with-body --connect-timeout 3 --max-time 120 "$@"
    fi
}

query() {
    local sql="$1"
    local encoded
    encoded="$(jq -rn --arg q "$sql" '$q | @uri')"
    curl_ch --request POST "${CLICKHOUSE_URL}/?query=${encoded}"
}

migration() {
    local path="$1"
    local sql_file statements_file statement statement_number
    sql_file="$(mktemp)"
    statements_file="$(mktemp)"
    sed "s/llm_observability/${DATABASE}/g" "$path" > "$sql_file"

    # The ClickHouse HTTP interface accepts one SQL statement per request.
    # Split outside quoted strings/identifiers and SQL comments; semicolons in
    # comments or string literals must not terminate a statement.
    awk '
        BEGIN { state = "normal"; sql = "" }
        function emit_statement() {
            sub(/^[[:space:]]+/, "", sql)
            if (length(sql) > 0) {
                printf "%s%c", sql, 30
            }
            sql = ""
        }
        {
            line = $0 "\n"
            for (i = 1; i <= length(line); i++) {
                ch = substr(line, i, 1)
                next_ch = substr(line, i + 1, 1)

                if (state == "line_comment") {
                    if (ch == "\n") {
                        state = "normal"
                        sql = sql " "
                    }
                    continue
                }
                if (state == "block_comment") {
                    if (ch == "*" && next_ch == "/") {
                        state = "normal"
                        i++
                    }
                    continue
                }
                if (state == "single_quote") {
                    sql = sql ch
                    if (ch == "\\") {
                        if (i < length(line)) {
                            sql = sql substr(line, i + 1, 1)
                            i++
                        }
                    } else if (ch == "\047") {
                        if (next_ch == "\047") {
                            sql = sql next_ch
                            i++
                        } else {
                            state = "normal"
                        }
                    }
                    continue
                }
                if (state == "double_quote" || state == "backtick") {
                    sql = sql ch
                    terminator = (state == "double_quote" ? "\042" : "`")
                    if (ch == terminator) {
                        if (next_ch == terminator) {
                            sql = sql next_ch
                            i++
                        } else {
                            state = "normal"
                        }
                    }
                    continue
                }

                if (ch == "-" && next_ch == "-") {
                    state = "line_comment"
                    i++
                    continue
                }
                if (ch == "/" && next_ch == "*") {
                    state = "block_comment"
                    i++
                    continue
                }
                if (ch == "\047") {
                    state = "single_quote"
                    sql = sql ch
                    continue
                }
                if (ch == "\042") {
                    state = "double_quote"
                    sql = sql ch
                    continue
                }
                if (ch == "`") {
                    state = "backtick"
                    sql = sql ch
                    continue
                }
                if (ch == ";") {
                    emit_statement()
                    continue
                }
                sql = sql (ch == "\n" ? " " : ch)
            }
        }
        END {
            if (state == "single_quote" || state == "double_quote" ||
                state == "backtick" || state == "block_comment") {
                exit 2
            }
            emit_statement()
        }
    ' "$sql_file" > "$statements_file" || {
        rm -f "$sql_file" "$statements_file"
        fail "Could not split SQL statements in $path"
    }

    statement_number=0
    while IFS= read -r -d $'\036' statement; do
        statement_number=$((statement_number + 1))
        if ! curl_ch --request POST "${CLICKHOUSE_URL}/" --data-binary "$statement" >/dev/null; then
            rm -f "$sql_file" "$statements_file"
            fail "Statement $statement_number failed in migration $(basename "$path")"
        fi
    done < "$statements_file"

    rm -f "$sql_file" "$statements_file"
    [[ "$statement_number" -gt 0 ]] || fail "Migration contains no SQL statements: $path"
}
insert_jsonl() {
    local path="$1"
    local sql="INSERT INTO ${DATABASE}.route_records_raw FORMAT JSONAsString"
    local encoded line line_number response
    encoded="$(jq -rn --arg q "$sql" '$q | @uri')"
    line_number=0
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ -z "$line" ]] && continue
        line_number=$((line_number + 1))
        if ! response="$(curl_ch --request POST "${CLICKHOUSE_URL}/?query=${encoded}" --data-binary "$line" 2>&1)"; then
            printf '%s\n' "$response" >&2
            fail "Could not insert JSON fixture line $line_number from $path"
        fi
    done < "$path"
}
cleanup() {
    local sql="DROP DATABASE IF EXISTS ${DATABASE} SYNC"
    local encoded
    encoded="$(jq -rn --arg q "$sql" '$q | @uri')"
    curl_ch --request POST "${CLICKHOUSE_URL}/?query=${encoded}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

curl_ch "${CLICKHOUSE_URL}/ping" >/dev/null || fail "ClickHouse is unavailable at ${CLICKHOUSE_URL}"
pass "ClickHouse is reachable"

for migration_name in \
    001_route_decisions.sql \
    002_task_tags.sql \
    003_task_decisions.sql \
    004_tier_names.sql \
    005_tier_outcomes.sql
do
    migration "${PROJECT_ROOT}/platform/clickhouse/init/${migration_name}" \
        || fail "Migration failed: ${migration_name}"
    pass "Applied ${migration_name}"
done

# These records enter the append-only raw table only after migrations 001-005,
# simulating rows written by the previous router version before migration 006.
insert_jsonl "${PROJECT_ROOT}/platform/clickhouse/init/006_execution_results.fixture.jsonl"
migration "${PROJECT_ROOT}/platform/clickhouse/init/006_execution_results.sql" \
    || fail "Migration failed: 006_execution_results.sql"
pass "Applied 006_execution_results.sql"

# New-format records are written after the materialized view is installed.
insert_jsonl "${PROJECT_ROOT}/tests/clickhouse/006_execution_results.new.fixture.jsonl"

ROWS="$(query "SELECT toString(request_id) AS request_id, cascade_decision_tier, decided_by_tier_name, dispatch_attempts_json, final_strategy, final_provider, final_model_id, final_status, final_failure_reason FROM ${DATABASE}.route_execution_results FINAL ORDER BY request_id FORMAT JSONEachRow")" \
    || fail "Could not read canonical execution projection"
[[ -n "$ROWS" ]] || fail "Migration 006 produced no execution projection rows"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f1" and
    .cascade_decision_tier == 3 and
    .decided_by_tier_name == "encoder" and
    .final_strategy == "decision" and
    .final_provider == "ollaya" and
    .final_model_id == null and
    ((.dispatch_attempts_json | fromjson)[0].strategy == "decision") and
    ((.dispatch_attempts_json | fromjson)[0].model_id == null)
  )
' <<<"$ROWS" >/dev/null || fail "Historical Decision/modernbert mapping or unknown model identity is incorrect"
pass "Historical tier5/jev maps to decision/ollaya; model identity remains NULL; modernbert maps to encoder"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f2" and
    .cascade_decision_tier == 6 and
    .decided_by_tier_name == "fallback" and
    .final_strategy == "generative" and
    .final_provider == "ollama" and
    .final_model_id == null and
    ((.dispatch_attempts_json | fromjson | length) == 2) and
    ((.dispatch_attempts_json | fromjson)[0].strategy == "decision") and
    ((.dispatch_attempts_json | fromjson)[0].outcome == "rejected") and
    ((.dispatch_attempts_json | fromjson)[1].strategy == "generative") and
    ((.dispatch_attempts_json | fromjson)[1].provider == "ollama")
  )
' <<<"$ROWS" >/dev/null || fail "Historical fallback/attempt mapping is incorrect"
pass "Historical cascade_decision_tier=6 remains fallback; tier6/llm maps to generative/ollama"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f3" and
    .cascade_decision_tier == 2 and
    .decided_by_tier_name == "fasttext" and
    .final_strategy == "generative" and
    .final_provider == "ollama" and
    .final_model_id == null and
    ((.dispatch_attempts_json | fromjson | length) == 1)
  )
' <<<"$ROWS" >/dev/null || fail "Historical direct generative mapping is incorrect"
pass "Historical direct llm execution maps to generative/ollama without inventing model identity"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f4" and
    .cascade_decision_tier == 2 and
    .decided_by_tier_name == "fasttext" and
    .final_strategy == "generative" and
    .final_provider == "ollama" and
    .final_model_id == "gemma3:latest" and
    ((.dispatch_attempts_json | fromjson)[0].model_id == "gemma3:latest")
  )
' <<<"$ROWS" >/dev/null || fail "New-format dispatch projection failed to retain configured model identity"
pass "New records project their explicit strategy, provider, model ID and dispatch attempt"

printf '\n[PASS] ClickHouse migration smoke completed.\n'
\\036' statement; do
        statement_number=$((statement_number + 1))
        if ! curl_ch --request POST "${CLICKHOUSE_URL}/" --data-binary "$statement" >/dev/null; then
            rm -f "$sql_file" "$statements_file"
            fail "Statement $statement_number failed in migration $(basename "$path")"
        fi
    done < "$statements_file"

    rm -f "$sql_file" "$statements_file"
    [[ "$statement_number" -gt 0 ]] || fail "Migration contains no SQL statements: $path"
}

insert_jsonl() {
    local path="$1"
    local sql="INSERT INTO ${DATABASE}.route_records_raw FORMAT JSONAsString"
    local encoded
    encoded="$(jq -rn --arg q "$sql" '$q | @uri')"
    curl_ch --request POST "${CLICKHOUSE_URL}/?query=${encoded}" --data-binary "@${path}" >/dev/null
}

cleanup() {
    local sql="DROP DATABASE IF EXISTS ${DATABASE} SYNC"
    local encoded
    encoded="$(jq -rn --arg q "$sql" '$q | @uri')"
    curl_ch --request POST "${CLICKHOUSE_URL}/?query=${encoded}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

curl_ch "${CLICKHOUSE_URL}/ping" >/dev/null || fail "ClickHouse is unavailable at ${CLICKHOUSE_URL}"
pass "ClickHouse is reachable"

for migration_name in \
    001_route_decisions.sql \
    002_task_tags.sql \
    003_task_decisions.sql \
    004_tier_names.sql \
    005_tier_outcomes.sql
do
    migration "${PROJECT_ROOT}/platform/clickhouse/init/${migration_name}" \
        || fail "Migration failed: ${migration_name}"
    pass "Applied ${migration_name}"
done

# These records enter the append-only raw table only after migrations 001-005,
# simulating rows written by the previous router version before migration 006.
insert_jsonl "${PROJECT_ROOT}/platform/clickhouse/init/006_execution_results.fixture.jsonl"
migration "${PROJECT_ROOT}/platform/clickhouse/init/006_execution_results.sql" \
    || fail "Migration failed: 006_execution_results.sql"
pass "Applied 006_execution_results.sql"

# New-format records are written after the materialized view is installed.
insert_jsonl "${PROJECT_ROOT}/tests/clickhouse/006_execution_results.new.fixture.jsonl"

ROWS="$(query "SELECT toString(request_id) AS request_id, cascade_decision_tier, decided_by_tier_name, dispatch_attempts_json, final_strategy, final_provider, final_model_id, final_status, final_failure_reason FROM ${DATABASE}.route_execution_results FINAL ORDER BY request_id FORMAT JSONEachRow")" \
    || fail "Could not read canonical execution projection"
[[ -n "$ROWS" ]] || fail "Migration 006 produced no execution projection rows"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f1" and
    .cascade_decision_tier == 3 and
    .decided_by_tier_name == "encoder" and
    .final_strategy == "decision" and
    .final_provider == "ollaya" and
    .final_model_id == null and
    ((.dispatch_attempts_json | fromjson)[0].strategy == "decision") and
    ((.dispatch_attempts_json | fromjson)[0].model_id == null)
  )
' <<<"$ROWS" >/dev/null || fail "Historical Decision/modernbert mapping or unknown model identity is incorrect"
pass "Historical tier5/jev maps to decision/ollaya; model identity remains NULL; modernbert maps to encoder"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f2" and
    .cascade_decision_tier == 6 and
    .decided_by_tier_name == "fallback" and
    .final_strategy == "generative" and
    .final_provider == "ollama" and
    .final_model_id == null and
    ((.dispatch_attempts_json | fromjson | length) == 2) and
    ((.dispatch_attempts_json | fromjson)[0].strategy == "decision") and
    ((.dispatch_attempts_json | fromjson)[0].outcome == "rejected") and
    ((.dispatch_attempts_json | fromjson)[1].strategy == "generative") and
    ((.dispatch_attempts_json | fromjson)[1].provider == "ollama")
  )
' <<<"$ROWS" >/dev/null || fail "Historical fallback/attempt mapping is incorrect"
pass "Historical cascade_decision_tier=6 remains fallback; tier6/llm maps to generative/ollama"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f3" and
    .cascade_decision_tier == 2 and
    .decided_by_tier_name == "fasttext" and
    .final_strategy == "generative" and
    .final_provider == "ollama" and
    .final_model_id == null and
    ((.dispatch_attempts_json | fromjson | length) == 1)
  )
' <<<"$ROWS" >/dev/null || fail "Historical direct generative mapping is incorrect"
pass "Historical direct llm execution maps to generative/ollama without inventing model identity"

jq -s -e '
  any(.[];
    .request_id == "00000000-0000-4000-8000-0000000000f4" and
    .cascade_decision_tier == 2 and
    .decided_by_tier_name == "fasttext" and
    .final_strategy == "generative" and
    .final_provider == "ollama" and
    .final_model_id == "gemma3:latest" and
    ((.dispatch_attempts_json | fromjson)[0].model_id == "gemma3:latest")
  )
' <<<"$ROWS" >/dev/null || fail "New-format dispatch projection failed to retain configured model identity"
pass "New records project their explicit strategy, provider, model ID and dispatch attempt"

printf '\n[PASS] ClickHouse migration smoke completed.\n'
