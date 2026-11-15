#!/usr/bin/env bash
#
# Apply migrations 001-006 to an isolated ClickHouse database.
# Verify historical compatibility mappings and new-format execution records.
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

for cmd in curl jq sed awk; do
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
    curl_ch --request POST "${CLICKHOUSE_URL}/" --data-binary "$sql"
}

migration() {
    local path="$1"
    local sql_file statements_file statement statement_number
    sql_file="$(mktemp)"
    statements_file="$(mktemp)"
    sed "s/llm_observability/${DATABASE}/g" "$path" > "$sql_file"

    # Split at semicolons only when outside SQL strings, quoted identifiers,
    # and comments. The record separator keeps multi-line SELECT/CREATE SQL
    # intact while allowing one HTTP request per statement.
    awk '
        BEGIN { state = "normal"; sql = "" }
        function emit_statement() {
            sub(/^[[:space:]]+/, "", sql)
            if (length(sql) > 0) printf "%s%c", sql, 30
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
                state == "backtick" || state == "block_comment") exit 2
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
    local sql="INSERT INTO ${DATABASE}.route_records_raw (line) FORMAT JSONAsString"
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
    curl_ch --request POST "${CLICKHOUSE_URL}/" --data-binary "$sql" >/dev/null 2>&1 || true
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

# Insert old-format records before migration 006 so its backfill must translate them.
insert_jsonl "${PROJECT_ROOT}/platform/clickhouse/init/006_execution_results.fixture.jsonl"
migration "${PROJECT_ROOT}/platform/clickhouse/init/006_execution_results.sql" \
    || fail "Migration failed: 006_execution_results.sql"
pass "Applied 006_execution_results.sql"

# New-format records are inserted after the materialized view exists.
insert_jsonl "${PROJECT_ROOT}/tests/clickhouse/006_execution_results.new.fixture.jsonl"

# Correlate a raw route event and raw task-tag event using one UUID. The route
# event above uses f4; attach a tag record with precisely that same request ID.
CORRELATION_UUID="00000000-0000-4000-8000-0000000000f4"
TAG_RECORD="$(jq -cn --arg id "$CORRELATION_UUID" '{
    request_id: $id,
    timestamp: "2026-11-15T10:00:04.000001Z",
    taxonomy_version: "test-v1",
    tagger_model: "test-tagger",
    tagger_version: "1.0.0",
    task_id: "tag",
    output_kind: "multiclass",
    latency_ms: 1.0,
    queue_wait_ms: 0.1,
    result: {
        status: "tagged",
        primary: {category_id: "help_desk", score: 0.95},
        alternatives: []
    }
}')"
TAG_INSERT_SQL="INSERT INTO ${DATABASE}.task_tags_raw (line) FORMAT JSONAsString"
TAG_INSERT_QUERY="$(jq -rn --arg q "$TAG_INSERT_SQL" '$q | @uri')"
curl_ch --request POST "${CLICKHOUSE_URL}/?query=${TAG_INSERT_QUERY}" \
    --data-binary "$TAG_RECORD" >/dev/null || fail "Could not insert correlated task-tag event"

for table in route_decisions route_execution_results task_tags; do
    ROW_COUNT="$(query "SELECT count() FROM ${DATABASE}.${table} FINAL WHERE request_id=toUUID('${CORRELATION_UUID}') FORMAT TabSeparated")" \
        || fail "Could not count typed ${table} rows for correlation UUID"
    [[ "${ROW_COUNT//[[:space:]]/}" == "1" ]] \
        || fail "Expected exactly one typed ${table} row for ${CORRELATION_UUID}; found ${ROW_COUNT}"
done

RAW_ROUTE_COUNT="$(query "SELECT count() FROM ${DATABASE}.route_records_raw WHERE JSONExtractString(line, 'request_id')='${CORRELATION_UUID}' FORMAT TabSeparated")"
RAW_TAG_COUNT="$(query "SELECT count() FROM ${DATABASE}.task_tags_raw WHERE JSONExtractString(line, 'request_id')='${CORRELATION_UUID}' FORMAT TabSeparated")"
[[ "$RAW_ROUTE_COUNT" == "1" && "$RAW_TAG_COUNT" == "1" ]] \
    || fail "Expected one raw route and one raw task-tag event for ${CORRELATION_UUID}"
pass "Raw route/tag events and typed route_decisions/route_execution_results/task_tags share one UUID"


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
pass "Historical tier5/jev maps to decision/ollaya; model ID remains NULL; modernbert maps to encoder"

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
