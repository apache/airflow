#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# [DO NOT MERGE] AIP-125 proof of concept: checks AIP section 9 for Phases 1a, 1b and 2 end to end, and
# sets up a story for presenting the AIP.
#
#   breeze run --backend postgres --answer no "bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b 2"
#   breeze shell --backend postgres, then: bash dev/aip125_demo/demo.sh show
#
# Arguments, run in the order given (default: 1a 1b 2):
#   1a     the inbound endpoint, delivery records, the deliveries feed, the webhooks overview and the CLI
#   1b     topic events: WebhookTopic, GitHub, paused Dags, and the proof of concept's quick start
#   2      an asset route, and webhook waits with WaitForWebhookOperator and @task.webhook_wait
#   bench  bench_payload.py and bench_latency.py, against the processes this script starts
#   keep   leave the processes running until Ctrl-C, e.g. to look at the UI (see README.md)
# Or, alone:
#   show          one API server with the UI, a story with every outcome and parked merge gates, and the
#                 URLs and commands to present them; runs until Ctrl-C (see PRESENTER.md)
#   show --check  the same set-up, then checks the compiled UI and the API behind its pages, and that the
#                 printed commands resume the parked runs, and exits
#
# It resets the metadata database, so run it in a breeze container of its own.
set -euo pipefail

KIT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CORE=http://localhost:8080
# Where breeze publishes the container's port 8080 on the host: WEB_HOST_PORT in
# dev/breeze/src/airflow_breeze/global_constants.py, forwarded by breeze shell and breeze run --forward-ports.
HOST_UI=${DEMO_HOST_UI:-http://localhost:28080}
REPOSITORY=${GITHUB_DEMO_REPO:-my-org/configs}

MODE=checks
SHOW_CHECK=false
steps=("$@")
if [ "${1:-}" = show ]; then
    MODE=show
    case "${2:-}" in
        "") ;;
        --check) SHOW_CHECK=true ;;
        *) echo "Unknown argument '$2' after show: use show or show --check" >&2; exit 2 ;;
    esac
    [ $# -le 2 ] || { echo "show takes no other arguments" >&2; exit 2; }
    steps=()
elif [ ${#steps[@]} -eq 0 ]; then
    steps=(1a 1b 2)
fi
for step in ${steps[@]+"${steps[@]}"}; do
    case $step in
        1a | 1b | 2 | bench | keep) ;;
        *) echo "Unknown argument '$step': use 1a, 1b, 2, bench, keep, or show [--check] alone" >&2; exit 2 ;;
    esac
done

# /files is mounted from files/ in the Airflow checkout, so the logs outlive the container of breeze run.
if [ "$MODE" = show ]; then
    OUT=${DEMO_OUT:-/files/aip125_demo/show-${BACKEND:-sqlite}}
    # One process serves the UI, the REST API, the Execution API and the webhooks.
    WEBHOOKS_ROOT=$CORE
else
    OUT=${DEMO_OUT:-/files/aip125_demo/${BACKEND:-sqlite}}
    WEBHOOKS_ROOT=http://localhost:8081
fi
WH=$WEBHOOKS_ROOT/webhooks
DAGS=$OUT/dags
BODY=$OUT/body.json
GITHUB_SEND=(github --url "$WH/github" --signature-header X-Hub-Signature-256 --signature-prefix sha256=)

export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$DAGS
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=$CORE/execution/
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$OUT/logs
export AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY=$OUT/logs/dag_processor
# Every API server reads it: the webhooks process serves only these names, the core process lists their
# deliveries, and the Execution API registers waits only on them. jira has no connection, so that the
# overview shows a listed webhook that cannot be served.
export AIRFLOW__WEBHOOKS__NAMES=demo,github,etl,jira
# Unset, whatever the environment says, so that the register-wait route refuses a wait without a timeout
# (check 8).
export AIRFLOW__WEBHOOKS__DEFAULT_WAIT_TIMEOUT=
if [ "$MODE" = show ]; then
    # Early deliveries stay claimable for the whole presentation, and the third early GitHub delivery of
    # the story expires the first, so that the feed shows the outcome expired.
    export AIRFLOW__WEBHOOKS__EARLY_DELIVERY_TTL=86400
    export AIRFLOW__WEBHOOKS__EARLY_DELIVERY_MAX_PER_WEBHOOK=2
fi
DEMO_EXPORTS=(
    AIRFLOW__CORE__EXECUTOR AIRFLOW__CORE__LOAD_EXAMPLES AIRFLOW__CORE__DAGS_FOLDER
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL AIRFLOW__LOGGING__BASE_LOG_FOLDER
    AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY AIRFLOW__WEBHOOKS__NAMES
    AIRFLOW__WEBHOOKS__DEFAULT_WAIT_TIMEOUT AIRFLOW__WEBHOOKS__EARLY_DELIVERY_TTL
    AIRFLOW__WEBHOOKS__EARLY_DELIVERY_MAX_PER_WEBHOOK
)

log() { printf '\n== %s\n' "$*"; }
ok() { printf '  ok: %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
expect() { # expect ACTUAL EXPECTED WHAT
    [ "$1" = "$2" ] || fail "$3: expected '$2', got '$1'"
    ok "$3: $2"
}

# pyjson EXPR: print the Python expression EXPR evaluated on the JSON read from stdin, bound to d. The
# CI image has no jq. A CLI command may print log lines around its JSON line.
pyjson() {
    python -c '
import json
import sys
from datetime import datetime

text = sys.stdin.read()
try:
    d = json.loads(text)
except ValueError:
    for line in reversed(text.splitlines()):
        if line.startswith(("{", "[")):
            try:
                d = json.loads(line)
                break
            except ValueError:
                pass
    else:
        sys.exit(f"No JSON in {text[:500]!r}")
print(eval(sys.argv[1]))
' "$1"
}

# http CURL-ARGS...: print the status code of one request, and keep its body in $BODY.
http() {
    curl -s -o "$BODY" -w '%{http_code}' --max-time 60 "$@" || true
}

get_token() { # get_token USER: a JWT from the simple auth manager; breeze sets each password to the user name
    curl -sf -X POST "$CORE/auth/token" -H 'Content-Type: application/json' \
        -d "{\"username\": \"$1\", \"password\": \"$1\"}" | pyjson 'd["access_token"]'
}

api() { # api PATH: GET /api/v2/PATH as admin and print the answer; fail, saying why, unless it is a 200
    local status
    status=$(curl -s -o "$OUT/api.json" -w '%{http_code}' --max-time 60 -H "Authorization: Bearer $ADMIN" \
        "$CORE/api/v2/$1") || true
    if [ "$status" != 200 ]; then
        printf 'GET /api/v2/%s answered %s: %s\n' "$1" "$status" "$(head -c 1000 "$OUT/api.json")" >&2
        return 1
    fi
    cat "$OUT/api.json"
}

# send STATUS OUTCOME NAME SEND-TEST-ARGS...: send one delivery with airflow webhooks send-test, which
# prints {"status": ..., "body": ...}, and check the answer. OUTCOME - checks no outcome. The answer is
# kept in $SENT.
SENT=
send() {
    local want_status=$1 want_outcome=$2 name=$3
    shift 2
    SENT=$(airflow webhooks send-test "$@") || true
    printf '  airflow webhooks send-test %s ...: %s\n' "$name" "$SENT"
    expect "$(pyjson 'd["status"]' <<<"$SENT")" "$want_status" "status answered to the $name delivery"
    if [ "$want_outcome" != - ]; then
        expect "$(pyjson 'd["body"]["outcome"]' <<<"$SENT")" "$want_outcome" "outcome answered to the $name delivery"
    fi
}

send_github() { # send_github STATUS OUTCOME EVENT DELIVERY_ID BODY_FILE: send a signed GitHub delivery
    send "$1" "$2" "${GITHUB_SEND[@]}" --header "X-GitHub-Event: $3" --header "X-GitHub-Delivery: $4" --data "@$5"
}

# rewrite_pr FILE NUMBER [ACTION [REPOSITORY]]: print the GitHub pull request delivery in FILE about pull
# request NUMBER instead, with ACTION (an action other than labeled drops the label) in REPOSITORY.
rewrite_pr() {
    python -c '
import json
import sys

delivery = json.load(open(sys.argv[1]))
number = int(sys.argv[2])
delivery["number"] = delivery["pull_request"]["number"] = number
if len(sys.argv) > 3:
    delivery["action"] = sys.argv[3]
    if sys.argv[3] != "labeled":
        delivery.pop("label", None)
        delivery["pull_request"]["labels"] = []
repository = sys.argv[4] if len(sys.argv) > 4 else delivery["repository"]["full_name"]
owner, name = repository.split("/")
delivery["repository"].update(full_name=repository, name=name)
delivery["repository"]["owner"]["login"] = delivery["organization"]["login"] = owner
for side in ("head", "base"):
    delivery["pull_request"][side]["repo"].update(full_name=repository, owner={"login": owner})
delivery["pull_request"]["html_url"] = f"https://github.com/{repository}/pull/{number}"
print(json.dumps(delivery, indent=2))
' "$@"
}

pr_payload() { # pr_payload merged|unmerged NUMBER: write the closed pull request NUMBER of $REPOSITORY; print its path
    local file=$OUT/payloads/pull_request_closed_$1_$2.json
    mkdir -p "$OUT/payloads"
    rewrite_pr "$KIT/payloads/github_pull_request_closed_$1.json" "$2" closed "$REPOSITORY" >"$file"
    printf '%s\n' "$file"
}

records() { # records WEBHOOK DELIVERY_ID: how many delivery records the REST API lists for the sender's id
    api "webhooks/$1/deliveries?delivery_id=$2" | pyjson 'd["total_entries"]'
}

delivery() { # delivery WEBHOOK DELIVERY_ID EXPR: EXPR on the record of the sender's delivery id, bound to r
    api "webhooks/$1/deliveries?delivery_id=$2" | pyjson "(lambda r: $3)(d['deliveries'][0])"
}

runs() { # runs DAG_ID [STATE]: how many runs the Dag has, in STATE if given
    api "dags/$1/dagRuns${2:+?state=$2}" | pyjson 'd["total_entries"]'
}

has_runs() { # has_runs DAG_ID N [STATE]
    local count
    count=$(runs "$1" "${3:-}") || return 1
    [ "$count" -ge "$2" ]
}

wait_for_runs() { # wait_for_runs DAG_ID N: until N runs of the Dag have succeeded, failing on a failed run
    local deadline=$((SECONDS + 300))
    until has_runs "$1" "$2" success; do
        if has_runs "$1" 1 failed; then
            fail "a run of $1 failed; see $OUT/scheduler.log and the task logs under $OUT/logs"
        fi
        [ "$SECONDS" -lt "$deadline" ] || fail "timed out waiting for $2 successful runs of $1"
        sleep 3
    done
    expect "$(runs "$1")" "$2" "runs of $1, all successful"
}

trigger() { # trigger DAG_ID RUN_ID PR_NUMBER: start a run of a merge gate through the REST API
    local status
    status=$(http -X POST "$CORE/api/v2/dags/$1/dagRuns" -H "Authorization: Bearer $ADMIN" \
        -H 'Content-Type: application/json' \
        -d "{\"dag_run_id\": \"$2\", \"logical_date\": null, \"conf\": {\"pr_number\": $3}}")
    [ "$status" = 200 ] || fail "POST /api/v2/dags/$1/dagRuns answered $status: $(head -c 1000 "$BODY")"
    ok "triggered $1 with pr_number $3 as run $2"
}

ti() { # ti DAG_ID RUN_ID TASK_ID EXPR: EXPR on the task instance, bound to d
    api "dags/$1/dagRuns/$2/taskInstances/$3" | pyjson "$4"
}

# wait_for_ti DAG_ID RUN_ID TASK_ID STATE [SECONDS]: until the task instance is in STATE, failing if it
# ends in another state.
wait_for_ti() {
    local state deadline=$((SECONDS + ${5:-300}))
    while :; do
        state=$(ti "$1" "$2" "$3" 'd["state"]' 2>/dev/null) || state="not created"
        [ "$state" = "$4" ] && break
        case $state in
            success | failed | upstream_failed | skipped | removed)
                fail "$3 of $1 run $2 is $state rather than $4; see the task logs under $OUT/logs" ;;
        esac
        [ "$SECONDS" -lt "$deadline" ] || fail "timed out waiting for $3 of $1 run $2 to be $4; it is $state"
        sleep 2
    done
    ok "$3 of $1 run $2: $4"
}

wait_for_run() { # wait_for_run DAG_ID RUN_ID STATE: until the Dag run ends, failing unless it ends in STATE
    local state deadline=$((SECONDS + 300))
    while :; do
        state=$(api "dags/$1/dagRuns/$2" | pyjson 'd["state"]') || fail "no run $2 of $1"
        case $state in
            success | failed) break ;;
        esac
        [ "$SECONDS" -lt "$deadline" ] || fail "timed out waiting for run $2 of $1 to end; it is $state"
        sleep 2
    done
    expect "$state" "$3" "state of run $2 of $1"
}

webhook_wait() { # webhook_wait DAG_ID RUN_ID TASK_ID EXPR: EXPR on the task instance's newest wait, bound to w
    api "dags/$1/dagRuns/$2/taskInstances/$3/webhookWaits" |
        pyjson "(lambda w: $4)(d['webhook_waits'][0]) if d['webhook_waits'] else 'no wait'"
}

xcom() { # xcom DAG_ID RUN_ID TASK_ID EXPR: EXPR on the task's return value, bound to v
    api "dags/$1/dagRuns/$2/taskInstances/$3/xcomEntries/return_value?stringify=false" |
        pyjson "(lambda v: $4)(d['value'])"
}

# log_says DAG_ID RUN_ID TASK_ID TEXT WHAT: check that the log of the task instance's current try has TEXT.
log_says() {
    local try file=$OUT/task-log.json
    try=$(ti "$1" "$2" "$3" 'd["try_number"]') || fail "no task instance $3 in run $2 of $1"
    curl -sf --max-time 60 -H "Authorization: Bearer $ADMIN" -H 'Accept: application/json' \
        "$CORE/api/v2/dags/$1/dagRuns/$2/taskInstances/$3/logs/$try?full_content=true" >"$file" ||
        fail "GET the log of $3 in run $2 of $1 failed"
    grep -qF -- "$4" "$file" || fail "$5: the log of $3 in run $2 of $1 does not say '$4'; see $file"
    ok "$5: the log of $3 says '$4'"
}

occupied_slots() { api pools/default_pool | pyjson 'd["occupied_slots"]'; }

# check_browse_entry: the UI builds its menus from GET /ui/auth/menus, which must give admin the Browse
# entry of the deliveries view. The UI's own routes cannot be checked by status: the core process answers
# every path it does not route with the UI's index.html.
check_browse_entry() {
    expect "$(http "$CORE/ui/auth/menus" -H "Authorization: Bearer $ADMIN")/$(pyjson '"Webhook Deliveries" in d["authorized_menu_items"]' <"$BODY")" \
        200/True "GET /ui/auth/menus as admin: Webhook Deliveries (Browse -> Webhook Deliveries) is authorized"
}

PIDS=()
start() { # start NAME COMMAND...: run COMMAND in the background in a process group of its own
    local name=$1
    shift
    setsid "$@" >"$OUT/$name.log" 2>&1 &
    PIDS+=("$!")
}

cleanup() {
    local status=$?
    trap - EXIT
    if [ ${#PIDS[@]} -gt 0 ]; then
        for pid in "${PIDS[@]}"; do kill -TERM -- "-$pid" 2>/dev/null || true; done
        for _ in $(seq 30); do
            [ -z "$(jobs -rp)" ] && break
            sleep 1
        done
        for pid in "${PIDS[@]}"; do kill -KILL -- "-$pid" 2>/dev/null || true; done
    fi
    # 130 is Ctrl-C, which ends keep and show.
    if [ "$status" -ne 0 ] && [ "$status" -ne 130 ]; then
        for file in "$OUT"/*.log; do
            [ -f "$file" ] || continue
            printf '\n--- last lines of %s\n' "$file"
            tail -n 40 "$file"
        done
        printf '\nFAILED; the logs are in %s\n' "$OUT" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

REGENERATE="the get_provider_info.py of the standard and github providers must be regenerated \
(prek hook update-providers-build-files)"

check_providers() {
    local sources hook
    sources=$(airflow providers webhook-sources -o json) || fail "airflow providers webhook-sources failed"
    if [ "$(pyjson '" ".join(sorted(s["source_type"] + "=" + s["status"] for s in d))' <<<"$sources")" \
        != "github=ok hmac_json=ok token_header=ok" ]; then
        printf '%s\n' "$sources"
        fail "airflow providers webhook-sources does not list hmac_json, token_header and github as ok;" \
            "$REGENERATE"
    fi
    ok "airflow providers webhook-sources lists hmac_json, token_header and github"
    hook=$(airflow providers hooks -o json |
        pyjson '[h["class"] for h in d if h["connection_type"] == "inbound_webhook"]') ||
        fail "airflow providers hooks failed"
    # Otherwise airflow connections add stores the connections as generic, and the webhooks process
    # answers their deliveries with 503.
    [ "$hook" = "['airflow.providers.standard.hooks.inbound_webhook.InboundWebhookHook']" ] ||
        fail "airflow providers hooks lists $hook for the connection type inbound_webhook; $REGENERATE"
    ok "airflow providers hooks lists InboundWebhookHook for the connection type inbound_webhook"
}

setup() {
    log "Setup: the providers, a fresh metadata database, and the connections webhook_demo, webhook_github and webhook_etl"
    check_providers
    rm -rf "$OUT"
    mkdir -p "$DAGS" "$OUT/payloads"
    # Through the migrations, which add the inbound_delivery and inbound_wait tables and, on PostgreSQL,
    # the GIN index on asset_event.extra that bench_payload.py measures; a schema created from the ORM
    # has no such index.
    airflow db reset -y --use-migration-files >"$OUT/db-reset.log" 2>&1 || fail "airflow db reset failed"
    # Before the webhooks process starts: it caches a missing connection for [webhooks] connection_cache_ttl.
    airflow connections add webhook_demo --conn-type inbound_webhook --conn-password demo-secret \
        --conn-extra '{"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event", "path_field": "service", "delivery_id_header": "X-Demo-Delivery"}'
    airflow connections add webhook_github --conn-type inbound_webhook --conn-password gh-secret \
        --conn-extra '{"source_type": "github"}'
    # The asset route of the standard provider's inbound_webhook.rst: a load.completed delivery about the
    # orders table records an event on s3://warehouse/orders, keyed by the day it loaded.
    airflow connections add webhook_etl --conn-type inbound_webhook --conn-password etl-secret \
        --conn-extra '{"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event", "path_field": "table", "delivery_id_header": "X-Etl-Delivery", "asset_routes": [{"topic": "etl/orders/load.completed", "asset": "s3://warehouse/orders", "partition_key_field": "data.date"}]}'
}

is_healthy() {
    [ "$(http "$CORE/api/v2/monitor/health")" = 200 ] && [ "$(http "$WH/_health")" = 200 ]
}

start_airflow() {
    if [ "$MODE" = show ]; then
        log "Starting airflow api-server --apps all,webhooks, the scheduler and the Dag processor (logs in $OUT)"
        start api-server airflow api-server --apps all,webhooks --port 8080
    else
        log "Starting the core and webhooks API servers, the scheduler and the Dag processor (logs in $OUT)"
        start core airflow api-server --apps core,execution --port 8080
        start webhooks airflow api-server --apps webhooks --port 8081
    fi
    start scheduler airflow scheduler
    start dag-processor airflow dag-processor
    local deadline=$((SECONDS + 180))
    until is_healthy; do
        [ "$(jobs -rp | wc -l)" -eq ${#PIDS[@]} ] || fail "an Airflow process exited"
        [ "$SECONDS" -lt "$deadline" ] || fail "the API servers did not answer their health checks"
        sleep 2
    done
    if [ "$MODE" = show ]; then
        ok "airflow api-server --apps all,webhooks on :8080"
    else
        ok "airflow api-server --apps core,execution on :8080, airflow api-server --apps webhooks on :8081"
    fi
    ADMIN=$(get_token admin) || fail "no token for admin from $CORE/auth/token; run this inside breeze"
}

unpause() { # unpause DAG_ID: print is_paused after PATCH /api/v2/dags/DAG_ID
    curl -sf -X PATCH "$CORE/api/v2/dags/$1" -H "Authorization: Bearer $ADMIN" \
        -H 'Content-Type: application/json' -d '{"is_paused": false}' | pyjson 'd["is_paused"]'
}

load_dags() {
    [ ! -f "$DAGS/deploy_preview.py" ] || return 0
    log "Loading the demo Dags"
    cp "$KIT"/dags/*.py "$DAGS"/
    # Parsed now rather than at the Dag processor's next scan of the folder.
    airflow dags reserialize >"$OUT/reserialize.log" 2>&1 || fail "airflow dags reserialize failed"
    # New Dags start paused, and a delivery that only paused Dags subscribe to records nothing. release_notes
    # and teardown_preview stay paused, for the outcomes dag_paused and early; 1b unpauses each after that.
    for dag_id in smoke_test_billing deploy_preview orders_by_day merge_gate_operator merge_gate_decorator \
        merge_gate_timeout merge_gate_no_timeout pr_open_and_wait; do
        expect "$(unpause "$dag_id")" False "is_paused of $dag_id"
    done
}

phase_1a() {
    log "Phase 1a: plugins, the proof-of-concept package and airflow webhooks check"
    expect "$(airflow plugins -o json | pyjson '[p["name"] for p in d if "webhook" in p["name"].lower()]')" \
        "[]" "plugins with webhook in their name"
    expect "$(python -c '
import importlib.metadata

try:
    print(importlib.metadata.version("airflow-provider-inbound-webhooks"))
except importlib.metadata.PackageNotFoundError:
    print("not installed")
')" "not installed" "the proof-of-concept package airflow-provider-inbound-webhooks"
    airflow webhooks check demo github etl || fail "airflow webhooks check found problems"
    ok "airflow webhooks check demo github etl"
    local problems
    if problems=$(airflow webhooks check jira 2>&1); then
        fail "airflow webhooks check jira passed, but no connection webhook_jira exists"
    fi
    printf '  airflow webhooks check jira: %s\n' "$problems"
    [[ $problems == *"Problem with webhook jira:"* ]] || fail "airflow webhooks check jira names no problem"
    ok "airflow webhooks check jira exits 1 and names the problem"

    log "Phase 1a: what each process serves"
    expect "$(http "$WH/_health")" 200 "webhooks process: GET /webhooks/_health"
    for path in /docs /redoc /openapi.json /api/v2/version; do
        expect "$(http "$WEBHOOKS_ROOT$path")" 404 "webhooks process: GET $path"
    done
    expect "$(http "$WH/demo")" 405 "webhooks process: GET /webhooks/demo, which only POST serves"
    for request in "GET /webhooks/_health" "POST /webhooks/demo"; do
        expect "$(http -X "${request% *}" "$CORE${request#* }")" 404 "core process: $request"
        expect "$(pyjson 'd["error"]' <"$BODY")" \
            "Inbound webhooks are served only by airflow api-server --apps webhooks" "core process: JSON body"
    done

    log "Phase 1a: refused requests"
    local json=(-H 'Content-Type: application/json')
    expect "$(http -X POST "$WH/nope" "${json[@]}" --data-binary '{}')" 404 "a name [webhooks] names does not list"
    expect "$(http -X POST "$WH/jira" "${json[@]}" --data-binary '{}')" 404 \
        "a listed name without a connection"
    expect "$(http -X POST "$WH/demo" "${json[@]}" -H 'X-Signature: 00' --data-binary '{}')" 401 "a bad signature"
    python -c 'import json; print(json.dumps({"event": "built", "service": "billing", "pad": "a" * 2_097_152}))' \
        >"$OUT/oversized.json"
    # With Expect: 100-continue, curl waits for the server to ask for the body. The webhooks app refuses a
    # Content-Length over [webhooks] max_body_bytes (1 MiB by default) without reading the body.
    expect "$(http -X POST "$WH/demo" "${json[@]}" -H 'X-Signature: 00' -H 'Expect: 100-continue' \
        --data-binary @"$OUT/oversized.json")" 413 "a body over max_body_bytes"

    log "Phase 1a: a signed hmac_json delivery that no Dag subscribes to, and its redelivery"
    local built='{"event": "built", "service": "billing"}'
    send 202 no_subscribers demo --url "$WH/demo" --delivery-id d-1 --data "$built"
    expect "$(pyjson 'd["body"]["delivery_id"]' <<<"$SENT")" d-1 "delivery_id answered"
    # It recorded nothing, so the redelivery is processed again rather than answered as a duplicate.
    send 202 no_subscribers demo --url "$WH/demo" --delivery-id d-1 --data "$built"
    expect "$(records demo d-1)" 1 "records of d-1 in GET /api/v2/webhooks/demo/deliveries"
    expect "$(delivery demo d-1 '(r["outcome"], r["source_type"], r["candidates"], r["correlation_key"], r["actions"])')" \
        "('no_subscribers', 'hmac_json', ['demo/billing/built', 'demo/*/built'], None, None)" "the record of d-1"

    log "Phase 1a: a GitHub delivery that nothing acts on, kept early for its correlation key"
    rewrite_pr "$KIT/payloads/github_pull_request_labeled.json" 41 opened >"$OUT/payloads/github_pull_request_opened_41.json"
    send_github 202 early pull_request g-0 "$OUT/payloads/github_pull_request_opened_41.json"
    # An early delivery recorded no event, so its redelivery is processed again too.
    send_github 202 early pull_request g-0 "$OUT/payloads/github_pull_request_opened_41.json"
    expect "$(records github g-0)" 1 "records of g-0"
    expect "$(delivery github g-0 '(r["outcome"], r["correlation_key"], r["attributes"]["number"], r["actions"])')" \
        "('early', 'my-org/web#41', 41, None)" "the record of g-0"

    log "Phase 1a: the deliveries feed across webhooks, the webhooks overview, and the Browse entry"
    expect "$(api 'webhooks/~/deliveries' | pyjson '[(r["webhook"], r["delivery_id"], r["outcome"]) for r in d["deliveries"]]')" \
        "[('github', 'g-0', 'early'), ('demo', 'd-1', 'no_subscribers')]" \
        "GET /api/v2/webhooks/~/deliveries, newest first"
    expect "$(api 'webhooks/~/deliveries?webhook_names=demo' | pyjson '[r["delivery_id"] for r in d["deliveries"]]')" \
        "['d-1']" "GET /api/v2/webhooks/~/deliveries?webhook_names=demo"
    expect "$(api 'webhooks/~/deliveries?webhook_names=demo&webhook_names=github' | pyjson 'd["total_entries"]')" \
        2 "GET /api/v2/webhooks/~/deliveries?webhook_names=demo&webhook_names=github"
    expect "$(api 'webhooks/~/deliveries?source_type=github' | pyjson '[r["delivery_id"] for r in d["deliveries"]]')" \
        "['g-0']" "GET /api/v2/webhooks/~/deliveries?source_type=github"
    expect "$(api webhooks | pyjson '[(w["name"], w["listed"], w["connection_status"], w["source_type"], w["source_type_status"], w["last_outcome"]) for w in d["webhooks"]]')" \
        "[('demo', True, 'ok', 'hmac_json', 'ok', 'no_subscribers'), ('etl', True, 'ok', 'hmac_json', 'ok', None), ('github', True, 'ok', 'github', 'ok', 'early'), ('jira', True, 'not_found', None, None, None)]" \
        "GET /api/v2/webhooks: name, listed, connection_status, source_type, source_type_status, last_outcome"
    check_browse_entry

    log "Phase 1a: who may read deliveries"
    local viewer
    viewer=$(get_token viewer) || fail "no token for viewer from $CORE/auth/token"
    expect "$(http "$CORE/api/v2/webhooks/demo/deliveries" -H "Authorization: Bearer $viewer")" 403 \
        "deliveries of demo for viewer, who may not read connections"
    expect "$(http "$CORE/api/v2/webhooks/~/deliveries" -H "Authorization: Bearer $viewer")/$(pyjson 'd["total_entries"]' <"$BODY")" \
        200/0 "GET /api/v2/webhooks/~/deliveries for viewer: an empty page"
    expect "$(http "$CORE/api/v2/webhooks" -H "Authorization: Bearer $viewer")/$(pyjson 'd["total_entries"]' <"$BODY")" \
        200/0 "GET /api/v2/webhooks for viewer: an empty list"
    expect "$(http "$CORE/api/v2/webhooks/demo/deliveries")" 401 "deliveries without a token"
}

phase_1b() {
    load_dags

    log "Phase 1b: Dags scheduled on webhook topics"
    local errors
    errors=$(airflow dags list-import-errors -o json) || true
    expect "$(pyjson '[(e["filepath"].rsplit("/", 1)[-1], e["error"]) for e in d]' <<<"$errors")" \
        "[('partitioned_topic_rejected.py', 'AirflowTimetableInvalid: PartitionedAssetTimetable cannot schedule on a WebhookTopic; its events have no partition key')]" \
        "Dag import errors"
    expect "$(api 'assets?name_pattern=deploy-preview' | pyjson '[(a["name"], a["group"]) for a in d["assets"]]')" \
        "[('github/my-org/web/pull_request/labeled/deploy-preview', 'webhook')]" "the WebhookTopic of deploy_preview"

    log "Phase 1b: GitHub"
    local labeled=$KIT/payloads/github_pull_request_labeled.json
    send 200 - "${GITHUB_SEND[@]}" --header 'X-GitHub-Event: ping' --header 'X-GitHub-Delivery: ping-1' \
        --data "@$KIT/payloads/github_ping.json"
    expect "$(pyjson 'd["body"]' <<<"$SENT")" "{}" "body answered to the ping"
    send_github 202 accepted pull_request g-1 "$labeled"
    wait_for_runs deploy_preview 1
    expect "$(api 'assets/events?extra=delivery_id=g-1' |
        pyjson '[(e["name"], [r["dag_id"] for r in e["created_dagruns"]]) for e in d["asset_events"]]')" \
        "[('github/my-org/web/pull_request/labeled/deploy-preview', ['deploy_preview'])]" \
        "the topic event of g-1 and the run it started"
    expect "$(delivery github g-1 '([e["asset"] for e in r["actions"]["asset_events"]], r["correlation_key"])')" \
        "(['github/my-org/web/pull_request/labeled/deploy-preview'], 'my-org/web#42')" \
        "the asset events in the record of g-1"
    # GitHub's redelivery repeats the delivery id and the body.
    send_github 200 duplicate pull_request g-1 "$labeled"
    expect "$(records github g-1)" 1 "records of g-1"
    expect "$(records github ping-1)" 0 "records of the ping"
    # Long enough for the scheduler to start a run for any event the redelivery could have recorded.
    sleep 15
    expect "$(runs deploy_preview)" 1 "runs of deploy_preview after the redelivery"
    python -c '
import json
import sys

delivery = json.load(open(sys.argv[1]))
delivery["number"] = delivery["pull_request"]["number"] = 43
delivery["pull_request"]["html_url"] = "https://github.com/my-org/web/pull/43"
delivery["pull_request"]["head"]["ref"] = "feature/search"
print(json.dumps(delivery, indent=2))
' "$labeled" >"$OUT/payloads/github_pull_request_labeled_43.json"
    send_github 202 accepted pull_request g-2 "$OUT/payloads/github_pull_request_labeled_43.json"
    wait_for_runs deploy_preview 2

    log "Phase 1b: a delivery that only a paused Dag subscribes to, and its redelivery once the Dag is unpaused"
    local released='{"event": "released", "service": "billing", "version": "2.4.1"}'
    send 202 dag_paused demo --url "$WH/demo" --delivery-id rel-1 --data "$released"
    expect "$(unpause release_notes)" False "is_paused of release_notes"
    # A dag_paused delivery recorded nothing, so the sender's redelivery is processed again in full.
    send 202 accepted demo --url "$WH/demo" --delivery-id rel-1 --data "$released"
    expect "$(records demo rel-1)" 1 "records of rel-1"
    wait_for_runs release_notes 1

    log "Phase 1b: a GitHub delivery that only a paused Dag subscribes to, kept early, and its redelivery once the Dag is unpaused"
    local closed=$OUT/payloads/github_pull_request_closed_merged_42.json
    rewrite_pr "$KIT/payloads/github_pull_request_closed_merged.json" 42 closed my-org/web >"$closed"
    # It has a correlation key, so it is kept early for a webhook wait rather than recorded as dag_paused.
    send_github 202 early pull_request g-3 "$closed"
    expect "$(delivery github g-3 '(r["outcome"], r["correlation_key"], r["actions"])')" \
        "('early', 'my-org/web#42', None)" "the record of g-3"
    expect "$(unpause teardown_preview)" False "is_paused of teardown_preview"
    # An early delivery recorded nothing, so the redelivery is processed again in full, topic events
    # included (decision P2-15).
    send_github 202 accepted pull_request g-3 "$closed"
    expect "$(records github g-3)" 1 "records of g-3"
    expect "$(delivery github g-3 '(r["outcome"], [e["asset"] for e in r["actions"]["asset_events"]])')" \
        "('accepted', ['github/my-org/web/pull_request/closed'])" "the asset events in the record of g-3"
    wait_for_runs teardown_preview 1

    log "Phase 1b: the proof of concept's quick start, on core"
    send 202 accepted demo --url "$WH/demo" --delivery-id qs-1 --data "@$KIT/payloads/hmac_json_deployed.json"
    wait_for_runs smoke_test_billing 1
    local event record_id
    event=$(api 'assets/events?extra=delivery_id=qs-1') || fail "GET /api/v2/assets/events failed"
    record_id=$(delivery demo qs-1 'r["id"]') || fail "no record of qs-1 in GET /api/v2/webhooks/demo/deliveries"
    expect "$(pyjson '[e["name"] for e in d["asset_events"]]' <<<"$event")" "['demo/billing/deployed']" \
        "topic events of qs-1"
    expect "$(pyjson 'sorted(d["asset_events"][0]["extra"])' <<<"$event")" \
        "['correlation_key', 'delivery_id', 'from_webhook', 'inbound_delivery_id', 'payload', 'source_type', 'webhook']" \
        "keys of the topic event's extra"
    expect "$(pyjson '(lambda x: (x["from_webhook"], x["webhook"], x["source_type"], x["payload"]["version"]))(d["asset_events"][0]["extra"])' <<<"$event")" \
        "(True, 'demo', 'hmac_json', '2.4.1')" "the topic event's extra"
    expect "$(pyjson 'd["asset_events"][0]["extra"]["inbound_delivery_id"]' <<<"$event")" "$record_id" \
        "inbound_delivery_id, the id of the delivery record"
}

# check_resumed CHECK DAG_ID RUN_ID NUMBER: the checks after a merged delivery resumed a merge gate's wait.
check_resumed() {
    local check=$1 dag_id=$2 run_id=$3 number=$4 record_id
    wait_for_run "$dag_id" "$run_id" success
    expect "$(ti "$dag_id" "$run_id" wait_for_merge 'd["state"]')/$(ti "$dag_id" "$run_id" apply 'd["state"]')" \
        success/success "check $check: states of wait_for_merge and apply"
    record_id=$(delivery github "gh-$number" 'r["id"]') || fail "no record of gh-$number"
    expect "$(delivery github "gh-$number" '(r["outcome"], [(w["dag_id"], w["run_id"], w["task_id"]) for w in r["actions"]["waits"]])')" \
        "('resumed_wait', [('$dag_id', '$run_id', 'wait_for_merge')])" \
        "check $check: the outcome and the waits in the record of gh-$number, in the deliveries feed"
    expect "$(webhook_wait "$dag_id" "$run_id" wait_for_merge '(w["status"], w["inbound_delivery_id"], w["ended_at"] is not None)')" \
        "('resumed', '$record_id', True)" "check $check: the wait of wait_for_merge after the delivery"
    expect "$(xcom "$dag_id" "$run_id" wait_for_merge '(v["webhook"], v["event"], v["delivery_id"], v["attributes"]["number"], v["payload"]["pull_request"]["merged"])')" \
        "('github', 'pull_request', 'gh-$number', $number, True)" "check $check: the XCom of wait_for_merge, the delivery"
    expect "$(xcom "$dag_id" "$run_id" apply 'v')" 6dcb09b5b57875f334f61aebed695e2e4193db5e \
        "check $check: the XCom of apply, the merge commit"
}

# check_parked_then_merged CHECK DAG_ID RUN_ID NUMBER: checks 1 and 2.
check_parked_then_merged() {
    local check=$1 dag_id=$2 run_id=$3 number=$4
    log "Phase 2, check $check: $dag_id waits for pull request $number, which is then merged"
    expect "$(webhook_wait "$dag_id" "$run_id" wait_for_merge '(w["status"], w["webhook"], w["events"], sorted(w["match"].items()), w["inbound_delivery_id"])')" \
        "('registered', 'github', ['pull_request'], [('action', 'closed'), ('number', $number), ('repository', '$REPOSITORY')], None)" \
        "check $check: the open wait of the parked wait_for_merge"
    expect "$(occupied_slots)" 0 "check $check: occupied slots of default_pool while wait_for_merge is parked"
    send_github 202 resumed_wait pull_request "gh-$number" "$(pr_payload merged "$number")"
    check_resumed "$check" "$dag_id" "$run_id" "$number"
}

phase_2() {
    load_dags

    log "Phase 2: an asset route records an event with a partition key on a data asset"
    send 202 accepted etl --url "$WH/etl" --delivery-id etl-1 --data "@$KIT/payloads/hmac_json_orders_loaded.json"
    expect "$(delivery etl etl-1 '[(e["topic"], e["asset"], e["partition_key"]) for e in r["actions"]["asset_routes"]]')" \
        "[('etl/orders/load.completed', 's3://warehouse/orders', '2026-10-19')]" "the asset route event in the record of etl-1"
    local asset_id record_id
    asset_id=$(delivery etl etl-1 'r["actions"]["asset_routes"][0]["asset_id"]') || fail "no asset route event in etl-1"
    record_id=$(delivery etl etl-1 'r["id"]')
    expect "$(api "assets/events?asset_id=$asset_id" | pyjson '[(e["uri"], e["partition_key"], e["extra"]["webhook"], e["extra"]["inbound_delivery_id"]) for e in d["asset_events"]]')" \
        "[('s3://warehouse/orders', '2026-10-19', 'etl', '$record_id')]" "the event on s3://warehouse/orders"
    wait_for_runs orders_by_day 1
    expect "$(api dags/orders_by_day/dagRuns | pyjson '[r["partition_key"] for r in d["dag_runs"]]')" "['2026-10-19']" \
        "the partition key of the run of orders_by_day, which has a PartitionedAssetTimetable"
    send 202 no_subscribers etl --url "$WH/etl" --delivery-id etl-2 \
        --data '{"event": "load.completed", "table": "orders", "data": {"rows": 0}}'
    expect "$(delivery etl etl-2 '[(s["topic"], s["asset"], s["partition_key_field"], s["reason"]) for s in r["actions"]["skipped_asset_routes"]]')" \
        "[('etl/orders/load.completed', 's3://warehouse/orders', 'data.date', 'partition_key_missing')]" \
        "the skipped asset route in the record of etl-2, a delivery without data.date"

    log "Phase 2: webhook waits; checks 1, 2, 3 and 7 start their merge gates together"
    trigger merge_gate_operator check-1 4242
    trigger merge_gate_decorator check-2 4243
    trigger merge_gate_operator check-3 4244
    trigger merge_gate_operator check-7a 4247
    wait_for_ti merge_gate_operator check-1 wait_for_merge awaiting_input
    wait_for_ti merge_gate_decorator check-2 wait_for_merge awaiting_input
    wait_for_ti merge_gate_operator check-3 wait_for_merge awaiting_input
    wait_for_ti merge_gate_operator check-7a wait_for_merge awaiting_input
    expect "$(occupied_slots)" 0 "occupied slots of default_pool while four tasks are parked"

    check_parked_then_merged 1 merge_gate_operator check-1 4242
    check_parked_then_merged 2 merge_gate_decorator check-2 4243

    log "Phase 2, check 5 starts: merge_gate_timeout waits 30 seconds for pull request 4246"
    # Not before checks 1 and 2: its task runs again when the wait times out, and would hold a pool slot.
    trigger merge_gate_timeout check-5 4246

    log "Phase 2, check 3: merge_gate_operator waits for pull request 4244, which is closed without merging"
    send_github 202 resumed_wait pull_request gh-4244 "$(pr_payload unmerged 4244)"
    wait_for_run merge_gate_operator check-3 failed
    expect "$(ti merge_gate_operator check-3 wait_for_merge 'd["state"]')/$(ti merge_gate_operator check-3 apply 'd["state"]')" \
        success/failed "check 3: states of wait_for_merge and apply"
    log_says merge_gate_operator check-3 apply "Pull request 4244 was closed without merging" "check 3"

    log "Phase 2, check 4: pull request 4245 is merged before merge_gate_operator waits for it"
    send_github 202 early pull_request gh-4245 "$(pr_payload merged 4245)"
    expect "$(delivery github gh-4245 '(r["outcome"], r["correlation_key"])')" "('early', '$REPOSITORY#4245')" \
        "check 4: the record of gh-4245, kept early"
    trigger merge_gate_operator check-4 4245
    check_resumed 4 merge_gate_operator check-4 4245

    log "Phase 2, check 6: GitHub redelivers check 1's delivery"
    local try_number
    try_number=$(ti merge_gate_operator check-1 wait_for_merge 'd["try_number"]')
    send_github 200 duplicate pull_request gh-4242 "$OUT/payloads/pull_request_closed_merged_4242.json"
    expect "$(records github gh-4242)" 1 "check 6: records of gh-4242"
    # Long enough for the scheduler to run the task again, had the redelivery resumed it.
    sleep 10
    expect "$(ti merge_gate_operator check-1 wait_for_merge '(d["state"], d["try_number"])')" "('success', $try_number)" \
        "check 6: state and try number of wait_for_merge in check-1"
    expect "$(api 'dags/merge_gate_operator/dagRuns/check-1/taskInstances/wait_for_merge/webhookWaits' | pyjson '[w["status"] for w in d["webhook_waits"]]')" \
        "['resumed']" "check 6: the waits of wait_for_merge in check-1"

    log "Phase 2, check 7: a second run of merge_gate_operator waits for pull request 4247 too"
    trigger merge_gate_operator check-7b 4247
    wait_for_ti merge_gate_operator check-7b wait_for_merge failed
    log_says merge_gate_operator check-7b wait_for_merge wait_key_taken "check 7: the refused registration"
    log_says merge_gate_operator check-7b wait_for_merge "'status_code': 409" "check 7: the status answered to it"
    expect "$(webhook_wait merge_gate_operator check-7b wait_for_merge 'w["status"]')" "no wait" \
        "check 7: the waits of the refused task"
    expect "$(ti merge_gate_operator check-7a wait_for_merge 'd["state"]')" awaiting_input \
        "check 7: the first run's wait_for_merge, still parked"
    send_github 202 resumed_wait pull_request gh-4247 "$(pr_payload merged 4247)"
    wait_for_run merge_gate_operator check-7a success

    log "Phase 2, check 8: merge_gate_no_timeout waits for pull request 4248 without a timeout, and [webhooks] default_wait_timeout is unset"
    trigger merge_gate_no_timeout check-8 4248
    wait_for_ti merge_gate_no_timeout check-8 wait_for_merge failed
    log_says merge_gate_no_timeout check-8 wait_for_merge timeout_required "check 8: the refused registration"
    log_says merge_gate_no_timeout check-8 wait_for_merge "'status_code': 422" "check 8: the status answered to it"
    expect "$(webhook_wait merge_gate_no_timeout check-8 wait_for_merge 'w["status"]')" "no wait" \
        "check 8: the waits of the refused task"

    log "Phase 2, check 5: merge_gate_timeout, which received nothing"
    wait_for_ti merge_gate_timeout check-5 wait_for_merge failed 180
    expect "$(webhook_wait merge_gate_timeout check-5 wait_for_merge '(w["status"], w["inbound_delivery_id"], 0 <= (datetime.fromisoformat(w["ended_at"]) - datetime.fromisoformat(w["deadline"])).total_seconds() <= 60)')" \
        "('timed_out', None, True)" "check 5: the wait, timed out within a minute of its deadline"
    log_says merge_gate_timeout check-5 wait_for_merge WebhookWaitTimeoutError "check 5"
    log_says merge_gate_timeout check-5 wait_for_merge "No matching delivery arrived before" "check 5"
}

phase_bench() {
    load_dags
    log "Spike S10: the cost of payloads in asset_event.extra"
    local payload_args
    # Such as BENCH_PAYLOAD_ARGS='--max-event-payload-bytes 65536' for the default cap; see --help.
    read -ra payload_args <<<"${BENCH_PAYLOAD_ARGS:-}"
    python "$KIT/bench_payload.py" --api-url "$CORE" "${payload_args[@]}"
    log "Delivery latency with a subscribed Dag"
    python "$KIT/bench_latency.py" --url "$WH/demo" --count "${BENCH_COUNT:-100}" \
        --concurrency "${BENCH_CONCURRENCY:-4}"
}

phase_keep() {
    log "The processes keep running until Ctrl-C"
    cat <<EOF
  UI, as admin/admin:  $HOST_UI/webhook_deliveries (with breeze run --forward-ports)
  Inside the container: $CORE (core API, UI) and $WH/<name> (webhooks)
  Logs:                 $OUT (/files is files/ in the Airflow checkout)
EOF
    wait
}

# The story of show mode: what it seeds, the commands it prints, and the URLs to open.

LIVE_COMMANDS=()
seed_story() {
    log "Seeding the story: deliveries with every outcome, a topic-started run, an asset route, parked merge gates"
    send_github 202 accepted pull_request show-labeled-42 "$KIT/payloads/github_pull_request_labeled.json"
    send 202 accepted demo --url "$WH/demo" --delivery-id show-deployed-1 --data "@$KIT/payloads/hmac_json_deployed.json"
    send 202 no_subscribers demo --url "$WH/demo" --delivery-id show-built-1 \
        --data '{"event": "built", "service": "billing"}'
    send 202 dag_paused demo --url "$WH/demo" --delivery-id show-released-1 \
        --data '{"event": "released", "service": "billing", "version": "2.4.1"}'
    send 202 accepted etl --url "$WH/etl" --delivery-id show-orders-2026-10-19 \
        --data "@$KIT/payloads/hmac_json_orders_loaded.json"
    send 202 no_subscribers etl --url "$WH/etl" --delivery-id show-orders-no-date \
        --data '{"event": "load.completed", "table": "orders", "data": {"rows": 0}}'
    # Opened pull requests that no Dag and no wait acts on; with early_delivery_max_per_webhook 2, the
    # third expires the first.
    local number
    for number in 44 45 46; do
        rewrite_pr "$KIT/payloads/github_pull_request_labeled.json" "$number" opened \
            >"$OUT/payloads/github_pull_request_opened_$number.json"
        send_github 202 early pull_request "show-opened-$number" "$OUT/payloads/github_pull_request_opened_$number.json"
    done
    expect "$(delivery github show-opened-44 'r["outcome"]')" expired "the outcome of show-opened-44 now"

    trigger merge_gate_operator show-pr-5000 5000
    trigger merge_gate_operator show-pr-5001 5001
    trigger merge_gate_decorator show-pr-5002 5002
    trigger merge_gate_timeout show-timeout-5004 5004
    wait_for_ti merge_gate_operator show-pr-5000 wait_for_merge awaiting_input
    send_github 202 resumed_wait pull_request show-merged-5000 "$(pr_payload merged 5000)"
    wait_for_ti merge_gate_operator show-pr-5001 wait_for_merge awaiting_input
    wait_for_ti merge_gate_decorator show-pr-5002 wait_for_merge awaiting_input
    wait_for_run merge_gate_operator show-pr-5000 success
    wait_for_runs deploy_preview 1
    wait_for_runs smoke_test_billing 1
    wait_for_runs orders_by_day 1

    # What the presenter runs live, in a second shell in the container (breeze exec).
    local send_test="airflow webhooks send-test github --url $WH/github --signature-header X-Hub-Signature-256 --signature-prefix sha256= --header 'X-GitHub-Event: pull_request'"
    LIVE_COMMANDS=(
        "# merge_gate_operator, run show-pr-5001: pull request 5001 is merged, so wait_for_merge resumes and apply succeeds"
        "$send_test --header 'X-GitHub-Delivery: live-5001' --data @$(pr_payload merged 5001)"
        "# merge_gate_decorator, run show-pr-5002: pull request 5002 is closed without merging, so apply fails"
        "$send_test --header 'X-GitHub-Delivery: live-5002' --data @$(pr_payload unmerged 5002)"
        "# The early case: pull request 5003 is merged before anything waits for it (outcome early) ..."
        "$send_test --header 'X-GitHub-Delivery: live-5003' --data @$(pr_payload merged 5003)"
        "# ... then the merge gate that waits for it starts, and its wait takes the early delivery at once"
        "airflow dags trigger merge_gate_operator --run-id show-pr-5003 --conf '{\"pr_number\": 5003}'"
    )
}

ui_path() { # ui_path PATH...: print the UI URL of a path, each argument a segment, URL-encoded
    python -c '
import sys
from urllib.parse import quote

print(sys.argv[1] + "".join("/" + quote(segment, safe="") for segment in sys.argv[2:]))
' "$HOST_UI" "$@"
}

print_guide() {
    local labeled_id deploy_run orders_id orders_run
    labeled_id=$(delivery github show-labeled-42 'r["id"]')
    deploy_run=$(api dags/deploy_preview/dagRuns | pyjson 'd["dag_runs"][0]["dag_run_id"]')
    orders_id=$(delivery etl show-orders-2026-10-19 'r["actions"]["asset_routes"][0]["asset_id"]')
    orders_run=$(api dags/orders_by_day/dagRuns | pyjson 'd["dag_runs"][0]["dag_run_id"]')
    cat <<EOF

== Open these in a browser on the host (breeze publishes the container's port 8080 there).
   Sign in as admin, password admin. PRESENTER.md has the walkthrough.

  Browse -> Webhook Deliveries:     $HOST_UI/webhook_deliveries
  A delivery's details panel:       $HOST_UI/webhook_deliveries?id=$labeled_id
  The early ones:                   $HOST_UI/webhook_deliveries?outcome=early
  The Webhooks tab:                 $HOST_UI/webhook_overview
  The topic-started Dag run:        $(ui_path dags deploy_preview runs "$deploy_run" asset_events)
  The asset of the asset route:     $(ui_path assets "$orders_id")
  The run of its keyed event:       $(ui_path dags orders_by_day runs "$orders_run" asset_events)
  Parked (operator), Details tab:   $HOST_UI/dags/merge_gate_operator/runs/show-pr-5001/tasks/wait_for_merge/details
  Parked (decorator), Details tab:  $HOST_UI/dags/merge_gate_decorator/runs/show-pr-5002/tasks/wait_for_merge/details
  Resumed by a delivery:            $HOST_UI/dags/merge_gate_operator/runs/show-pr-5000/tasks/wait_for_merge/details
  Timed out after 30 seconds:       $HOST_UI/dags/merge_gate_timeout/runs/show-timeout-5004/tasks/wait_for_merge/details
  Pools (parked tasks hold none):   $HOST_UI/pools

== Run these live from a second terminal in the container: breeze exec

EOF
    local command
    for command in "${LIVE_COMMANDS[@]}"; do
        printf '  %s\n' "$command"
    done
    cat <<EOF

== Logs: $OUT (files/aip125_demo/show-${BACKEND:-sqlite} in the Airflow checkout). Ctrl-C stops everything.
EOF
}

check_show() {
    log "show --check: the compiled UI, its Browse entry, and the routes behind the pages"
    local script path
    expect "$(http "$CORE/")" 200 "GET / (the UI)"
    script=$(grep -o 'src="./static/assets/index-[^"]*\.js"' "$BODY" | sed 's/^src="\.\///; s/"$//') ||
        fail "GET / did not answer the UI's index.html; run breeze ui compile-assets"
    expect "$(http "$CORE/$script")" 200 "GET /$script, the UI's main script"
    # The route definitions, path:`webhook_overview` and so on, whichever quotes the minifier picks.
    for path in webhook_deliveries webhook_deliveries/:webhookName webhook_overview; do
        grep -qE "path: ?[\"'\`]${path}[\"'\`]" "$BODY" ||
            fail "the UI has no route $path: compile it from this branch with breeze ui compile-assets"
    done
    ok "the UI's main script routes webhook_deliveries, webhook_deliveries/:webhookName and webhook_overview"
    check_browse_entry
    # What the details panel of the printed URL loads, over the all-webhooks feed.
    expect "$(api "webhooks/~/deliveries?id=$(delivery github show-labeled-42 'r["id"]')&limit=1" | pyjson '[r["delivery_id"] for r in d["deliveries"]]')" \
        "['show-labeled-42']" "GET /api/v2/webhooks/~/deliveries?id=, for the details panel the guide prints"
    expect "$(http "$WH/_health")" 200 "GET /webhooks/_health"
    expect "$(api webhooks | pyjson '[(w["name"], w["connection_status"]) for w in d["webhooks"]]')" \
        "[('demo', 'ok'), ('etl', 'ok'), ('github', 'ok'), ('jira', 'not_found')]" "GET /api/v2/webhooks"
    expect "$(api 'webhooks/~/deliveries?limit=100' | pyjson 'sorted({r["outcome"] for r in d["deliveries"]})')" \
        "['accepted', 'dag_paused', 'early', 'expired', 'no_subscribers', 'resumed_wait']" \
        "the outcomes in GET /api/v2/webhooks/~/deliveries"
    local run_id
    run_id=$(api dags/deploy_preview/dagRuns | pyjson 'd["dag_runs"][0]["dag_run_id"]')
    expect "$(api "dags/deploy_preview/dagRuns/$run_id/upstreamAssetEvents" | pyjson '[(e["extra"]["from_webhook"], e["extra"]["webhook"], e["extra"]["delivery_id"]) for e in d["asset_events"]]')" \
        "[(True, 'github', 'show-labeled-42')]" "the source event of the topic-started run, from a webhook"
    run_id=$(api dags/orders_by_day/dagRuns | pyjson 'd["dag_runs"][0]["dag_run_id"]')
    expect "$(api "dags/orders_by_day/dagRuns/$run_id/upstreamAssetEvents" | pyjson '[(e["uri"], e["partition_key"], e["extra"]["webhook"]) for e in d["asset_events"]]')" \
        "[('s3://warehouse/orders', '2026-10-19', 'etl')]" "the source event of the keyed run, from the asset route"
    expect "$(webhook_wait merge_gate_operator show-pr-5001 wait_for_merge 'w["status"]')" registered \
        "GET .../show-pr-5001/taskInstances/wait_for_merge/webhookWaits"
    expect "$(webhook_wait merge_gate_decorator show-pr-5002 wait_for_merge 'w["status"]')" registered \
        "GET .../show-pr-5002/taskInstances/wait_for_merge/webhookWaits"

    log "show --check: the printed commands, run as the presenter would"
    local command
    for command in "${LIVE_COMMANDS[@]}"; do
        [[ $command == "#"* ]] && continue
        printf '  $ %s\n' "$command"
        # Without what this script exports, as in a breeze exec shell.
        (
            unset "${DEMO_EXPORTS[@]}"
            eval "$command"
        ) || fail "the printed command failed"
    done
    wait_for_run merge_gate_operator show-pr-5001 success
    wait_for_run merge_gate_decorator show-pr-5002 failed
    expect "$(ti merge_gate_decorator show-pr-5002 apply 'd["state"]')" failed "apply in show-pr-5002"
    wait_for_run merge_gate_operator show-pr-5003 success
    expect "$(delivery github live-5003 '(r["outcome"], [w["run_id"] for w in r["actions"]["waits"]])')" \
        "('resumed_wait', ['show-pr-5003'])" "the record of live-5003, which the wait of show-pr-5003 took early"
}

show() {
    setup
    start_airflow
    load_dags
    seed_story
    print_guide
    if [ "$SHOW_CHECK" = true ]; then
        check_show
        printf '\nshow --check: done\n'
        return
    fi
    wait
}

if [ "$MODE" = show ]; then
    show
    exit 0
fi
setup
start_airflow
for step in "${steps[@]}"; do
    "phase_$step"
    [ "$step" = keep ] || printf '\n%s: done\n' "$step"
done
