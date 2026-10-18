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

# [DO NOT MERGE] AIP-125 proof of concept: checks AIP section 9 for Phases 1a and 1b end to end.
#
#   breeze run --backend postgres bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b
#
# Arguments, run in the order given (default: 1a 1b):
#   1a     the inbound endpoint, delivery records and the CLI
#   1b     topic events: WebhookTopic, GitHub, and the proof of concept's quick start
#   bench  bench_payload.py and bench_latency.py, against the processes this script starts
#   keep   leave the processes running until Ctrl-C, e.g. to look at the UI (see README.md)
#
# It resets the metadata database, so run it in a breeze container of its own.
set -euo pipefail

KIT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# /files is mounted from files/ in the Airflow checkout, so the logs outlive the container of breeze run.
OUT=${DEMO_OUT:-/files/aip125_demo/${BACKEND:-sqlite}}
DAGS=$OUT/dags
BODY=$OUT/body.json
CORE=http://localhost:8080
WEBHOOKS_ROOT=http://localhost:8081
WH=$WEBHOOKS_ROOT/webhooks

steps=("$@")
if [ ${#steps[@]} -eq 0 ]; then
    steps=(1a 1b)
fi
for step in "${steps[@]}"; do
    case $step in
        1a | 1b | bench | keep) ;;
        *) echo "Unknown argument '$step': use 1a, 1b, bench or keep" >&2; exit 2 ;;
    esac
done

export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$DAGS
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=$CORE/execution/
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$OUT/logs
export AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY=$OUT/logs/dag_processor
# Both API servers read it: the webhooks process serves only these names, and the core process lists
# their deliveries.
export AIRFLOW__WEBHOOKS__NAMES=demo,github

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

api() { # api PATH: GET /api/v2/PATH as admin
    curl -sf --max-time 60 -H "Authorization: Bearer $ADMIN" "$CORE/api/v2/$1"
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

records() { # records WEBHOOK DELIVERY_ID: how many delivery records the REST API lists for the sender's id
    api "webhooks/$1/deliveries?delivery_id=$2" | pyjson 'd["total_entries"]'
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
    # 130 is Ctrl-C, which ends keep.
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
    log "Setup: the providers, a fresh metadata database, and the connections webhook_demo and webhook_github"
    check_providers
    rm -rf "$OUT"
    mkdir -p "$DAGS"
    # Through the migrations, which add the inbound_delivery table and, on PostgreSQL, the GIN index on
    # asset_event.extra that bench_payload.py measures; a schema created from the ORM has no such index.
    airflow db reset -y --use-migration-files >"$OUT/db-reset.log" 2>&1 || fail "airflow db reset failed"
    # Before the webhooks process starts: it caches a missing connection for [webhooks] connection_cache_ttl.
    airflow connections add webhook_demo --conn-type inbound_webhook --conn-password demo-secret \
        --conn-extra '{"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event", "path_field": "service", "delivery_id_header": "X-Demo-Delivery"}'
    airflow connections add webhook_github --conn-type inbound_webhook --conn-password gh-secret \
        --conn-extra '{"source_type": "github"}'
}

is_healthy() {
    [ "$(http "$CORE/api/v2/monitor/health")" = 200 ] && [ "$(http "$WH/_health")" = 200 ]
}

start_airflow() {
    log "Starting the core and webhooks API servers, the scheduler and the Dag processor (logs in $OUT)"
    start core airflow api-server --apps core,execution --port 8080
    start webhooks airflow api-server --apps webhooks --port 8081
    start scheduler airflow scheduler
    start dag-processor airflow dag-processor
    local deadline=$((SECONDS + 180))
    until is_healthy; do
        [ "$(jobs -rp | wc -l)" -eq ${#PIDS[@]} ] || fail "an Airflow process exited"
        [ "$SECONDS" -lt "$deadline" ] || fail "the API servers did not answer their health checks"
        sleep 2
    done
    ok "airflow api-server --apps core,execution on :8080, airflow api-server --apps webhooks on :8081"
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
    # New Dags start paused, and a delivery that only paused Dags subscribe to records nothing.
    for dag_id in smoke_test_billing deploy_preview; do
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
    airflow webhooks check || fail "airflow webhooks check found problems"
    ok "airflow webhooks check"

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
    expect "$(api 'webhooks/demo/deliveries?delivery_id=d-1' |
        pyjson '[(r["outcome"], r["source_type"], r["candidates"]) for r in d["deliveries"]]')" \
        "[('no_subscribers', 'hmac_json', ['demo/billing/built', 'demo/*/built'])]" "the record of d-1"
    local viewer
    viewer=$(get_token viewer) || fail "no token for viewer from $CORE/auth/token"
    expect "$(http "$CORE/api/v2/webhooks/demo/deliveries" -H "Authorization: Bearer $viewer")" 403 \
        "deliveries for viewer, who may not read connections"
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
    local github=(github --url "$WH/github" --signature-header X-Hub-Signature-256 --signature-prefix sha256=)
    local labeled=$KIT/payloads/github_pull_request_labeled.json
    send 200 - "${github[@]}" --header 'X-GitHub-Event: ping' --header 'X-GitHub-Delivery: ping-1' \
        --data "@$KIT/payloads/github_ping.json"
    expect "$(pyjson 'd["body"]' <<<"$SENT")" "{}" "body answered to the ping"
    send 202 accepted "${github[@]}" --header 'X-GitHub-Event: pull_request' --header 'X-GitHub-Delivery: g-1' \
        --data "@$labeled"
    wait_for_runs deploy_preview 1
    expect "$(api 'assets/events?extra=delivery_id=g-1' |
        pyjson '[(e["name"], [r["dag_id"] for r in e["created_dagruns"]]) for e in d["asset_events"]]')" \
        "[('github/my-org/web/pull_request/labeled/deploy-preview', ['deploy_preview'])]" \
        "the topic event of g-1 and the run it started"
    # GitHub's redelivery repeats the delivery id and the body.
    send 200 duplicate "${github[@]}" --header 'X-GitHub-Event: pull_request' --header 'X-GitHub-Delivery: g-1' \
        --data "@$labeled"
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
' "$labeled" >"$OUT/github_pull_request_labeled_43.json"
    send 202 accepted "${github[@]}" --header 'X-GitHub-Event: pull_request' --header 'X-GitHub-Delivery: g-2' \
        --data "@$OUT/github_pull_request_labeled_43.json"
    wait_for_runs deploy_preview 2

    log "Phase 1b: the proof of concept's quick start, on core"
    send 202 accepted demo --url "$WH/demo" --delivery-id qs-1 --data "@$KIT/payloads/hmac_json_deployed.json"
    wait_for_runs smoke_test_billing 1
    local event record_id
    event=$(api 'assets/events?extra=delivery_id=qs-1') || fail "GET /api/v2/assets/events failed"
    record_id=$(api 'webhooks/demo/deliveries?delivery_id=qs-1' | pyjson 'd["deliveries"][0]["id"]') ||
        fail "no record of qs-1 in GET /api/v2/webhooks/demo/deliveries"
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
  UI, as admin/admin:  http://localhost:28080/webhook_deliveries/demo (with breeze run --forward-ports)
  Inside the container: $CORE (core API, UI) and $WH/<name> (webhooks)
  Logs:                 $OUT (/files is files/ in the Airflow checkout)
EOF
    wait
}

setup
start_airflow
for step in "${steps[@]}"; do
    "phase_$step"
    [ "$step" = keep ] || printf '\n%s: done\n' "$step"
done
