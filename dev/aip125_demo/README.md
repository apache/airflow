<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [AIP-125 proof of concept: demo kit (DO NOT MERGE)](#aip-125-proof-of-concept-demo-kit-do-not-merge)
  - [What `demo.sh` checks](#what-demosh-checks)
  - [Run it](#run-it)
  - [Benchmarks](#benchmarks)
  - [Not covered by the live run](#not-covered-by-the-live-run)
  - [Files](#files)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# AIP-125 proof of concept: demo kit (DO NOT MERGE)

This directory lets voters on AIP-125 run the checks of AIP section 9 for Phases 1a and 1b against the
code of this stack, and measure what payloads and deliveries cost. It is not part of any PR to be
merged: it is dropped at the post-vote split, when the stack is opened as PRs of their own.

Phase 2 (targeted events, asset routes, the dbt Cloud source type) is not built in this stack, so
nothing here checks it.

## What `demo.sh` checks

`demo.sh` starts `airflow api-server --apps core,execution` on port 8080, a separate
`airflow api-server --apps webhooks` on port 8081, the scheduler and the Dag processor, with
`[webhooks] names` set to `demo,github`. It creates two connections of type `inbound_webhook`:
`webhook_demo` (source type `hmac_json`, secret `demo-secret`, the delivery id in `X-Demo-Delivery`)
and `webhook_github` (source type `github`, secret `gh-secret`). Deliveries are sent with
`airflow webhooks send-test`, and checked through the REST API with a token for the breeze user
`admin`. Every check prints `ok:`; the first that fails stops the script and prints the logs.

Whatever its arguments, `demo.sh` first checks that `airflow providers webhook-sources` lists
`hmac_json`, `token_header` and `github`, and that `airflow providers hooks` lists the connection type
`inbound_webhook`, before it resets the database and creates the connections.

**`1a`, the inbound endpoint:**

- No plugin with "webhook" in its name is loaded, the proof-of-concept package
  `airflow-provider-inbound-webhooks` is not installed, and `airflow webhooks check` passes.
- The webhooks process answers `GET /webhooks/_health`, serves no `/docs`, `/redoc`, `/openapi.json`
  or `/api/v2`, and answers `GET /webhooks/demo` with 405. The core process answers `/webhooks/*`
  with a JSON 404.
- A name that `[webhooks] names` does not list gets 404, a bad signature 401, and a body over
  `[webhooks] max_body_bytes` (1 MiB) 413.
- A signed `hmac_json` delivery with a delivery id that no Dag subscribes to gets 202 with
  `{"delivery_id": "d-1", "outcome": "no_subscribers"}`. The first delivery recorded nothing, so its
  redelivery is processed again, and `GET /api/v2/webhooks/demo/deliveries` still lists one record,
  with the candidate names `demo/billing/built` and `demo/*/built`. The user `viewer`, who may not
  read connections, gets 403, and a request without a token 401.

**`1b`, topic events:**

- `dags/partitioned_topic_rejected.py`, a `PartitionedAssetTimetable` over a `WebhookTopic`, is a
  Dag import error. `dags/deploy_preview.py`, the example of AIP section 3.4, schedules on its
  `WebhookTopic`, an asset in the group `webhook`.
- A signed GitHub `ping` gets 200 with `{}` and is not recorded.
- A `pull_request` delivery that adds the label `deploy-preview` gets 202 `accepted`, and
  `deploy_preview` runs once; the topic event it recorded lists that run. GitHub's redelivery of it
  gets 200 `duplicate`, and there is still one record and one run. A second labeled pull request
  starts a second run.
- The proof of concept's quick start on core: `payloads/hmac_json_deployed.json`, the quick start's
  body with `version` and `environment` added so that the event's extra can be checked, sent to `demo`
  gets 202 `accepted`, and `dags/quickstart.py`, which schedules on the plain
  `Asset("demo/billing/deployed")`, runs. The topic event's extra holds `from_webhook`, `webhook`, `source_type`, `delivery_id`,
  `inbound_delivery_id` (the id of the delivery record), `correlation_key` and `payload`.

## Run it

It needs a breeze CI image built from this branch, and the `get_provider_info.py` of the standard and
github providers regenerated (the `update-providers-build-files` prek hook), so that they declare their
`webhook-sources` and the standard provider's connection type `inbound_webhook`. `demo.sh` checks both
first, whatever its arguments, and says what is missing.

```bash
breeze run --backend postgres bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b
breeze run --backend mysql bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b
```

`demo.sh` resets the metadata database through the migrations, so that the database is built by this
stack's migration and, on PostgreSQL, has the GIN index on `asset_event.extra` that a schema created
from the ORM lacks. `breeze run` gives each run a container and database of its own. `demo.sh` keeps
the logs of its processes, the task logs (under `logs/`) and its Dags folder in
`/files/aip125_demo/<backend>`, which is `files/aip125_demo/<backend>` in the Airflow checkout, so they
outlive the container; each run empties it first. `DEMO_OUT` sets another directory.

To look at the deliveries page as well, keep the processes running and forward the ports. `breeze run`
does not compile the UI, so compile it from this branch first, which builds the core UI and the
simple auth manager's login page. `--forward-ports` takes the host ports breeze forwards, such as 28080
for the API server and 25433 for PostgreSQL, so no other breeze environment that forwards them may be
running.

```bash
breeze ui compile-assets
breeze run --backend postgres --forward-ports bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b keep
```

Then open <http://localhost:28080/webhook_deliveries/demo> and sign in as `admin`, password `admin`.
The page is also reached from Admin → Connections, through the "View Deliveries" action of
`webhook_demo`. Ctrl-C stops the processes.

## Benchmarks

```bash
breeze run --backend postgres bash /opt/airflow/dev/aip125_demo/demo.sh bench
breeze run --backend postgres bash -c "BENCH_PAYLOAD_ARGS='--max-event-payload-bytes 65536' bash /opt/airflow/dev/aip125_demo/demo.sh bench"
breeze run --backend postgres bash -c 'BENCH_COUNT=500 BENCH_CONCURRENCY=8 bash /opt/airflow/dev/aip125_demo/demo.sh bench'
```

`bench` loads the demo Dags, then runs both scripts against the processes `demo.sh` started:

- `bench_payload.py` (spike S10) records deliveries of 1 KiB, 64 KiB and 1 MiB payloads, each as a
  topic event on 9 names in one transaction, through `AssetManager.register_asset_change` as the
  delivery pipeline does, without the payload cap. It prints, per size, the insert time per delivery,
  the growth of `asset_event` with its TOAST table and indexes, the growth of its GIN index on `extra`
  (PostgreSQL only), the latency and size of `GET /api/v2/assets/events` for the latest 50 events, and
  the latency of one delivery's events found by `extra`. At these table sizes PostgreSQL scans
  `asset_event` rather than use the GIN index, because its cost model does not count TOAST, so
  `GET by extra` reads and casts every event's extra; the `by extra plan` column names the scan the
  planner chose, and `by extra, GIN` times the indexed lookup. Each payload is built just under its
  size, so with `BENCH_PAYLOAD_ARGS='--max-event-payload-bytes 65536'`, the default cap, the 64 KiB
  payload is still copied and the 1 MiB one is not. `BENCH_PAYLOAD_ARGS` passes any other option of
  the script.
- `bench_latency.py` sends `BENCH_COUNT` (100) signed deliveries of `payloads/hmac_json_deployed.json`
  to `demo`, `BENCH_CONCURRENCY` (4) at a time. `smoke_test_billing` subscribes to them, so each
  records a topic event and queues a run. It prints p50 and p99 against the time Slack (3 s) and
  GitHub (10 s) wait for an answer, and fails unless every answer is 202 `accepted`. The percentiles
  leave out a first batch of `BENCH_CONCURRENCY` deliveries, which pay the webhooks process's
  first-use costs and are reported on their own line. The latencies are measured over loopback inside
  the container, without network, TLS or proxy time, so they are a lower bound of what a real sender
  waits.

The results go in the Draft PR description.

## Not covered by the live run

| What | Covered by |
|---|---|
| Rate limit (429), metrics, the 400, 500 and 503 answers | `airflow-core/tests/unit/api_fastapi/webhooks/test_app.py` |
| Concurrent copies of one delivery | `airflow-core/tests/unit/webhooks/test_pipeline_concurrency.py` |
| A source type two providers declare, `[webhooks] source_type_owners` | `airflow-core/tests/unit/webhooks/test_connections.py`, `airflow-core/tests/unit/always/test_providers_manager.py` |
| The content of the deliveries page | `WebhookDeliveries.test.tsx` (vitest) |
| Released providers | breeze runs the providers from source |

## Files

| File | What it is |
|---|---|
| `demo.sh` | The checks above, and `bench` and `keep` |
| `bench_payload.py` | Spike S10: payloads in `asset_event.extra` |
| `bench_latency.py` | Delivery latency against senders' timeouts |
| `dags/quickstart.py` | The proof of concept's quick start Dag, its code unchanged |
| `dags/deploy_preview.py` | The `WebhookTopic` example of AIP section 3.4 |
| `dags/partitioned_topic_rejected.py` | A `PartitionedAssetTimetable` over a `WebhookTopic`, refused |
| `payloads/github_ping.json` | A synthetic GitHub `ping` |
| `payloads/github_pull_request_labeled.json` | A synthetic GitHub `pull_request` that adds the `deploy-preview` label |
| `payloads/hmac_json_deployed.json` | The quick start's `hmac_json` body, with `version` and `environment` added so that the event's extra can be checked |
