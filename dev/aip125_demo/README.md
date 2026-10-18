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

- [AIP-125 proof of concept: demo kit (DO NOT MERGE)](#aip-125-proof-of-concept-demo-kit-do-not-merge)
  - [What `demo.sh` checks](#what-demosh-checks)
  - [Run the checks](#run-the-checks)
  - [Present the AIP: `demo.sh show`](#present-the-aip-demosh-show)
  - [Live mode: a real GitHub repository](#live-mode-a-real-github-repository)
  - [Benchmarks](#benchmarks)
  - [Not covered by the live run](#not-covered-by-the-live-run)
  - [Files](#files)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# AIP-125 proof of concept: demo kit (DO NOT MERGE)

This directory lets voters on AIP-125 run the checks of AIP section 9 for Phases 1a, 1b and 2 against
the code of this stack, present the whole AIP from one running Airflow, and measure what payloads and
deliveries cost. It is not part of any PR to be merged: it is dropped at the post-vote split, when the
stack is opened as PRs of their own.

Phase 2 is covered as this stack builds it: asset routes, and webhook waits through
`WaitForWebhookOperator` and `@task.webhook_wait` with GitHub `pull_request` deliveries. The dbt Cloud
source type is not built (it is help wanted), so nothing here checks it.

## What `demo.sh` checks

`demo.sh 1a 1b 2` starts `airflow api-server --apps core,execution` on port 8080, a separate
`airflow api-server --apps webhooks` on port 8081, the scheduler and the Dag processor, with the
LocalExecutor and `[webhooks] names` set to `demo,github,etl,jira`. It creates three connections of type
`inbound_webhook`:

| Connection | Source type | Secret | Notes |
|---|---|---|---|
| `webhook_demo` | `hmac_json` | `demo-secret` | Signature in `X-Signature`, delivery id in `X-Demo-Delivery` |
| `webhook_github` | `github` | `gh-secret` | |
| `webhook_etl` | `hmac_json` | `etl-secret` | Delivery id in `X-Etl-Delivery`, and the asset route of the standard provider's `inbound_webhook.rst` |

`jira` is listed without a connection, so that the webhooks overview has a webhook that cannot be
served. Deliveries are sent with `airflow webhooks send-test` (GitHub ones with
`--signature-header X-Hub-Signature-256 --signature-prefix sha256=`, since a `github` connection sets no
signature header), and checked through the CLI and the REST API with a token for the breeze user
`admin`. Every check prints `ok:`; the first that fails stops the script and prints the logs.

Whatever its arguments, `demo.sh` first checks that `airflow providers webhook-sources` lists
`hmac_json`, `token_header` and `github`, and that `airflow providers hooks` lists the connection type
`inbound_webhook`, before it resets the database and creates the connections.

**`1a`, the inbound endpoint, the deliveries feed and the overview:**

- No plugin with "webhook" in its name is loaded, the proof-of-concept package
  `airflow-provider-inbound-webhooks` is not installed, `airflow webhooks check demo github etl` passes,
  and `airflow webhooks check jira` exits 1 and names the problem.
- The webhooks process answers `GET /webhooks/_health`, serves no `/docs`, `/redoc`, `/openapi.json`
  or `/api/v2`, and answers `GET /webhooks/demo` with 405. The core process answers `/webhooks/*`
  with a JSON 404.
- A name that `[webhooks] names` does not list gets 404, as does `jira`, which has no connection; a bad
  signature gets 401, and a body over `[webhooks] max_body_bytes` (1 MiB) 413.
- A signed `hmac_json` delivery that no Dag subscribes to gets 202 with
  `{"delivery_id": "d-1", "outcome": "no_subscribers"}`. It recorded nothing, so its redelivery is
  processed again, and `GET /api/v2/webhooks/demo/deliveries` still lists one record, with the
  candidate names `demo/billing/built` and `demo/*/built` and no correlation key.
- A GitHub `pull_request` delivery that nothing acts on gets 202 `early`, not `no_subscribers`: the
  GitHub source type gives it the correlation key `my-org/web#41`, so it is kept for a webhook wait
  (decision P2-15). Its redelivery is processed again too, and there is still one record.
- `GET /api/v2/webhooks/~/deliveries` lists both webhooks' deliveries, newest first, and narrows them
  with `webhook_names` (once or repeated) and `source_type`. `GET /api/v2/webhooks` lists `demo`, `etl`,
  `github` and `jira`, each listed, with its connection status (`not_found` for `jira`), source type and
  last outcome.
- `GET /ui/auth/menus`, which the UI builds its menus from, authorizes `Webhook Deliveries` for `admin`:
  the Browse entry of the deliveries view. The checks run without a browser, so they check the API
  behind the view rather than the view itself, which vitest covers (see
  [Not covered by the live run](#not-covered-by-the-live-run)).
- The user `viewer`, who may not read connections, gets 403 for `demo`'s deliveries and an empty page
  from `~` and from `GET /api/v2/webhooks`; a request without a token gets 401.

**`1b`, topic events:**

- `dags/partitioned_topic_rejected.py`, a `PartitionedAssetTimetable` over a `WebhookTopic`, is a
  Dag import error. `dags/deploy_preview.py`, the example of AIP section 3.4, schedules on its
  `WebhookTopic`, an asset in the group `webhook`.
- A signed GitHub `ping` gets 200 with `{}` and is not recorded.
- A `pull_request` delivery that adds the label `deploy-preview` gets 202 `accepted`, and
  `deploy_preview` runs once; the topic event it recorded lists that run, and the delivery's record
  lists the event under `actions.asset_events`. GitHub's redelivery of it gets 200 `duplicate`, and
  there is still one record and one run. A second labeled pull request starts a second run.
- A delivery that only the paused `dags/release_notes.py` subscribes to gets 202 `dag_paused`. Once the
  Dag is unpaused, the sender's redelivery is processed again, gets 202 `accepted`, and starts it.
- A GitHub `pull_request` `closed` delivery that only the paused `dags/teardown_preview.py` subscribes
  to gets 202 `early`, not `dag_paused`: it has the correlation key `my-org/web#42`, so it is kept for a
  webhook wait. Once the Dag is unpaused, GitHub's redelivery of it, with the same `X-GitHub-Delivery`
  and body, is processed again in full (decision P2-15, as amended), gets 202 `accepted`, lists the topic
  event under `actions.asset_events` of its one record, and starts the Dag.
- The proof of concept's quick start on core: `payloads/hmac_json_deployed.json`, the quick start's
  body with `version` and `environment` added so that the event's extra can be checked, sent to `demo`
  gets 202 `accepted`, and `dags/quickstart.py`, which schedules on the plain
  `Asset("demo/billing/deployed")`, runs. The topic event's extra holds `from_webhook`, `webhook`,
  `source_type`, `delivery_id`, `inbound_delivery_id` (the id of the delivery record), `correlation_key`
  and `payload`.

**`2`, asset routes and webhook waits:**

The asset route check is here rather than under `1b`, because asset routes are a Phase 2 deliverable.

- Asset route: `payloads/hmac_json_orders_loaded.json`, a `load.completed` delivery about the `orders`
  table, sent to `etl` gets 202 `accepted`. Its record lists, under `actions.asset_routes`, the event
  it recorded on `s3://warehouse/orders` with the partition key `2026-10-19` from `data.date`; the
  event's extra names the webhook and the delivery record; and `dags/orders_by_day.py`, which schedules
  on that asset with a `PartitionedAssetTimetable`, runs once with that partition key. The same delivery
  without `data.date` gets 202 `no_subscribers`, and its record lists the route under
  `actions.skipped_asset_routes` with the reason `partition_key_missing`.
- Webhook waits, the checks of the Phase 2 plan's section 5, and the refusal of a wait without a timeout
  that AIP section 9's Phase 2 check names (check 8). The merge gates are triggered through the
  REST API with the Dag param `pr_number` (send-test mode), and wait for a `pull_request` `closed`
  delivery about that pull request in `my-org/configs`, built from
  `payloads/github_pull_request_closed_merged.json` or `..._unmerged.json`:

  | # | What happens | What is checked |
  |---|---|---|
  | 1 | `merge_gate_operator` (`WaitForWebhookOperator`) waits for pull request 4242, which is then merged | While `wait_for_merge` is `awaiting_input`, its wait (`GET .../taskInstances/wait_for_merge/webhookWaits`) is `registered` with its events and match, and `default_pool` has 0 occupied slots. The delivery gets 202 `resumed_wait`; its record in the feed lists the wait's task under `actions.waits`; the wait is `resumed`, with the record's id as `inbound_delivery_id`; the run succeeds; the XCom of `wait_for_merge` is the delivery, and that of `apply` the merge commit |
  | 2 | `merge_gate_decorator` (`@task.webhook_wait`) waits for pull request 4243 | The same as 1 |
  | 3 | `merge_gate_operator` waits for pull request 4244, which is closed without merging | 202 `resumed_wait`; `wait_for_merge` succeeds, `apply` fails, and its log says "Pull request 4244 was closed without merging" |
  | 4 | Pull request 4245 is merged before `merge_gate_operator` runs for it | The delivery gets 202 `early` with the correlation key `my-org/configs#4245`; the wait registered later takes it, the run succeeds, the wait is `resumed` with that record, and the record becomes `resumed_wait` |
  | 5 | `merge_gate_timeout` waits 30 seconds for pull request 4246, and nothing comes | `wait_for_merge` fails, its log says `WebhookWaitTimeoutError` and "No matching delivery arrived before", and its wait is `timed_out` within a minute of its deadline |
  | 6 | GitHub redelivers check 1's delivery, with the same `X-GitHub-Delivery` and body | 200 `duplicate`; one record; check 1's `wait_for_merge` keeps its state, try number and single `resumed` wait |
  | 7 | Two runs of `merge_gate_operator` wait for pull request 4247 | The second registration is refused: its `wait_for_merge` fails, its log says `wait_key_taken` and the status 409, it has no wait, and the first run stays parked until a delivery resumes it |
  | 8 | `merge_gate_no_timeout` waits for pull request 4248 with `WaitForWebhookOperator` and no `timeout`, while `[webhooks] default_wait_timeout` is unset (`demo.sh` sets it empty) | The register-wait route refuses it: `wait_for_merge` fails, its log says `timeout_required` and the status 422, and it has no wait |

  Checks 1, 2, 3 and the first run of 7 start together, and `default_pool` is checked to have 0
  occupied slots while all four tasks are parked. Check 5 starts after check 2 and is checked last;
  check 8 runs after check 7.

## Run the checks

It needs a breeze CI image built from this branch, and the `get_provider_info.py` of the standard and
github providers regenerated (the `update-providers-build-files` prek hook), so that they declare their
`webhook-sources`, the standard provider's connection type `inbound_webhook` and its `webhook_wait`
task decorator. `demo.sh` checks the first two first, whatever its arguments, and says what is missing.

```bash
breeze run --backend postgres --answer no "bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b 2"
breeze run --backend mysql --answer no "bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b 2"
```

Pass the command as one quoted string, as breeze takes options such as `-p` for itself otherwise.
`--answer no` keeps an image built from the whole stack rather than rebuilding it whenever a layer's
dependency files differ from it. Each takes about seven minutes, most of it the database reset, the
start-up of each `airflow` command, and the merge gates' runs.

`demo.sh` resets the metadata database through the migrations, so that the database is built by this
stack's migrations and, on PostgreSQL, has the GIN index on `asset_event.extra` that a schema created
from the ORM lacks. `breeze run` gives each run a container and database of its own. `demo.sh` keeps
the logs of its processes, the task logs (under `logs/`), its Dags folder and the payloads it sent in
`/files/aip125_demo/<backend>`, which is `files/aip125_demo/<backend>` in the Airflow checkout, so they
outlive the container; each run empties it first. `DEMO_OUT` sets another directory.

To look at the UI after the checks, add `keep`, which leaves the processes running until Ctrl-C, and
forward the ports (see the next section for compiling the UI):

```bash
breeze run --backend postgres --answer no --forward-ports "bash /opt/airflow/dev/aip125_demo/demo.sh 1a 1b 2 keep"
```

## Present the AIP: `demo.sh show`

`show` sets up one Airflow to present the whole AIP from, with a story already in it, and prints the
URLs to open and the commands to run live. [PRESENTER.md](PRESENTER.md) is the walkthrough. Unlike the
checks, it runs one `airflow api-server --apps all,webhooks` on port 8080, which serves the UI, the REST
API, the Execution API and the webhooks, as a small deployment would.

Compile the UI from this branch first, which builds the core UI and the simple auth manager's login
page; neither `breeze shell` nor `breeze run` does it:

```bash
breeze ui compile-assets
breeze shell --backend postgres
# inside the container:
bash dev/aip125_demo/demo.sh show
```

`breeze shell` publishes the container's port 8080 on the host's port 28080 (`WEB_HOST_PORT` in
`dev/breeze/src/airflow_breeze/global_constants.py`; `breeze run --forward-ports` does too), so the UI
is at <http://localhost:28080>. Sign in as `admin`, password `admin`: breeze sets up the simple auth
manager with these users, and `admin` sees everything. No other breeze environment that forwards the
ports may be running. `show` resets the database of the `breeze shell` environment.

The story it seeds, through `send-test` and the REST API:

| Delivery or run | Outcome or state |
|---|---|
| GitHub `pull_request` labeled `deploy-preview` (`my-org/web#42`) | `accepted`; `deploy_preview` runs (a topic-started run) |
| `demo` `deployed` for `billing` | `accepted`; `smoke_test_billing` runs (the quick start) |
| `demo` `built` for `billing` | `no_subscribers` |
| `demo` `released` for `billing` | `dag_paused`: `release_notes` stays paused |
| `etl` `load.completed` for `orders` on 2026-10-19 | `accepted` through the asset route; `orders_by_day` runs with that partition key |
| `etl` `load.completed` for `orders` without a date | `no_subscribers`; the route is skipped as `partition_key_missing` |
| GitHub `pull_request` opened, `my-org/web#44`, `#45` and `#46` | `early`; the third makes the first `expired` |
| `merge_gate_operator` run `show-pr-5000`, then pull request 5000 merged | `resumed_wait`; the run succeeds |
| `merge_gate_operator` run `show-pr-5001` | parked in `awaiting_input` |
| `merge_gate_decorator` run `show-pr-5002` | parked in `awaiting_input` |
| `merge_gate_timeout` run `show-timeout-5004` | fails 30 seconds later, with a `timed_out` wait |

`jira` is listed without a connection, so the Webhooks tab shows it as not found. For the story, show
mode sets `[webhooks] early_delivery_max_per_webhook` to 2, so that a third early GitHub delivery
expires the first, and `[webhooks] early_delivery_ttl` to a day, so that the early case still works
however long the presentation runs. The commands it prints resume `show-pr-5001` with a merged pull
request, `show-pr-5002` with one closed without merging, and send pull request 5003 merged before
triggering the run that waits for it, which then resumes at once. Run them in a second terminal in the
container, opened with `breeze exec`. Ctrl-C stops everything.

`demo.sh show --check` does the same set-up and prints the same guide, then checks what the pages are
built on, without a browser: that `/` serves the compiled UI and its main script defines the routes
`webhook_deliveries`, `webhook_deliveries/:webhookName` and `webhook_overview` (the core process answers
any path it does not route with the UI's `index.html`, so a status would prove nothing), that
`GET /ui/auth/menus` authorizes the Browse entry `Webhook Deliveries`, that
`GET /api/v2/webhooks/~/deliveries?id=` finds the delivery whose details panel the printed URL opens, and
that `/webhooks/_health`, `GET /api/v2/webhooks` (with `jira` not found),
`GET /api/v2/webhooks/~/deliveries` (with all six outcomes), the source asset events of the
topic-started run and of the keyed run, and the `webhookWaits` route of both parked tasks answer as the
walkthrough says. It then runs the printed commands, as the presenter would, checks that they resume the
runs as described, and exits, so it can run without a terminal:

```bash
breeze run --backend postgres --answer no "bash /opt/airflow/dev/aip125_demo/demo.sh show --check"
```

## Live mode: a real GitHub repository

The merge gates can wait for a real pull request instead of a `send-test` delivery. Triggered without
`pr_number`, `open_pr` opens a pull request in `$GITHUB_DEMO_REPO` through the `github_default`
connection, on a new branch with one new file, and the run waits for it to be closed.

1. Pick a test repository you can push to, and a token that can create branches, contents and pull
   requests in it.
2. Start a tunnel from the internet to the host's port 28080, such as
   `cloudflared tunnel --url http://localhost:28080` or `ngrok http 28080`, and note its HTTPS URL. The
   tunnel exposes the whole API server, not only `/webhooks`, so stop it after the demo.
3. In the repository's Settings → Webhooks, add a webhook with the payload URL
   `<tunnel URL>/webhooks/github`, the content type `application/json`, the secret `gh-secret` (the
   password of `webhook_github`) and only the "Pull requests" event. GitHub's `ping` gets 200 and is
   not recorded.
4. In the container, before `demo.sh show`: `export GITHUB_DEMO_REPO=<owner>/<repository>`, so that the
   Dag processor and the workers see it, and after it has started, in a `breeze exec` shell:
   `airflow connections add github_default --conn-type github --conn-password <token>`.
5. Trigger `merge_gate_operator` (or `merge_gate_decorator`) from the UI without `pr_number`.
   `open_pr`'s log prints the pull request it opened; merge or close it on GitHub, and watch the
   delivery arrive in Browse → Webhook Deliveries and resume the run. Or trigger `pr_open_and_wait`
   (its `title` param names the pull request): it opens a pull request the same way, waits until it is
   merged or closed, and its `report` task prints which, by whom and with which merge commit.

With `GITHUB_DEMO_REPO` set, the Dags match on that repository, and `demo.sh` writes it into the
payloads it sends, so the seeded story and the printed commands still work. The checks, which run
without it, use `my-org/configs`; they load `pr_open_and_wait` but never trigger it. Live mode was tried
against a real repository on 2026-10-21: labeled pull requests started their topic Dags, and
`pr_open_and_wait` runs resumed on the `closed` delivery of the pull request each opened.

## Benchmarks

```bash
breeze run --backend postgres --answer no "bash /opt/airflow/dev/aip125_demo/demo.sh bench"
breeze run --backend postgres --answer no "bash -c \"BENCH_PAYLOAD_ARGS='--max-event-payload-bytes 65536' bash /opt/airflow/dev/aip125_demo/demo.sh bench\""
breeze run --backend postgres --answer no "bash -c 'BENCH_COUNT=500 BENCH_CONCURRENCY=8 bash /opt/airflow/dev/aip125_demo/demo.sh bench'"
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
| A delivery racing a wait's registration (spike S6) | `airflow-core/tests/unit/webhooks/test_waits_concurrency.py` |
| A source type two providers declare, `[webhooks] source_type_owners` | `airflow-core/tests/unit/webhooks/test_connections.py`, `airflow-core/tests/unit/always/test_providers_manager.py` |
| Multi-team: topic events, asset routes and waits serve only Dags without a team | `airflow-core/tests/unit/webhooks/test_topics.py`, `test_asset_routes.py`, `airflow-core/tests/unit/api_fastapi/execution_api/versions/head/test_inbound_waits.py` |
| Wait refusals other than `wait_key_taken` and `timeout_required`, retries and clears of a waiting task, a delivery taken back by a new try | `airflow-core/tests/unit/webhooks/test_wait_registration.py`, `test_wait_matching.py`, `test_wait_resumption.py` |
| `expired`: shown by `demo.sh show`, not asserted by the checks | `airflow-core/tests/unit/webhooks/test_wait_matching.py` |
| The pages in a browser: the deliveries view, its details panel, the Webhooks tab, a task's wait panel | vitest: `WebhookDeliveries.test.tsx`, `DeliveryDetails.test.tsx`, `Webhooks.test.tsx`, `WebhookWait.test.tsx`, `AssetEvent.test.tsx` |
| Live GitHub deliveries | Live mode above, by hand |
| Released providers | breeze runs the providers from source |

## Files

| File | What it is |
|---|---|
| `demo.sh` | The checks above, `bench`, `keep`, and `show` |
| `PRESENTER.md` | How to present the AIP with `demo.sh show` in about 15 minutes |
| `bench_payload.py` | Spike S10: payloads in `asset_event.extra` |
| `bench_latency.py` | Delivery latency against senders' timeouts |
| `dags/quickstart.py` | The proof of concept's quick start Dag, its code unchanged |
| `dags/deploy_preview.py` | The `WebhookTopic` example of AIP section 3.4 |
| `dags/partitioned_topic_rejected.py` | A `PartitionedAssetTimetable` over a `WebhookTopic`, refused |
| `dags/release_notes.py` | A `WebhookTopic` Dag that starts paused, for `dag_paused` |
| `dags/teardown_preview.py` | A GitHub `WebhookTopic` Dag that starts paused, for an `early` delivery that starts it once redelivered |
| `dags/orders_by_day.py` | A `PartitionedAssetTimetable` on `s3://warehouse/orders`, which the `etl` asset route feeds |
| `dags/merge_gate_operator.py` | The merge gate with `WaitForWebhookOperator` |
| `dags/merge_gate_decorator.py` | The merge gate with `@task.webhook_wait` |
| `dags/merge_gate_timeout.py` | `WaitForWebhookOperator` with a 30-second timeout |
| `dags/merge_gate_no_timeout.py` | `WaitForWebhookOperator` without a timeout, refused |
| `dags/pr_open_and_wait.py` | Live mode: opens a pull request in `$GITHUB_DEMO_REPO`, waits with `@task.webhook_wait` until it is merged or closed, and prints which |
| `payloads/github_ping.json` | A synthetic GitHub `ping` |
| `payloads/github_pull_request_labeled.json` | A synthetic GitHub `pull_request` that adds the `deploy-preview` label; also the base of the `opened` deliveries |
| `payloads/github_pull_request_closed_merged.json` | A synthetic GitHub `pull_request` closed and merged, in `my-org/configs`; `demo.sh` rewrites its number |
| `payloads/github_pull_request_closed_unmerged.json` | The same, closed without merging |
| `payloads/hmac_json_deployed.json` | The quick start's `hmac_json` body, with `version` and `environment` added so that the event's extra can be checked |
| `payloads/hmac_json_orders_loaded.json` | An `etl` `load.completed` delivery for the `orders` table on 2026-10-19 |
