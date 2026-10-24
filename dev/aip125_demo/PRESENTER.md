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

- [Presenting AIP-125 with `demo.sh show` (DO NOT MERGE)](#presenting-aip-125-with-demosh-show-do-not-merge)
  - [Before you present](#before-you-present)
  - [The walkthrough](#the-walkthrough)
  - [The live-GitHub variant](#the-live-github-variant)
  - [If something goes wrong](#if-something-goes-wrong)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Presenting AIP-125 with `demo.sh show` (DO NOT MERGE)

A walkthrough for presenting the whole AIP, inbound webhooks in Phases 1a, 1b and 2, and the roadmap's
Phase 3: cancelling runs, in about 18 minutes, from one Airflow that `demo.sh show` sets up with a story
already in it. Each step says what to open, what to point at, and what to say. Times are a guide.

## Before you present

About ten minutes ahead:

1. On the host, from this branch: `breeze ui compile-assets` (once per change to the UI).
2. `breeze shell --backend postgres`, then inside it `bash dev/aip125_demo/demo.sh show`. It resets the
   database, starts Airflow and seeds the story, which takes a few minutes, then prints the URLs
   and the commands this walkthrough uses, and keeps running until Ctrl-C. To rehearse without a
   browser, `demo.sh show --check` does the same, checks the compiled UI and the API behind every page,
   and runs every command (see README.md). The preview runs it starts keep running for four hours
   (`PREVIEW_DEMO_SECONDS`, 14400) unless cancelled, and a close sent after that cancels nothing.
3. Open a second terminal on the host and run `breeze exec`: a shell in the same container, for the
   live commands. Keep the printed commands at hand in the first terminal.
4. Open <http://localhost:28080> and sign in as `admin`, password `admin`. Open the printed URLs in
   tabs in the order below.

Nothing below needs anything else: no GitHub account, no tunnel, no second machine. The last section
has the variant with a real GitHub repository.

## The walkthrough

### 1. What the AIP adds (1 minute)

Say: services such as GitHub, GitLab, Stripe or an ETL tool tell you that something happened by calling
a URL. Today Airflow has nowhere to receive that call, so people poll, run sensors, or put a gateway in
front that calls the REST API. AIP-125 adds inbound webhooks to the API server: Airflow verifies the
sender's signature, records each delivery and shows it in a deliveries feed (Phase 1a), and does one of
three things with it:

- **Topics** start Dags: a Dag schedules on a `WebhookTopic` such as "a pull request in my-org/web got
  the label deploy-preview" (Phase 1b).
- **Asset routes** record an event on a data asset a Dag already schedules on, without the Dag knowing
  about webhooks (Phase 2).
- **Webhook waits** resume a task that waits in a running Dag run, such as "until this pull request is
  closed", without holding a worker, a pool slot or a triggerer (Phase 2).

Point at: Admin → Connections. Each webhook is a connection named `webhook_<name>` of type Inbound
Webhook, which holds the shared secret and the source type: `github`, or the generic `hmac_json` and
`token_header` of the standard provider. The webhooks a deployment serves are listed in
`[webhooks] names`, and only an API server started with `--apps webhooks` serves them, so a Deployment
Manager can put it on its own process and network. Everything is experimental in Airflow 3.5.

### 2. Browse → Webhook Deliveries (Phase 1a, 3 minutes)

Open: **Browse → Webhook Deliveries** (the first printed URL).

Point at: one feed of every delivery to every webhook you may read, newest first, with the webhook, the
outcome, the sender's delivery id, the correlation key and the source type; filters in the bar, kept in
the URL. Say: this answers the first support question about webhooks, "why didn't my Dag run?". Users
who may read a webhook's connection see its deliveries, and never its payload here.

Open the details panel of a delivery (the second printed URL, GitHub's labeled pull request): it opens
over the same feed of every webhook. Point at, from top to bottom:

- the **outcome** with its explanation in plain words;
- the **attributes** the GitHub source type parsed, such as `repository`, `number` and `label`, which
  waits match on;
- the **topic names** the delivery offered, each with a copy button: a Dag author copies one into a
  `WebhookTopic`;
- the **actions**: here the topic event it recorded, linked to its asset.

Close the panel, which leaves you on the feed of every webhook, and walk the outcomes with the outcome
filter, one row each:

| Outcome | Row | Say |
|---|---|---|
| Accepted | `show-labeled-42`, `show-deployed-1`, `show-orders-2026-10-19` | It recorded events, so Dags run |
| No Subscribers | `show-built-1` | Nothing schedules on its topic names; the panel lists them, so you can see what a Dag would need |
| Dag Paused | `show-released-1` | Only a paused Dag (`release_notes`) subscribes. A redelivery after unpausing is processed again and starts it |
| Early | `show-opened-45`, `show-opened-46` | Nothing acted on it, but it has a correlation key (`my-org/web#45`), so it is kept for a webhook wait registered later |
| Expired | `show-opened-44` | It was early until the webhook kept too many newer deliveries for waits (show mode sets the limit to 4; the two Cancel Requested ones count too) |
| Resumed Wait | `show-merged-5000` | It resumed a waiting task; the panel links to it under Webhook Waits |
| Cancel Requested | `show-closed-7001`, `show-sync-7002` | It asked the scheduler to cancel runs; step 8 comes back to it |

### 3. The Webhooks tab (Phase 1a, 1 minute)

Open: the **Webhooks** tab of the same page, next to the **Deliveries** tab (printed as "The Webhooks
tab").

Point at: each webhook, whether `[webhooks] names` lists it, its connection status, its source type and
the last delivery it received. `jira` is listed but has no connection, so it shows **Not Found** with
the explanation; its deliveries are refused with 404. Say: `airflow webhooks check` reports the same
from the command line, with the details that the API never returns, such as why a connection is
misconfigured. Each row links to that webhook's deliveries.

### 4. A Dag run started by a webhook topic (Phase 1b, 2 minutes)

Open: the **Asset Events** tab of the run of `deploy_preview` (printed as "The topic-started Dag run").

Point at: the run was triggered by an asset event whose source is **Webhook**; that link goes back to
the delivery's details panel, which lists the same event under its actions. Open the Dag's **Code** tab: it schedules on
`WebhookTopic(webhook="github", path="my-org/web", event="pull_request", action="labeled",
detail="deploy-preview")`, which is an asset named `github/my-org/web/pull_request/labeled/deploy-preview`
in the group `webhook`. Say: a topic is an asset that only its webhook can record events on, so
everything Airflow does with assets, such as conditions, the asset graph and the events list, works for
webhooks too. The task reads the payload from the triggering event's extra.

### 5. An asset route's keyed event (Phase 2, 2 minutes)

Open: the delivery `show-orders-2026-10-19` in the feed (filter by the `etl` webhook), then the asset
it links to under **Asset Route Events** (printed as "The asset of the asset route").

Point at: the `etl` connection has an asset route: deliveries that offer the topic
`etl/orders/load.completed` also record an event on the data asset `s3://warehouse/orders`, with the
partition key read from the payload's `data.date`. The asset's event has the partition key
`2026-10-19` and the source Webhook, and `orders_by_day`, which has a `PartitionedAssetTimetable` and no
webhook code at all, ran for that day: the **Asset Events** tab of its run (printed as "The run of its
keyed event") shows the event, and the run has the partition key `2026-10-19`. Then open
`show-orders-no-date`: its route is under **Skipped Asset Routes** with the reason, "The payload has no
value at the route's partition key field". Say: routes need only edit rights on the connection, so they
are a Deployment Manager's tool, and the AIP's security section covers that.

### 6. A parked task resumed live (Phase 2, 4 minutes)

Open: `merge_gate_operator` run `show-pr-5001`, task `wait_for_merge`, **Details** tab (printed as
"Parked (operator), Details tab"). Open the Dag's **Code** tab next to it.

Point at: the code. `open_pr` returns the pull request number (here from the `pr_number` param, as in
live mode; without it, `open_pr` opens a real pull request through `github_default`),
`WaitForWebhookOperator` waits for a `pull_request` delivery whose `repository`, `number` and `action`
match, and `apply` checks `merged` in the payload. Then the task's state, **awaiting input**, and the
**Webhook Wait** panel: the webhook, linked to its deliveries; the status **Registered** and what it
means; the events and the match it waits for; the deadline. Open **Admin → Pools** (printed):
`default_pool` has no occupied slot, though two tasks are parked (the pool `previews` holds the running
preview of step 8). Say: a waiting task holds no worker, no pool slot and no trigger, so no triggerer has
to run; the wait is a row in the metadata database, a delivery to the API server resumes it, and the
scheduler times it out at its deadline.

In the `breeze exec` terminal, run the first printed command, which sends pull request 5001 closed and
merged. It answers `{"status": 202, "body": {"delivery_id": "live-5001", "outcome": "resumed_wait"}}`.
Refresh the Details tab: the wait is **Resumed**, with a link to the delivery. Within a few seconds
`wait_for_merge` and `apply` succeed. Open `wait_for_merge`'s **XCom** tab: its return value is the
delivery, with its attributes and payload, which `apply` received.

Then open `merge_gate_decorator` run `show-pr-5002` (printed): the same gate written with
`@task.webhook_wait`, whose function returns the match. Run the second command, which closes pull
request 5002 without merging: the wait resumes, and `apply` fails with "Pull request 5002 was closed
without merging". Say: GitHub sends `closed` for both, so the Dag decides from the payload.

If someone asks about timeouts, open `merge_gate_timeout` run `show-timeout-5004` (printed): it waited
30 seconds for nothing, the scheduler resumed it with a timeout event, and it failed with
`WebhookWaitTimeoutError`; its wait is **Timed Out**.

### 7. The early-delivery case (Phase 2, 1.5 minutes)

Say: a delivery can arrive before its task waits, for example when a pull request is merged while the
task that waits for it is still queued. Run the third command, which merges pull request 5003 before
anything waits for it: it answers `early`. Show it in the feed with its explanation. Then run the fourth
command, `airflow dags trigger merge_gate_operator --run-id show-pr-5003 --conf '{"pr_number": 5003}'`
(or trigger it from the UI with `pr_number` 5003). When `wait_for_merge` registers its wait, the wait
takes the early delivery at once, so the task resumes as soon as it parks and the run succeeds without
waiting; the delivery is now **Resumed Wait**. Say: only deliveries with a correlation key are kept, for
`[webhooks] early_delivery_ttl` (10 minutes by default; show mode sets a day), and a delivery that
started a Dag is never kept, so one delivery never both starts a Dag and resumes a wait later.

### 8. Phase 3: cancelling runs (roadmap, 3 minutes)

Say: a pull request that is closed should stop its preview environment, and a new commit should stop its
end-to-end tests, without anyone clicking in the UI. Phase 3 lets a Dag or a task declare which deliveries
cancel it.

Open: the Dag `preview_environment`, its **Code** tab, then its **Details** tab (printed as "The Dag's
Cancel On"). Point at the code: the Dag starts on a `WebhookTopic` (a pull request of `my-org/shop` labeled
`deploy-preview`), and has `cancel_on=[WebhookMatch("github", events=["pull_request"], match={"repository":
..., "action": "closed"}, correlation_key_conf="pr")]`; the task `run_e2e_tests` has its own `cancel_on`
for `synchronize`, with `cancel_state="skipped"`. The Details tab shows the condition under **Cancel On**.
A run is about a pull request if a delivery about it started the run, or if it was triggered with the
pull request as its conf key `pr`.

Open: the run of pull request 7001 (the first URL printed under Phase 3). Point at: the run is failed; its
note says "Cancelled by webhook github delivery show-closed-7001"; `serve_preview` is failed, because the
scheduler set it so and its worker killed it, calling its `on_kill`. Open the run's **Audit Log** (the
second URL): one `webhook_cancel` entry names the delivery.

Open: the details panel of `show-closed-7001` (printed). Point at: the outcome **Cancel Requested**, and
**Cancel Requests**: one request per Dag with a matching condition, three here, each applied by the
scheduler, `preview_environment`'s with the run it failed.

Open: `run_e2e_tests` of the run of pull request 7002 (printed as "Skipped by a new commit"). It was
skipped by `show-sync-7002`, a new commit, while `serve_preview` still runs. Then run the last printed
command, which closes pull request 7002, and refresh the run: within a few seconds it is failed too.

Say:

- the webhooks process only records the request; the scheduler applies it about once a second, and the
  task is killed at its next heartbeat;
- the webhooks process never touches runs, so the endpoint stays fast whatever the Dags hold;
- a cancel is a failure: `on_failure_callback` runs, with the reason `cancelled`, so the Dag can tear the
  preview down;
- this is roadmap, outside the vote on Phases 1 and 2.

### 9. Wrap up (1 minute)

Say what the vote covers and what it does not:

- The phases: 1a is the endpoint, the delivery records, the deliveries feed and the Webhooks overview
  (steps 2 and 3); 1b is topics (step 4). 1a and 1b ship together or not at all. Phase 2, asset routes
  and webhook waits (steps 5 to 7), targets 3.5 too, and whatever misses its freeze moves to 3.6.
  Phase 3, cancelling runs (step 8), is roadmap and not part of this vote.
- 3.5 ships webhooks without teams: in multi-team mode, topics, asset routes and waits serve only Dags
  without a team, and team support comes later with the multi-team owners.
- The GitHub source type is the reference; the generic `hmac_json` and `token_header` cover services
  that sign a body or send a token. A dbt Cloud source type and a dbt wait mode are help wanted.
- The docs: Administration and Deployment → Inbound Webhooks; Authoring and Scheduling → Scheduling on
  Inbound Webhooks, Waiting for Inbound Webhooks and Cancelling Runs from Inbound Webhooks; the standard
  provider's "Waiting for a Webhook Delivery" and the GitHub provider's "Inbound webhooks".
- `demo.sh 1a 1b 2 3` checks the behaviour behind these pages through the CLI and the REST API on
  PostgreSQL and MySQL, every outcome but `expired`, which only show mode shows; the pages themselves
  are covered by the UI's unit tests. README.md says how.

## The live-GitHub variant

The same phases with one real pull request instead of `send-test` deliveries: `demo.sh show --live`
sets up the same Airflow without the story, so the feed holds only the repository's deliveries, which is
better for screenshots. It needs a test repository you can push to, `gh` signed in on the host, and the
secret of the repository's webhook in a file. README.md's "Live mode" has the details.

Before you present:

1. On the host: `breeze ui compile-assets`, and put the webhook's secret in
   `files/aip125_live/github_secret` (mode 600; `files/` is not committed).
2. `breeze shell --backend postgres`, then inside it:
   `export GITHUB_DEMO_REPO=<owner>/<repository> GITHUB_WEBHOOK_SECRET_FILE=/files/aip125_live/github_secret`
   and `bash dev/aip125_demo/demo.sh show --live`. It starts a second API server that serves only the
   webhook `github` (`/webhooks/github`), on the host's port 25555, and prints the steps below with the
   repository filled in.
   A preview run keeps running for four hours after the label (`PREVIEW_DEMO_SECONDS`, 14400) unless
   cancelled, and a merge or close sent after that cancels nothing.
3. On the host, tunnel port 25555, not 28080, so the UI stays private:
   `ngrok http --url=<your ngrok domain> 25555`. `curl <tunnel URL>/webhooks/_health` must answer 200.
4. The repository's webhook has the payload URL `<tunnel URL>/webhooks/github`, the content type
   `application/json`, the secret, and the "Pull requests" events. Redeliver its ping from
   **Recent Deliveries**: Airflow answers 200.

Live, from a host terminal, run the printed `gh` commands one step at a time, and show each result:

1. Create a branch and open a pull request: its `opened` delivery is **Early** in the feed.
2. Add the label `deploy-preview` (Phase 1): **Accepted**, and `preview_environment` runs; its
   `serve_preview` keeps running.
3. In the `breeze exec` shell, trigger `merge_gate_operator` with the pull request's number (Phase 2):
   `wait_for_merge` parks, **Registered**, and Admin → Pools shows `default_pool` with no occupied slot.
4. Optionally push a new commit to the branch (Phase 3, task level): **Cancel Requested**;
   `run_e2e_tests` is skipped and `serve_preview` keeps running.
5. Merge the pull request (Phase 3): one `closed` delivery, **Resumed Wait**, both resumes the merge gate,
   which succeeds with the merge commit, and cancels the preview, whose run fails with the note. Its
   details panel lists the wait under **Webhook Waits** and the three requests under **Cancel Requests**.
   GitHub's **Recent Deliveries** page shows Airflow's answers.

Stop the tunnel after the demo.

## If something goes wrong

- The page has no **Webhook Deliveries** entry, or `show --check` says the UI has no webhook pages:
  run `breeze ui compile-assets` on the host and reload.
- The browser cannot reach `localhost:28080`: another breeze environment forwards the port, or the demo
  runs in `breeze run` without `--forward-ports`.
- A command answers 404 `Unknown webhook`, or a run fails at `wait_for_merge` with `webhook_not_found`:
  it ran against another Airflow, or the database was reset since; restart `demo.sh show`.
- The Dag has no **Cancel On** row, or a delivery's panel has no **Cancel Requests**: the UI was compiled
  before the Phase 3 layers; run `breeze ui compile-assets` again.
- Live mode: GitHub's **Recent Deliveries** shows 401 for a delivery: the secret in
  `GITHUB_WEBHOOK_SECRET_FILE` is not the webhook's; a timeout or 502: the tunnel does not reach the
  host's port 25555, or `demo.sh show --live` is not running.
- The logs of every process and task are in `files/aip125_demo/show-postgres` in the Airflow checkout.
