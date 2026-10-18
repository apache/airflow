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

A walkthrough for presenting the whole AIP, inbound webhooks in Phases 1a, 1b and 2, in about 15
minutes, from one Airflow that `demo.sh show` sets up with a story already in it. Each step says what to
open, what to point at, and what to say. Times are a guide.

## Before you present

About ten minutes ahead:

1. On the host, from this branch: `breeze ui compile-assets` (once per change to the UI).
2. `breeze shell --backend postgres`, then inside it `bash dev/aip125_demo/demo.sh show`. It resets the
   database, starts Airflow and seeds the story, which takes a few minutes, then prints the URLs
   and the commands this walkthrough uses, and keeps running until Ctrl-C. To rehearse without a
   browser, `demo.sh show --check` does the same, checks the compiled UI and the API behind every page,
   and runs every command (see README.md).
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
| Expired | `show-opened-44` | It was early until the webhook kept too many newer early deliveries (show mode sets the limit to 2) |
| Resumed Wait | `show-merged-5000` | It resumed a waiting task; the panel links to it under Webhook Waits |

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

Point at: the code. `open_pr` returns the pull request number (here from the `pr_number` param; in live
mode it opens a real pull request), `WaitForWebhookOperator` waits for a `pull_request` delivery whose
`repository`, `number` and `action` match, and `apply` checks `merged` in the payload. Then the task's
state, **awaiting input**, and the **Webhook Wait** panel: the webhook, linked to its deliveries; the
status **Registered** and what it means; the events and the match it waits for; the deadline. Open
**Admin → Pools** (printed): `default_pool` has no occupied slot, though two tasks are parked. Say: a
waiting task holds no worker, no pool slot and no trigger, so no triggerer has to run; the wait is a row
in the metadata database, a delivery to the API server resumes it, and the scheduler times it out at
its deadline.

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

### 8. Wrap up (1 minute)

Say what the vote covers and what it does not:

- The phases: 1a is the endpoint, the delivery records, the deliveries feed and the Webhooks overview
  (steps 2 and 3); 1b is topics (step 4). 1a and 1b ship together or not at all. Phase 2, asset routes
  and webhook waits (steps 5 to 7), targets 3.5 too, and whatever misses its freeze moves to 3.6.
- 3.5 ships webhooks without teams: in multi-team mode, topics, asset routes and waits serve only Dags
  without a team, and team support comes later with the multi-team owners.
- The GitHub source type is the reference; the generic `hmac_json` and `token_header` cover services
  that sign a body or send a token. A dbt Cloud source type and a dbt wait mode are help wanted.
- The docs: Administration and Deployment → Inbound Webhooks; Authoring and Scheduling → Scheduling on
  Inbound Webhooks and Waiting for Inbound Webhooks; the standard provider's "Waiting for a Webhook
  Delivery" and the GitHub provider's "Inbound webhooks".
- `demo.sh 1a 1b 2` checks the behaviour behind these pages through the CLI and the REST API on
  PostgreSQL and MySQL, every outcome but `expired`, which only show mode shows; the pages themselves
  are covered by the UI's unit tests. README.md says how.

## The live-GitHub variant

The same story with a real pull request instead of a `send-test` delivery, at step 6. It needs a test
repository and a token that can create branches, contents and pull requests in it.

Before you present: start a tunnel to the host's port 28080, such as
`cloudflared tunnel --url http://localhost:28080`, and add a webhook to the repository with the payload
URL `<tunnel URL>/webhooks/github`, the content type `application/json`, the secret `gh-secret` and the
"Pull requests" event. Start the demo with the repository set, so that the Dags match on it:
`export GITHUB_DEMO_REPO=<owner>/<repository>` in the container before `demo.sh show`, and once it runs,
`airflow connections add github_default --conn-type github --conn-password <token>` in the
`breeze exec` shell.

At step 6, trigger `merge_gate_operator` from the UI without `pr_number`. `open_pr` opens a pull request,
and its log prints the link. Open it on GitHub and merge it: GitHub's delivery arrives in the feed as
`resumed_wait` and the run finishes, as with the printed command. GitHub's **Recent Deliveries** page
for the webhook shows Airflow's answer, and its **Redeliver** button gets `200 duplicate`.

For a shorter live step, trigger `pr_open_and_wait` instead: it opens a pull request titled with its
`title` param, `wait_for_close` parks until the pull request is merged or closed on GitHub, and
`report` prints which.

The tunnel exposes the whole API server, the UI included, behind the `admin` password, so stop it
after the demo.

## If something goes wrong

- The page has no **Webhook Deliveries** entry, or `show --check` says the UI has no webhook pages:
  run `breeze ui compile-assets` on the host and reload.
- The browser cannot reach `localhost:28080`: another breeze environment forwards the port, or the demo
  runs in `breeze run` without `--forward-ports`.
- A command answers 404 `Unknown webhook`, or a run fails at `wait_for_merge` with `webhook_not_found`:
  it ran against another Airflow, or the database was reset since; restart `demo.sh show`.
- The logs of every process and task are in `files/aip125_demo/show-postgres` in the Airflow checkout.
