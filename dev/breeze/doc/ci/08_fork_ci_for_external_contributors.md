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

- [Fork CI for external contributors (AIP-120)](#fork-ci-for-external-contributors-aip-120)
  - [Goals and non-goals](#goals-and-non-goals)
  - [Overview](#overview)
  - [Configuration](#configuration)
  - [Eligibility](#eligibility)
  - [Project CI gate](#project-ci-gate)
  - [Fork mode CI](#fork-mode-ci)
  - [Reconciler](#reconciler)
  - [Monitor workflow](#monitor-workflow)
  - [`breeze ci audit`](#breeze-ci-audit)
  - [Documentation changes](#documentation-changes)
  - [Rollout and rollback](#rollout-and-rollback)
  - [Security considerations](#security-considerations)
  - [Known limitations](#known-limitations)
  - [Testing](#testing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Fork CI for external contributors (AIP-120)

This document is the design of the CI changes implementing
[AIP-120: Run CI for external contributions in contributor forks](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/451974711/AIP-120+Run+CI+for+external+contributions+in+contributor+forks).
The policy itself and its rationale live in the AIP and in the dev list discussions
([first](https://lists.apache.org/thread/w74655o8y1jzrzcnygntyyovocf7zmzj),
[second](https://lists.apache.org/thread/ky392mrrcmj312y1r276r30p584n804c), which agreed to
start with an opt-in pilot).
This document describes how the policy is mechanised.

## Goals and non-goals

Goals:

- External contributors get CI feedback from a plain `git push` to their fork. No pull request
  against their own fork, no manual workflow dispatch, no sorting through workflow files.
- Project CI (`apache/airflow` runners) does not run for gated pull requests. The decision is
  made by one function with one configuration file.
- A single scheduled reconciler owns the lifecycle of gated pull requests: labelling, drafting,
  undrafting, the state comment, the nudge after workflows are enabled, closing after
  inactivity, and closing a pull request that tampered with the gate to get project CI. It is
  idempotent, so missed or duplicated scheduled runs are harmless.
- Maintainers and contributors see the fork CI state where they already look: a commit status
  on the pull request head, red / yellow / green, linking to the run in the fork.
- A contributor can verify their whole setup locally with `breeze ci audit` before pushing.
- Everything ships dormant and is switched on by a one-line configuration change.
- Before it is switched on for everyone, the whole flow can be piloted on real traffic by
  committers on single pull requests and by volunteer external contributors on all their new
  pull requests, without affecting anyone else.

Non-goals:

- The one-time reset of the open pull request backlog (AIP-120 §9.1). The design makes it a
  configuration change, but it is decided separately.
- Any change to how committer, collaborator or exempted-organisation pull requests are tested.
- Event-driven mutation of pull requests. Nothing in this design uses `pull_request_target`.
  All writes come from the scheduled reconciler.

## Overview

```
                 contributor fork                          apache/airflow
   ┌──────────────────────────────────┐      ┌──────────────────────────────────────────┐
   │ git push feature-branch          │      │ pull_request event                        │
   │   └─ ci-amd.yml (push trigger)   │      │   └─ ci-amd.yml                           │
   │        gate job: fork -> run     │      │        gate job: eligibility -> skip      │
   │        build-info: fork mode     │      │        (build-info and everything after   │
   │          changed files from      │      │         are skipped for gated PRs)        │
   │          compare API vs upstream │      │                                           │
   │        public runners only       │      │ fork-ci-monitor.yml (every 15 minutes)    │
   └──────────────────────────────────┘      │   └─ breeze ci fork-ci-reconcile          │
                     ▲                        │        discover new gated PRs -> label    │
                     │ GraphQL: checkSuites   │        per labelled PR: fork state ->     │
                     │ REST: workflow state   │          draft/undraft, labels, comment,  │
                     └────────────────────────│          commit status with run link,     │
                                              │          close after 7 days or when the   │
                                              │          PR tampered with the gate        │
                                              │        unknown failures -> Slack          │
                                              └──────────────────────────────────────────┘
```

Four pieces, in dependency order:

1. **Eligibility** — `dev/breeze/src/airflow_breeze/utils/fork_ci_gate.py`, one pure function
   and one configuration file. Used by the gate job and by the reconciler.
2. **Project CI gate** — a `gate` job at the head of `ci-amd.yml`.
3. **Fork mode CI** — a push trigger plus fork-aware selective checks.
4. **Reconciler** — `breeze ci fork-ci-reconcile`, run by `fork-ci-monitor.yml` on a schedule.

Plus `breeze ci audit` for the contributor's local setup, and documentation.

## Configuration

One file, `.github/fork-ci-gate.toml`, parsed with the standard library `tomllib`:

```toml
# Fork CI gate (AIP-120). See dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md

# Master switch. When false the gate job lets every pull request run project CI, except
# pull requests labelled `use fork ci` and pull requests of [pilot] participants. Push
# filtering for apache/airflow branches applies regardless.
enabled = false

# Pull requests created before this instant are never gated, whatever their author.
# Moving the date back is how the backlog reset (AIP-120 §9.1) would be applied.
enabled_since = 2026-01-01T00:00:00Z

# Days without any fork CI activity after which a gated pull request is closed.
close_after_days = 7

# Organisations exempted under AIP-120 §2.2. Keys are display names used in comments,
# values are GitHub logins. Changes go through a reviewed pull request.
[allowlist]
# "Example Org" = ["login-one", "login-two"]

# Opt-in pilot participants. Keys are GitHub logins, values are the instant they joined.
# Their pull requests created at or after that instant are gated whether or not `enabled`
# is set. Contributors opt in and out with a pull request editing this table.
[pilot]
# "login-three" = 2026-10-20T00:00:00Z
```

The file lives in the repository rather than in an Actions variable because ASF projects
cannot self-serve Actions variables, and because exemption changes should be reviewed like
any other change. The gate job reads it from the pull request's *base* branch, never from
the pull request's own tree, so a pull request cannot change its own eligibility.

## Eligibility

`fork_ci_gate.decide(event, config) -> Decision` is a pure function. `event` carries the
fields both callers have: event name, repository, ref, head repository, author login,
author association, pull request creation time and the pull request's current labels.
`Decision` is `run` (a bool) plus a `reason` from a fixed set, used in logs, comments and by
the reconciler.

The rules, first match wins. The push rules are independent of `enabled` because the `'**'`
push trigger exists whether or not the gate is on, and today no CI runs for pushes to
`main` or to ad-hoc branches in `apache/airflow` (post-merge coverage is the cron canary).
The label and pilot rules are also independent of `enabled`; see "Override labels" and
"Contributor pilot" below.

The `fork_schedule` rule exists because enabling `ci-amd.yml` in a fork also enables its
cron trigger, and selective checks force the full matrix for `schedule` events. Without it
every enabled fork would run the full canary matrix twice a day on the contributor's
concurrency cap. A manual `workflow_dispatch` in a fork still runs.

| Rule | Decision | Reason |
|---|---|---|
| Event is `schedule` and repository is not `apache/airflow` | skip | `fork_schedule` |
| Event is `schedule` or `workflow_dispatch` | run | `scheduled` |
| Event is `push` and repository is not `apache/airflow` | run | `fork_push` |
| Event is `push` to `apache/airflow` and the branch matches the pre-existing push patterns (`v*-test`, `providers-*/v*`, `chart/v*x-test`, `airflow-ctl/v*-test`) | run | `canonical_branch` |
| Event is `push` to `apache/airflow`, any other branch | skip | `adhoc_branch` |
| Pull request carries `use project ci` | run | `label_project_ci` |
| Pull request head repository is `apache/airflow` | run | `head_is_upstream` |
| Pull request carries `use fork ci` | skip | `label_fork_ci` |
| Author login is in `[pilot]` and the pull request was created at or after that login's join instant | skip | `pilot` |
| `enabled` is false | run | `gate_disabled` |
| Pull request created before `enabled_since` | run | `before_enabled_since` |
| Author association is `MEMBER`, `OWNER` or `COLLABORATOR` (see below for private membership) | run | `trusted_author` |
| Author login is in `[allowlist]` | run | `allowlisted` |
| Anything else | skip | `external` |

The table applies once the base branch carries the gate. When the base branch has neither
`.github/fork-ci-gate.toml` nor `fork_ci_gate.py` (the pull request that introduces the gate,
or a release branch cut before it), the job runs project CI with reason `base_without_gate`,
as it did before the gate existed. A base branch with only one of the two files fails the job.

`head_is_upstream` comes before the label and pilot rules because a branch of `apache/airflow`
has no fork to run CI in, and its pushes are `adhoc_branch` skips: `use fork ci` on such a pull
request would otherwise leave it with no CI at all.

GitHub reports an author whose membership of the `apache` organisation is private as
`CONTRIBUTOR`, so a committer with private membership looks external and is gated. Neither the
gate nor the reconciler can look up the author's permission instead: that endpoint needs push
access, which the read-only token of a fork `pull_request` run and the monitor's token do not
have. A committer who is gated this way makes their `apache` membership public
(<https://github.com/orgs/apache/people>) or is added to `[allowlist]`.

"Gated" throughout this document means a pull request whose decision reason is `external`,
`pilot` or `label_fork_ci`. The reconciler labels exactly those; an `adhoc_branch` skip is
not a pull request and is never labelled.

Logins are compared case-insensitively in `[allowlist]` and `[pilot]`, as GitHub does.

### Override labels

Two labels let a committer or collaborator (anyone with triage rights; external contributors
cannot set labels) override the automatic decision for one pull request:

| Label | Effect |
|---|---|
| `use project ci` | Project CI runs for a pull request that would otherwise be gated. Reasons: the change needs the full matrix, self-hosted runners or secrets, or a maintainer has adopted the pull request. |
| `use fork ci` | The fork regime applies to a pull request that would otherwise run on project CI. Reasons: a committer testing the machinery on their own pull request, or a large pull request from an allowlisted organisation that should be verified in the fork first. |

Because both rules sit before the `enabled` switch, `use fork ci` works while
`enabled = false`. That is the committer pilot: label a handful of pull requests and the
gate, the fork run, the reconciler and the commit status all operate for those only.

Consequences for the other components:

- The gate job reads the pull request's labels live from the API (one read-only call) rather
  than from the event payload, so labelling a pull request and pressing "Re-run all jobs"
  switches it without a new push.
- Discovery in the reconciler also picks up open pull requests carrying `use fork ci`
  without `fork ci required`, regardless of `enabled`.
- A pull request carrying both `fork ci required` and `use project ci` is *released*: the
  reconciler removes `fork ci required`, undrafts it if the bot drafted it, edits the state
  comment to say project CI is in use, and posts a `success` status with the description
  "Project CI in use (label `use project ci`)". Project CI then runs on the next push or
  re-run.
- Both override labels on one pull request resolve to project CI and are reported to Slack as
  a conflict.

### Contributor pilot

The committer pilot exercises the machinery but not the experience it is meant for: an
external contributor setting up their fork from the instructions, waiting for their own
fork run and being drafted and undrafted by the reconciler. The `use fork ci` label does not
work for that. It is set after the pull request is opened, so the first `pull_request` run
has already used project CI, and a maintainer would have to label every pull request the
volunteer opens.

The `[pilot]` table covers it. A contributor opts in with a pull request that adds their
login and a join instant to `[pilot]`; any committer can merge it. Once the entry is on the
base branch, every pull request they open is gated from its first run, whether or not
`enabled` is set, and goes through exactly the flow described in this document: setup
comment, draft, fork run, commit status, undraft, close after inactivity, gate tampering
check. Pull requests they opened before the join instant keep project CI.

The gate reads the table from the base branch, so the entry takes effect when it merges,
while the join instant is compared with pull request creation. The join instant must
therefore not be earlier than the merge: pull requests opened in between ran project CI,
are labelled on the next reconciler run, and one of them that changes `.github/workflows/`
would be closed for gate tampering. The `check-fork-ci-gate-config` prek hook rejects a newly
added `[pilot]` instant, an `enabled_since` moved later, or `enabled` switched on while
`enabled_since` is still in the past; contributors use the next day at 00:00 UTC. Moving
`enabled_since` earlier is the backlog reset and is allowed. A base branch without the file,
such as a release branch the file is backported to, has no gate yet, so its entries are not
checked against it. In CI the hook fetches the pull
request's base branch to compare with, because the shallow checkout has none; a push in a
contributor's fork has no base branch, so it fetches `apache/airflow`'s `main`. Without any
base to compare with, the hook fails rather than comparing the change with itself. A merge
after that instant still leaves the window open; the
closure is reported to Slack as an attention item, and the maintainer reopens the pull
request with `use project ci`.

Leaving is a pull request that removes the line. Pull requests already labelled
`fork ci required` stay in the state machine until a maintainer adds `use project ci`, as
for any other gated pull request. A maintainer can also release a single pull request with
`use project ci` at any time.

Opting in only ever takes project CI away from the contributor, so the opt-in pull request
needs no more review than checking that the login is the author's own. The table is read
from the base branch like the rest of the file, so adding oneself in the same pull request
that should be gated has no effect on that pull request.

`[pilot]` is meant to be short-lived: it is emptied when `enabled = true` is set, because
from then on the participants are gated as `external` anyway.

`MEMBER` means a member of the `apache` GitHub organisation, which every ASF committer is.
`COLLABORATOR` is the triage role granted through `.asf.yaml`. The static `COMMITTERS` list in
`global_constants.py` is not consulted; it drives runner selection only and drifts from
reality.

The module has no imports from `airflow_breeze` so that the gate job can execute the file
directly with the runner's `python3` without installing breeze. The reconciler imports it.

## Project CI gate

`ci-amd.yml` gets a first job, `gate`, on `ubuntu-slim`:

1. Sparse checkout of `.github/fork-ci-gate.toml` and
   `dev/breeze/src/airflow_breeze/utils/fork_ci_gate.py` from the base branch
   (`github.base_ref` for pull requests, the default branch for pushes in `apache/airflow`,
   `github.ref` otherwise), `fetch-depth: 1`. A push in `apache/airflow` has no base branch and
   its decision depends only on the pushed ref, so it takes the gate from the default branch:
   taking it from the pushed branch would make a push to a branch without the gate files, such as
   a release branch cut before it, look like `base_without_gate` and run project CI where it never
   ran before. A push in a fork keeps the pushed commit's gate, checked out by `github.sha`. The
   fork's default branch is the contributor's to change outside the pull request, and so is the
   pushed branch's later tip, which a re-run of an earlier push would check out by `github.ref`; a
   gate there that skips every job would produce a green run for a commit nothing in the pull
   request shows. A gate change in the pushed commit is in the pull request's files and makes the
   green run `green_unverified`. While the default branch has no gate yet, which is the case
   until the gate merges, a push in `apache/airflow` takes the gate from the pushed commit
   instead. Only committers can push there, and otherwise every pushed branch would run the
   full project CI as `base_without_gate`.
2. For pull requests, one read-only API call for the current labels
   (`gh api repos/apache/airflow/pulls/<n> --jq '.labels[].name'`), so override labels added
   after the event are seen on a re-run. If that call fails, the gate uses the labels of the
   event payload instead. It never falls back to running project CI: anyone able to exhaust
   the shared token quota would otherwise switch the gate off for every pull request.
3. `python3 dev/breeze/src/airflow_breeze/utils/fork_ci_gate.py` with the event payload
   fields and the labels passed as environment variables. It writes `project-ci=true|false`
   and `reason=...` to `GITHUB_OUTPUT`. A base branch with neither file skips the script and
   writes `project-ci=true` with `reason=base_without_gate`.
4. For a gated pull request, the PR title check of `ci-image-checks.yml`
   (`scripts/ci/prek/check_no_conventional_commit_message.py`, from the same sparse checkout).
   Project CI is skipped and the fork run is a `push` event without a title, so this is the
   only place the title is checked.

`build-info` gains `needs: [gate]` and `if: needs.gate.outputs.project-ci == 'true'`. Every
other job already depends on `build-info`, so a gated pull request produces a run in which the
gate job succeeds in seconds and everything else is skipped. No red status is attached to the
pull request; the reconciler's comment explains what to do.

`ci-arm.yml` keeps its triggers: it has no `pull_request` trigger and never runs in forks. Its
`push.branches` list is the source of `CANONICAL_PUSH_BRANCH_FILTERS` in `fork_ci_gate.py`
(compiled with GitHub's filter semantics, where `*` is any characters except `/`), and its
`notify-slack` condition matches `ci-amd.yml`'s.
The `check-ci-workflows-in-sync` prek hook allowlist is extended to permit the `gate` job and
the `build-info` dependency to exist only in the AMD workflow.

## Fork mode CI

### Trigger

`ci-amd.yml` `push.branches` becomes `'**'`; the previous explicit patterns move into the
eligibility function as the canonical set. In a fork every push therefore triggers the
workflow and the gate lets it run. In `apache/airflow` the gate skips pushes to `main` and to
ad-hoc branches exactly as the old trigger list did, so the only observable change there is a
gate-only run (seconds on `ubuntu-slim`, no other jobs) for such pushes. The workflow's
concurrency group includes the event name for runs that are not pull requests, so such a
push run on `main` does not cancel the scheduled canary running on the same ref.

Pushes to `main` in a fork (for example `gh repo sync`) run too. The compare against
upstream yields no changed files, so such a run performs basic checks only.

### Selective checks in fork mode

`SelectiveChecks` gains `is_fork_push`: event is `push` and repository is not
`apache/airflow`. In fork mode:

- **Changed files** come from the cross-repository compare API,
  `GET /repos/apache/airflow/compare/<base>...<fork-owner>:<sha>`, three-dot semantics, so
  the set equals what a pull request against upstream would show. The fork's `main` is never
  used as a base, so a stale fork does not inflate the change set. `<base>` is the branch line
  of the pushed code, breeze's default branch at that commit (`main`, or the release branch it
  was cut from), because a branch name such as `backport-<sha>-v3-1-test` cannot be trusted to
  tell. A renamed file counts under both paths. The API caps the file list at 300; at the cap,
  and when the compare fails, the run falls back to "run everything", UI E2E tests included,
  rather than failing.
- **Previous versions** of changed files, for the `pyproject.toml` and provider dependency
  bump checks, are read at the compare's merge base in `apache/airflow`
  (`GET /repos/apache/airflow/contents/<path>?ref=<merge-base>`). The pushed commit's parent
  would be the contributor's own previous tip after they merge upstream `main`, so a release
  manager's bump from `main` would count as theirs. A file missing at the merge base is new; any
  other read failure fails the run rather than letting the check pass unseen.
- **Labels** come from the open `apache/airflow` pull request whose head is
  `<fork-owner>:<branch>`, since a push event carries none. `full tests needed`,
  `allow provider dependency bump`, `skip common compat check` and the other labels work as on
  a pull request; a label added later takes effect on the next fork run.
- `_should_run_all_tests_and_versions` treats a fork push like a pull request. Today any
  `push` runs the full matrix, which would cost a contributor ~220 minutes per push.
- `is_committer_build` is always false. Forks have no self-hosted runners; a committer pushing
  to their own fork must get public runners or the jobs would queue forever once AIP-118
  lands.
- Canary detection is unchanged and already false outside `apache/airflow`.

`breeze ci selective-check` gets the compare result through a new `--changed-files-from-api`
path taken automatically when `is_fork_push` holds; the `git diff-tree` path stays for every
other event. The `GITHUB_TOKEN` of the fork run is sufficient: the compare reads public data.

Images, caches and registries already work per repository (`GITHUB_REPOSITORY` is the fork),
which is how contributors run CI in forks today via pull requests against their own `main`.

## Reconciler

`breeze ci fork-ci-reconcile` is the only writer. It runs on a schedule and is idempotent: its
inputs are the current state of GitHub, its outputs are the minimal set of mutations to reach
the desired state. It never depends on having seen an earlier event.

### Discovery

Each run performs two passes.

**Pass 1, discovery.** GraphQL search for open pull requests in `apache/airflow` updated in
the last 24 hours without the `fork ci required` label, plus any open pull request carrying
`use fork ci` without `fork ci required`, fetching `authorAssociation`, `author.login`,
`createdAt`, `headRepository`, `baseRefName` and `labels`. Each is classified with
`fork_ci_gate.decide` against the gate configuration of its base branch, the one its gate job
reads too: the default branch's comes from the reconciler's checkout, any other base branch's
from the contents API once per run. Those with a gated reason get the label. A base branch with
neither gate file is never gated, as its gate job runs project CI (`base_without_gate`), and
one with only one of the two files is reported as unknown. The
24-hour window overlaps many 15-minute cadences, so a lost scheduled run changes nothing. A
pull request whose author later becomes a committer keeps the label until a maintainer
removes it or adds `use project ci`; the reconciler does not un-gate on its own.

**Pass 2, reconcile.** GraphQL search for open pull requests with the `fork ci required`
label, 25 per page, fetching for each: number, `isDraft`, labels, `authorAssociation`,
`author.login`, `createdAt`, `headRefName`, `headRefOid`, `headRepository { nameWithOwner }`,
the last `ConvertToDraftEvent` actor, the last `UnlabeledEvent` for `use project ci` and the
last `ReopenedEvent` from `timelineItems`, the current `Fork CI / Tests (AMD)` commit status
on the head commit (`commits(last: 1) { nodes { commit { status { context(name: ...) {
state targetUrl description } } } } }`), and the reconciler's own state comment
(`comments(first: 100)`, the earliest comment by `github-actions` with the marker, paging
through the remaining comments of the rare pull request with more). The reconciler creates that
comment on first contact and only edits it afterwards, so a marker posted by anyone else, or
echoed later by another workflow, is ignored. The fork state is then fetched in batches of 50 with one GraphQL
query per batch using aliases. Owner and name come from the pull request's
`headRepository { nameWithOwner }`, never a fixed `airflow`, because a fork can be renamed or
created as `airflow-1` on a name collision:

```graphql
pr123: repository(owner: "<fork-owner>", name: "<fork-name>") {
  object(oid: "<headRefOid>") { ... on Commit {
    checkSuites(first: 30) { nodes {
      createdAt status conclusion
      workflowRun { url workflow { resourcePath } }
    } }
  } }
}
```

The latest suite of the workflow file `ci-amd.yml` whose run was triggered by `push` is the one
that matters. It is matched by file, not by the workflow's display name, which a pull request can
change, and only `push` runs count because only they execute the workflow from the head commit
itself; a `pull_request` run inside the fork, into the fork's own `main`, runs a merge the
contributor controls. For pull requests
whose recorded state is `setup_required` (or that have no recorded state yet), one REST call
per pull request reads `GET /repos/<fork>/actions/workflows/ci-amd.yml` to distinguish
"workflows disabled" from "enabled but not yet pushed". This is bounded by the number of
pull requests in that state, not by the total.

### State machine

| Observed fork state | Draft | `ready for maintainer review` | Commit status | Comment |
|---|---|---|---|---|
| **setup_required**: workflow state is `disabled_fork`, or the fork has no `ci-amd.yml` | draft | remove | `error` (red), link to the fork's Actions page for `ci-amd.yml` | create or update: how to enable, with the exact `gh workflow enable ci-amd.yml -R <fork>` line, and `breeze ci audit` |
| **awaiting_push**: enabled, no run for the head SHA | draft | remove | `pending` (yellow), link to the fork's Actions page | on transition from setup_required: post a **new** comment asking to push again (an edit would not notify) |
| **running**: run in progress | draft | remove | `pending` (yellow), link to the run | update state and run link |
| **failed**: conclusion is failure, cancelled or timed out | draft | remove | `failure` (red), link to the run | update with the run link |
| **green**: conclusion success and the pull request does not change CI-defining files | undraft\* | add | `success` (green), link to the run | update: ready for review, run link |
| **green_unverified**: conclusion success, but the pull request changes CI-defining files (see "Trusting a green fork run") | draft | remove | `pending` (yellow), link to the run | update: a maintainer must run project CI; labelled `needs project ci` |
| setup_required or awaiting_push for `close_after_days` | unchanged | unchanged | unchanged | close with a new comment; reopening restarts the clock |
| **ungated**: the base branch has no gate, for example after retargeting to an older release branch | undraft\* | unchanged | `success` (green), link to the pull request's checks | update: project CI is in use; `fork ci required` is removed |
| **gate_bypassed**: a recorded project CI run of the pull request's branch bypassed the gate (see "Gate tampering") | draft | remove | `error` (red), link to the project CI run that should not have happened | post a **new** comment explaining why, then close; takes precedence over every row above |

\* only if the last convert-to-draft event on the pull request was by `github-actions[bot]`;
a pull request the author drafted stays a draft.

Rules that sit under the table:

- **Gate tampering wins.** The `gate_bypassed` check is evaluated first for every pull
  request whose eligibility reason is `external` or `pilot`; when it holds, the fork state is not
  consulted at all. A pull request that was already `green` and undrafted is drafted again,
  loses the review label and is closed on the same run.

- **A new push resets the row.** The head SHA is recorded in the state comment. A push to a
  green pull request is observed as `running` or `awaiting_push` on the next run, which drafts
  it again and removes the review label, so maintainers never see an unverified push in the
  queue.
- **Author drafts are respected.** "draft" in the table means the reconciler converts to draft
  if the pull request is not already one. On `green` it undrafts only when GitHub's timeline
  shows the last convert-to-draft actor was `github-actions[bot]`. A pull request the author
  drafted stays a draft until the author marks it ready, and is then treated like any other
  on the next run. This is derived from GitHub's own record, not from the reconciler's
  memory, so it survives missed runs.
- **`use project ci` is the escape hatch.** Only pull requests carrying `fork ci required`
  are touched, and adding `use project ci` takes a pull request out of the state machine on the
  next run (see "Override labels"). Removing `fork ci required` by hand is not enough: it updates
  the pull request, so discovery finds it again and labels it back while it is still gated.
- **Deleted head repository** is reported as unknown (see below) and otherwise left alone.
- **The close clock** starts at the `since` timestamp recorded for the current state and
  resets on any state change. Closing for inactivity records `closed` in the marker, so the
  first run after a reopen sees a state change from `closed` and starts a fresh clock rather
  than closing the pull request again at once. Only a reopen recorded in the timeline after
  the marker counts: an open pull request with a `closed` marker and no such reopen is one whose
  closing comment or close failed, and the next run finishes the closure. The same holds for a
  `gate_bypassed` marker, where only a reopen by someone other than the author counts, so the
  closure is finished even after the offending run has left the tampering check's window.
- **`fork ci required` goes last.** When a run removes the label (on release, or for an
  ungated base branch), it does so after every other change, so a failure on the way leaves
  the label in place and the next run retries the rest.

### Trusting a green fork run

The fork run executes the workflow files, the breeze sources and the CI scripts of the pushed
commit. A pull request that changes them decides what its own fork CI checks, and could make it
pass trivially. When a new head commit turns green, the reconciler lists the pull request's
changed files once (`GET /repos/apache/airflow/pulls/<n>/files`) and then reads the head commit
again: the files endpoint lists the current head, so when the head moved meanwhile nothing is
decided or recorded for the old commit, and the next run decides for the new one. If any file is under `.github/`,
`dev/breeze/`, `scripts/ci/`, `scripts/docker/` or `scripts/in_container/`, is a
`Dockerfile*` or a `.pre-commit-config.yaml`, or the list reaches GitHub's 3000-file cap so the
rest cannot be seen, the state is `green_unverified` instead of `green`. The pull request stays
a draft, the status is pending, and it is labelled `needs project ci` so maintainers can list
these pull requests, review the change and add `use project ci`. It is not posted to Slack:
the label already lists these pull requests, and a post for every new green commit would bury
the CI breakages that channel is for.
The reconciler removes `needs project ci` as soon as the state is anything else. A renamed file
counts with both its old and its new path, so moving a workflow out of `.github/` is a change
to `.github/` too.

The decision is read back from the `Fork CI / Tests (AMD)` commit status on the head commit,
which only `apache/airflow`'s own tokens can write, so it is made once per commit and cannot be
forged through a comment. The changed files depend on the base branch as well, so a decision
recorded before the pull request was last retargeted (a `BaseRefChangedEvent` in the timeline
newer than the status) is made again, and the status is written again to date it.

This is a guard against the cheapest way to fake a green run, not a guarantee. Any change can
weaken the tests it ships with (a `conftest.py` that skips tests, for example), exactly as it
can for a pull request that runs project CI. A green fork run tells a maintainer the pull
request is worth reviewing; the review remains what decides whether it is merged.

### Gate tampering

`pull_request` runs use the workflow files of the pull request's merge commit, so a gated
pull request can edit `ci-amd.yml` to drop the `gate` job or its `if` on `build-info`, or add
a workflow file with a `pull_request` trigger, and get project CI anyway. The gate job cannot
prevent this (see "Security considerations"); the reconciler detects it after the fact and
closes the pull request, which also stops further pushes from triggering `pull_request` runs
in `apache/airflow`.

The check applies only to pull requests whose `fork_ci_gate.decide` reason is `external` or
`pilot`, recomputed from the pass 2 fields. A pilot participant's pull request is gated from
its creation, so it never ran project CI legitimately unless `use project ci` was on it,
which is accounted for below, or it was opened before the `[pilot]` entry merged
(see "Contributor pilot"). Committers piloting with `use fork ci` are excluded:
their pull request legitimately ran project CI before the label was added.

The check works from the runs `apache/airflow` recorded, not from the head commit. Once per
reconciler run, paginated REST calls list the workflow runs created in the last 6 hours for
every event that runs the workflow files of a pull request's merge commit: `pull_request`,
`pull_request_review` and `pull_request_review_comment`
(`GET /repos/apache/airflow/actions/runs?event=<event>&created=>=...`). They are matched to pull
requests by head repository and branch. Looking at every recent run of
the branch, rather than only at the head commit, is what stops a contributor from pushing a
tampered commit and then a clean one to hide the tampered run. The 6-hour window spans many
15-minute cadences. Runs created before the most recent removal of `use project ci` are
ignored, because they ran legitimately while the label was on. So are runs created before the
most recent reopen of the pull request by someone other than its author, which is how a
maintainer brings back a pull request closed for tampering; reopening one whose gate is still
missing triggers a new run, which is caught again. A reopen by the author does not count, so
closing and reopening one's own pull request cannot hide a tampered run. A run counts as a
bypass when both signals hold:

1. **The run did project CI work.** Either the default branch's version of the run's workflow
   is not triggered by the run's event, which covers an injected workflow and a trigger the
   pull request added to an existing one (for example `pull_request` on `ci-arm.yml`, which
   has no gate), or it is a `ci-amd.yml` run that has run for more than 5 minutes, finished or
   still running, and whose jobs (`GET .../actions/runs/<id>/jobs`) show more than the gate: a
   started job other than `Fork CI gate` that was not skipped, a missing or duplicated gate job,
   or a gate job that ran for more than 5 minutes. The last case catches project CI work done in
   a job renamed to the gate's name. Runs and jobs that have not started do not count, because a
   queued job reports its creation as `started_at`: a gate job waiting more than 5 minutes for a
   runner would otherwise look like project CI work. A running run is judged as it goes, so one
   tampered to outlast the 6-hour window is still caught. A gate-only run finishes in about a
   minute, so jobs are listed only for the rare long runs. The triggers are read from the
   monitor's own checkout of `main`, so no API call is needed; a pull request into a release
   branch is judged by `main`'s workflows.
2. **The run's own commit changes workflow files.** The compare of the base branch with that
   commit (`GET /repos/apache/airflow/compare/<base>...<fork-owner>:<sha>`, first page only:
   the API returns `files` on the first page and lists commits on the others) must show a path
   under `.github/workflows/`. It is the run's commit, not the pull request's current head,
   so a later push that reverts the workflow change does not clear the signal. A change set
   at the compare API's 300-file cap counts as changing workflows, so padding a tampered
   commit with files cannot hide the change.

GitHub lists at most 1000 runs for such a query. When more were created in the window, the
reconciler reports on Slack that the tampering check was incomplete for that run.

Signal 2 exists so that project CI runs with a legitimate cause never close anything: a pull
request created before an `enabled_since` that was later moved back for the backlog reset, or
one that was released with `use project ci` and later re-gated, has full runs but does not
touch workflows. Signal 1 exists so that an external pull request that edits `ci-amd.yml`
legitimately, and leaves the gate intact, is not closed for touching the file.

When both hold the reconciler records `gate_bypassed` in the state comment marker, drafts the
pull request, removes `ready for maintainer review`, posts an `error` status whose
`target_url` is the project CI run that should not have happened, posts a new comment and
closes. The comment says which run was observed, that changes to `.github/workflows/` from a
gated pull request cannot be verified in `apache/airflow` without a maintainer, and that a
maintainer can reopen the pull request with `use project ci` if the workflow change is
wanted. The closure is also reported to Slack as an attention item, with the pull request
number, the author login and the run link.

Reopening without a change triggers a new `pull_request` run that bypasses the gate again,
and the pull request is closed again. Reopening after a push that restores the gate
triggers a fresh `pull_request` run in `apache/airflow` in which only `gate` executes, so
signal 1 no longer holds and the pull request re-enters the fork state machine. `use project
ci` releases it like any other gated pull request; a maintainer who wants to keep an external
workflow change has to leave that label on.

### State comment

One comment per pull request, created on first contact and edited afterwards. It starts with
an HTML marker the reconciler parses:

```
<!-- fork-ci-gate: {"state": "awaiting_push", "sha": "abc123", "since": "2026-09-19T10:00:00Z"} -->
```

followed by the human text for the current state. Editing does not notify, which is the
point: state churn is silent. The three moments that need the author's attention, the nudge
after enabling workflows, the close after inactivity and the close for gate tampering, are
new comments.

All comment texts live in `dev/breeze/src/airflow_breeze/utils/fork_ci_messages.py` as
templates so wording can be reviewed in one place. Every comment ends with the AI-attribution
footer required by `contributing-docs/25_maintainer_pr_triage.md`. The texts must say that CI
is free for the contributor and finite for the project, must not characterise the recipient,
and must give the exact command to run.

### Commit status

The reconciler posts a commit status with context `Fork CI / Tests (AMD)` on the pull
request head SHA in `apache/airflow` (`POST /repos/apache/airflow/statuses/<sha>`). That is
how the state becomes visible without opening the pull request: red for `setup_required`
and `gate_bypassed` (`error`) and `failed` (`failure`), yellow for `awaiting_push` and
`running` (`pending`), green for `green` (`success`). `target_url` is the run in the fork
when one exists and the fork's Actions page for `ci-amd.yml` otherwise, so a maintainer can
open the fork run in one click; for `gate_bypassed` it is the project CI run that was
observed. The `description` is a short sentence under 140 characters that always ends with
where to look next.

Statuses are immutable and per SHA. The reconciler compares the desired `(state,
target_url, description)` with the current one fetched in the pass 2 query and posts only on
a difference, so a stable pull request costs nothing and a new push naturally starts with no
status until the next run. The status feeds `statusCheckRollup`, which is what the pull
request list, the merge box and the triage skill already read.

Commit statuses were chosen over check runs because a check run created with the workflow
token is attached to the creating workflow's check suite and its details link then opens the
monitor run rather than `details_url`. A status honours `target_url`.

### Relationship to the triage skill

The `pr-management-triage` skill described in `contributing-docs/25_maintainer_pr_triage.md`
applies `ready for maintainer review`, drafts and closes pull requests under maintainer
confirmation. For gated pull requests the reconciler owns draft state, that label and the
close-after-inactivity decision; the skill must treat `fork ci required` pull requests as
out of scope for those three actions. Everything else the skill does (quality gates, pings,
suspicious-change flags) is unaffected.

### The `CI passed` check

The "Require CI on main" ruleset requires a `CI passed` check from GitHub Actions before a pull
request can merge or auto-merge. Project CI's `CI passed` job reports it, but a gated pull
request never runs project CI, so the reconciler reports it instead: when the state of the head
commit is `green`, it posts a `CI passed` commit status with state `success`, linking the fork
run. A commit status written with the monitor's `GITHUB_TOKEN` comes from GitHub Actions, which
is the source the ruleset accepts.

- Only `success` is posted. Without the status a gated pull request cannot merge, which is what a
  pending or failing one would say, and it never competes with project CI's own check.
- `green_unverified` gets no `CI passed`: the fork run used CI files the pull request changed, so
  it is not evidence. Once a maintainer adds `use project ci`, project CI reports the check.
- When the head commit leaves `green`, for example a re-run in the fork that fails, a `success`
  posted earlier is overwritten with `pending` so the pull request cannot merge on it.
- A pull request released with `use project ci` keeps any `success` the reconciler posted for
  its current head commit. A maintainer who wants the project CI result before merging waits
  for project CI before enabling auto-merge, or pushes a new commit.
- In a gated run of `ci-amd.yml` every job after the gate is skipped, so project CI's `CI passed`
  job must not report success there; it reports under another name when `build-info` is
  skipped, and the reconciler's status is the only `CI passed` the pull request has.

### Far behind the base branch

A fork run tests the pushed commit as it is, while project CI tests it merged with the base
branch. When a head commit turns green, the reconciler counts the commits it is behind its base
branch (`behind_by` of `GET /repos/apache/airflow/compare/<base>...<fork-owner>:<sha>`, once per
head commit, stored in the marker). From 200 commits behind, about four days of `main`, the
state comment says so and asks the contributor to rebase, since the merged result can still
fail. The count is advisory and does not change the state.

### Unknown failures and attention items go to Slack

The reconciler is defensive per pull request: an exception while processing one pull request
is caught, recorded and the loop continues. At the end of the run every recorded item that the
reconciler could not classify, plus every attention item, is turned into one Slack message
written to `slack-message.json` with `channel: internal-airflow-ci-cd`, and the workflow
posts it with `slackapi/slack-github-action` exactly as `ci-duration-monitor.yml` does. The
output `has-alerts=true` gates the posting step. Slack rejects a whole message with a section
over 3000 characters or more than 50 blocks, so items are split across sections, each item is
shortened to 500 characters, and sections past the 50th are replaced by a pointer to the run
log, which always lists every item in full.

Attention items are expected outcomes that a maintainer should still see: a pull request
closed for gate tampering, and a pull request carrying both override labels. They are listed
in their own section of the message so they are not mistaken for reconciler failures.

Unknown means any of:

- GraphQL or REST error that is not a rate-limit wait (rate limits back off and retry).
- A check suite for `Tests (AMD)`, in the fork or in `apache/airflow`, with a status or
  conclusion outside the modelled set.
- A workflow state outside `active` / `disabled_fork` / `disabled_manually` / not found.
- A mutation that returned an error (label, draft, undraft, comment, close, status).
- A head repository that no longer exists.
- A state comment whose marker cannot be parsed.

Expected states never alert. The message lists the pull request numbers and the reason for
each, with a link to the workflow run, so a maintainer can act. The run itself still ends
green unless the reconciler could not talk to GitHub at all.

### Budget

Per run, with ~700 gated pull requests: ~28 GraphQL calls for pass 2, ~14 for fork state,
~1 for pass 1, plus one REST call per pull request in `setup_required`, a few paginated REST
calls for the recent `pull_request` runs, one REST call per base branch for its workflow
files, a jobs call and a compare call only per long `ci-amd.yml` run of a gated pull request,
one changed-files call per head commit that turns green, plus one mutation per actual state
change. Well inside
the `GITHUB_TOKEN` limits at a 15-minute cadence. The reconciler logs its call counts so the
budget stays observable.

## Monitor workflow

`.github/workflows/fork-ci-monitor.yml`:

- `on: schedule: cron '*/15 * * * *'` and `workflow_dispatch` with a `dry-run` input.
- `permissions: actions: read, contents: read, pull-requests: write, statuses: write` with a
  justification comment. Comments and labels on pull requests are covered by
  `pull-requests: write`; the commit status needs `statuses: write`; the gate tampering check
  lists runs and their jobs, which needs `actions: read` because every scope a `permissions`
  block leaves out is `none`.
- `runs-on: ubuntu-latest` (the breeze setup action frees disk space with `sudo` and `docker`,
  which `ubuntu-slim` does not provide), `timeout-minutes: 20`, `concurrency: fork-ci-monitor`
  with `cancel-in-progress: false` so runs never overlap.
- Steps: checkout `main` with `persist-credentials: false`, `./.github/actions/breeze`,
  `breeze ci fork-ci-reconcile` with `GH_TOKEN`, then the Slack post step guarded by
  `has-alerts` and skipped on a dry run, whose alerts describe changes it did not make, and a
  second Slack step that reports a run failing before it could write its message.
- Every `gh` call times out after 120 seconds, and a rate limit is retried with backoff up to
  four attempts, without waiting after the last one.

The workflow does not exit early when `enabled` is false, because both pilots rely on it.
With `enabled = false`, an empty `[pilot]` and no `use fork ci` labels, every pull request
classifies as `gate_disabled` or better, so the reconciler finds nothing to label and makes
no mutations. That makes it safe to merge before the AIP vote.

## `breeze ci audit`

Read-only by default; `--fix` applies the offered commands after confirmation. Each check
prints one row: status, finding, and the command that fixes it.

| # | Check | Passing condition | Offered fix |
|---|---|---|---|
| 1 | `gh` present and authenticated | `gh auth status` succeeds, version at least `MIN_GH_VERSION` | install / `gh auth login` |
| 2 | Remote naming | a remote named `upstream` points at `apache/airflow` and `origin` does not | `git remote rename`/`add` per `contributing-docs/10_working_with_git.rst` |
| 3 | Fork exists | `origin` is a fork of `apache/airflow` owned by the authenticated user | `gh repo fork apache/airflow --remote=false` then add `origin` |
| 4 | Who you are | classifies the user: committer (`GET /user/memberships/orgs/apache` active), collaborator (`.asf.yaml`), exempted (`[allowlist]`), pilot participant (`[pilot]`), external | informational |
| 5 | Fork workflows | external or pilot participant: `ci-amd.yml` state on the fork is `active`; committer/collaborator/exempted: state is not `active` | `gh workflow enable ci-amd.yml -R <fork>` or `gh workflow disable ...` |
| 6 | Fork `main` freshness | fork `main` is not behind `upstream/main` by more than the configured threshold | `gh repo sync <fork> --branch main` |
| 7 | Current branch | not `main`, has an upstream tracking `origin` | informational |

Committers and collaborators are told to keep fork workflows disabled because their pull
requests run on project CI; an enabled fork would run every push twice and attach a second,
confusing status. Forks of a public repository cannot be made private on GitHub, so that is
the only recommendation available.

The remote-parsing block currently inside `breeze ci upgrade` is factored into
`utils/git_remotes.py` and reused, so both commands agree on what a correct setup is.

## Documentation changes

- `contributing-docs/18_contribution_workflow.rst`: replace the single sentence "set up your
  fork and enable GitHub Actions" with the real steps and `breeze ci audit`, and describe how
  to join and leave the contributor pilot.
- `contributing-docs/05_pull_requests.rst`: rewrite "you can set up both for free on your
  fork" and the draft-conversion promise to describe the fork CI regime and the labels.
- `contributing-docs/03_contributors_quick_start.rst` and `03a_..._beginners.rst`: add the
  `upstream` remote when cloning and the audit step before the first push.
- `contributing-docs/10_working_with_git.rst`: cross-link from the naming conventions.
- `contributing-docs/25_maintainer_pr_triage.md`: the ownership split with the reconciler.
- `dev/breeze/doc/ci/05_workflows.md`: add the fork push run type.
- `dev/breeze/doc/ci/03_github_variables.md`: the new gate job outputs.
- `dev/breeze/doc/08_ci_tasks.rst`: `breeze ci audit` and `breeze ci fork-ci-reconcile`.
- `dev/breeze/doc/ci/README.md`: index this document.

## Rollout and rollback

1. Merge with `enabled = false`. Fork push trigger, fork mode selective checks, `breeze ci
   audit` and the docs are useful immediately and harmless.
2. Committer pilot: committers label their own pull requests `use fork ci` and watch the
   gate, the fork run, the reconciler, the status and the comments behave end to end on real
   traffic.
3. Contributor pilot: volunteer external contributors add themselves to `[pilot]`. This is
   the first point where the setup instructions, the comments and the waiting time are
   tried by the people they are written for. The pilot is time-boxed and its results
   (time to green, setup problems, reconciler alerts) are reported on the dev list before the
   next step.
4. Populate `[allowlist]` for the organisations agreed on the list.
5. Set `enabled_since` to a switch-on instant after the expected merge and `enabled = true`,
   and empty `[pilot]`, in one pull request.
6. Watch `internal-airflow-ci-cd` and the reconciler's run summaries for the first days.
7. Rollback is `enabled = false` (and an empty `[pilot]` during the contributor pilot).
   Existing labels and drafts stay as they are; a maintainer sweep undrafts anything still
   drafted by the bot, or labels them `use project ci` and lets the reconciler release them.
   Switching on again must move `enabled_since` forward to the new switch-on instant;
   otherwise pull requests opened while the gate was off are gated retroactively.
8. The backlog reset, if ever adopted, is `enabled_since` moved back, staged over days by
   moving it in steps.

## Security considerations

- The gate job runs with `contents: read` and reads configuration from the base branch only.
- `[pilot]` can only take project CI away from a login, so a wrong or self-added entry costs
  that contributor project CI and nothing else.
- `pull_request` runs the workflow file from the pull request's merge commit, so a pull
  request can edit `ci-amd.yml` to remove the gate or add a workflow with a `pull_request`
  trigger. The gate job cannot prevent that; `pull_request_target` was deliberately not used
  to avoid running any logic with write permissions in reaction to an untrusted event. The
  cost is bounded instead: the reconciler closes such a pull request within one cadence (see
  "Gate tampering"), and a closed pull request no longer triggers `pull_request` runs, so
  one push buys at most one run. The change is also visible in review and covered by the
  existing workflow checks.
- The tampering check trusts nothing from the pull request's tree. Its inputs are the
  workflow runs and jobs GitHub recorded, the changed paths of each run's commit and the
  workflow files on the base branch.
- A green fork run is trusted only for pull requests that do not change what fork CI runs;
  see "Trusting a green fork run".
- The reconciler runs on `main` with `pull-requests: write` and never checks out or executes
  pull request content. Its inputs are API data; its comment templates are static.
- Fork runs get the fork's `GITHUB_TOKEN` only. Nothing in project CI trusts a fork run's
  result beyond reading its public status.

## Known limitations

- Drafting, the setup comment and the commit status arrive within one cadence of the event
  they reflect, not instantly. The cadence can be lowered to 5 minutes without changing the
  budget analysis, since unchanged pull requests cost no mutations.
- The workflows API lists workflows from the fork's default branch. A fork whose `main`
  predates `ci-amd.yml` reports it missing; the setup comment covers this with
  `gh repo sync`.
- A contributor whose GitHub account concurrency was raised by GitHub Support is not
  distinguished from anyone else; the AIP treats that as acceptable friction.
- `enabled_since` compares against pull request creation, so a pull request opened before the
  switch and pushed after keeps project CI. This is intentional.
- Gate tampering detection is after the fact: the first push that removes the gate gets one
  project CI run before the reconciler closes the pull request. A tampered run is only seen
  while it is in the 6-hour window, so a reconciler outage longer than that can miss one. A
  tampered run that does its work in under 5 minutes is not inspected; that buys little. It
  is also scoped to the
  gate: an edit to one of the workflows that already run for every pull request (CodeQL,
  dependency review, the newsfragment check) that makes it do more work is not distinguished
  from a legitimate fix to that workflow and is left to review.
- An external pull request that legitimately changes workflow files, was released with `use
  project ci` and then had that label removed is closed on the next run, because both
  signals hold. Keeping `use project ci` on such a pull request is the intended handling.

## Testing

- `dev/breeze/tests/test_fork_ci_gate.py`: one parametrized case per eligibility rule,
  including `enabled_since` boundaries, the push-to-apache branch patterns, both override
  labels with `enabled` true and false, `[pilot]` with `enabled` true and false, at the
  join-instant boundary, with a differently-cased login and overridden by `use project ci`,
  and `schedule` in a fork versus in `apache/airflow`.
- `scripts/tests/`: the `check-fork-ci-gate-config` hook accepts a future `[pilot]` instant
  and `enabled_since`, rejects a past one, and leaves unchanged entries alone.
- `dev/breeze/tests/test_fork_ci_reconcile.py`: one parametrized case per row of the state
  table, plus the human-draft rule, the label escape hatch, the `use project ci` release,
  the conflicting-labels alert, the close clock reset (including a reopen after an inactivity
  close), a fork with a name other than `airflow`, marker round-trips, status
  desired-vs-current comparison, the unknown-failure collection, green trust (a change to
  CI-defining files keeps the pull request a draft, decided once per commit) and the gate
  tampering check: a tampered run on an earlier commit is caught after a clean push, each
  signal alone does not close, short runs and gate-only jobs do not count, an in-progress
  non-gate job, a removed, duplicated or long-running gate job and an injected workflow do,
  runs before the last `use project ci` removal or the last reopen are ignored, an
  uncheckable 300-file change set is reported, `label_fork_ci` pull requests are exempt and
  `pilot` pull requests are checked. Discovery labels a pilot participant's
  new pull request with `enabled = false` and leaves their older ones alone. GraphQL and REST
  are mocked with `autospec`; no network.
- `dev/breeze/tests/test_selective_checks.py`: fork mode cases (changed files from compare,
  300-file fallback, committer build forced false, full-matrix not forced).
- `dev/breeze/tests/test_ci_audit.py`: each check with mocked `gh` and `git remote -v`
  output, and `--fix` command generation.
- The workflow YAML is exercised by the existing `check-ci-workflows-in-sync` hook, zizmor
  and the ASF allowlist check.
