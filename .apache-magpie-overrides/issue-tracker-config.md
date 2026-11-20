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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — issue-tracker configuration](#apache-airflow--issue-tracker-configuration)
  - [URL and project key](#url-and-project-key)
  - [Authentication](#authentication)
  - [Default query templates](#default-query-templates)
  - [Tracker-specific notes](#tracker-specific-notes)
  - [Cross-references](#cross-references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — issue-tracker configuration

The project's **general-issue tracker** configuration — where issues
live, how to authenticate, and how to query. Consumed by the
`issue-*` skill family (`issue-triage`, `issue-reassess`,
`issue-reproducer`, `issue-fix-workflow`).

This file is distinct from the `tracker_repo` field in
[`project.md`](project.md), which declares the **security** tracker
used by the `security-issue-*` skill family. Many projects use
different trackers for the two: e.g., a private GitHub repo for
security and a public JIRA project for general issues. Adopters
that use the same tracker for both can point both at the same
location.

## URL and project key

| Key | Value |
|---|---|
| `url` | `https://github.com/apache/airflow` |
| `project_key` | `apache/airflow` |
| `tracker_type` | `github-issues` |
| `issue_url_template` | `https://github.com/apache/airflow/issues/<N>` |

Skills resolve `<issue-tracker>` to `url` and `<issue-tracker-project>`
to `project_key`.

## Authentication

Public GitHub repository: reads work anonymously; writes use the maintainer's
`gh` CLI login.

- **Anonymous read** — true if the tracker permits unauthenticated
  browsing (many JIRA instances do). Set `anonymous_read: true` if
  so; skills can do the classification phase without credentials.
- **Authenticated write** — credentials needed to post comments,
  link issues, or apply any mutation. Document where credentials
  come from:
  - JIRA: API token in `~/.config/<tracker>-token` or an env var
  - GitHub Issues: `gh` CLI auth status
  - Other: project-specific

| Key | Value |
|---|---|
| `anonymous_read` | `true` |
| `auth_method` | `gh-cli` |
| `auth_env_var` | *(none — `gh` reads its token from the OS keyring; `GH_TOKEN` only if set explicitly)* |

## Default query templates

The project's canonical queries for the triage / reassess
pools, derived from `ISSUE_TRIAGE_PROCESS.rst` and `.github/workflows/stale.yml`. Skills use these as defaults; users can override per-invocation.

For JIRA-based projects, queries are JQL (not used — Airflow is on GitHub Issues):

```text
# n/a: triage pool — newly-filed, unsorted issues
project = <project_key> AND resolution = Unresolved AND status = Open

# n/a: reassess pool — silent wishlists and EOL issues
project = <project_key> AND resolution = Unresolved AND
  fixVersion in unreleasedVersions() AND status = Open

# n/a: reopened pool — issues that were closed and reopened
project = <project_key> AND status changed FROM "Closed" TO "Open"
```

For GitHub-Issues-based projects, queries are `gh search issues`
syntax:

```text
# triage pool — issue templates auto-apply `needs-triage`; the triager removes
# it once the issue is accepted and has kind:* / area:* labels
is:open is:issue label:needs-triage repo:apache/airflow

# reassess pool — accepted bug reports gone quiet (ISSUE_TRIAGE_PROCESS.rst
# "Stale Policy": ask the author to recheck on the latest version)
is:open is:issue label:kind:bug -label:needs-triage sort:updated-asc repo:apache/airflow

# awaiting-author pool — stale bot marks these stale after 14 days, closes 7 days later
is:open is:issue label:pending-response repo:apache/airflow
```

Adopters who use other trackers (Bugzilla, GitLab, custom) substitute
the appropriate query language.

## Tracker-specific notes

Airflow-specific quirks:

- **Label taxonomy** (`ISSUE_TRIAGE_PROCESS.rst`) — `kind:*` (bug, feature,
  documentation, task, meta), `area:*` (`area:core`, `area:providers`,
  `area:helm-chart`, finer `area:scheduler`, `area:UI`, …), `provider:<name>`
  for provider issues, `affected_version:<X.Y>` on core bugs only (latest
  reproducing version), `good first issue`, `pending-response`,
  `needs-triage`. Invalid issues get `duplicate`, `Can't Reproduce`,
  `invalid` or `won't fix` (the doc says `wontfix`; the GitHub label is `won't fix`).
- **Discussions** — vague ideas go to GitHub Discussions (Ideas); support
  requests to Discussions (Q&A), with a comment explaining why.
- **Assignee timeliness** — no activity for 2 weeks → remind the assignee;
  1 more week → unassign.
- **Security** — never triage vulnerability reports on public issues; they go
  to `security@airflow.apache.org` per the security policy.

Generic notes:

- **Rate limits** — most public trackers throttle. JIRA Cloud's free
  tier is 1500 requests / 5 minutes; GitHub's API is 5000 / hour
  authenticated.
- **Anon vs auth differences** — if anonymous queries return fewer
  fields than authenticated ones (e.g., JIRA's `worklog`), skills
  must know to escalate.
- **Custom fields** — JIRA projects often define custom fields
  (`customfield_NNNNN`). Document any the skills need to read.
- **Project board / kanban integration** — if the tracker has a
  separate "board" view with workflow states, document where it is
  and whether the skills should reconcile against it.

## Cross-references

- [`project.md`](project.md) — the manifest; declares
  `upstream_default_branch` and the security `tracker_repo` (distinct
  from this file's general-issue tracker).
- [`reassess-pool-defaults.md`](reassess-pool-defaults.md) — pool
  definitions consumed by `issue-reassess`, extending the default
  queries above.
- [`runtime-invocation.md`](runtime-invocation.md) — how `issue-reproducer`
  runs the extracted code.
