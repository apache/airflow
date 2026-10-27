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

- [Apache Airflow — pr-management-triage configuration](#apache-airflow--pr-management-triage-configuration)
  - [Identifiers](#identifiers)
  - [Project-specific labels](#project-specific-labels)
  - [Open PR limit](#open-pr-limit)
  - [Grace windows](#grace-windows)
  - [Feedback delivery](#feedback-delivery)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage configuration

This file is the **per-project configuration** for the
[`pr-management-triage`](../../.claude/skills/pr-management-triage/SKILL.md) skill.
It contains the concrete values for the Apache Airflow project.
New adopters should copy this file into their own
`<project-config>/pr-management-config.md` and replace every
Airflow-specific value with their project's equivalents.

## Identifiers

| Key | Value | Used by |
|---|---|---|
| `committers_team` | `apache/airflow-committers` | `classify-and-act.md` row F5b — team-mention detection. Used to recognise PR comments that `@`-mention the project's committers as a maintainer-to-maintainer ping. |
| `area_label_prefix` | `area:` | `classify-and-act.md`, `pr-management-stats` — area-label grouping. |

## Project-specific labels

Labels the skill applies or watches for. Each row maps a generic
**framework concept** to whatever label string the adopter uses.
If the project doesn't have a given concept, leave the value blank
and the skill will skip that row of decision-table actions.

| Concept | Label | Notes |
|---|---|---|
| `ready_for_maintainer_review` | `ready for maintainer review` | Applied by the `mark-ready` action; used by `pr-management-code-review` as a default selector. |
| `quality_violations_close` | `closed because of multiple quality violations` | Applied when a PR is closed for failing the project's PR quality criteria after multiple opportunities to fix. |
| `suspicious_changes` | `suspicious changes detected` | Applied to first-time-contributor workflow approvals where the diff looks suspicious (binary blobs, unrelated CI changes, etc.). |
| `work_in_progress` | | Airflow does not use a dedicated WIP label; the skill relies on draft status instead. |
| `open_pr_limit_close` | `closed because of open PR limit` | Airflow-specific. Applied only by `dev/close_prs_over_open_pr_limit.py` during the one-time closure that introduced the [open PR limit](#open-pr-limit) — the triage skill never applies it. A PR carrying it that the author has reopened is a deliberate prioritisation choice: triage it like any other PR and do not treat the label as a quality signal. |

## Open PR limit

`.asf.yaml` enables GitHub's pull request creation cap: an author
without write access can have at most **5** open PRs at a time.
Drafts do not count yet (GitHub does not support counting them).
The contributor-facing explanation is
[`32_open_pull_request_limit.rst`](https://github.com/apache/airflow/blob/main/contributing-docs/32_open_pull_request_limit.rst).

| Key | Value | Notes |
|---|---|---|
| `max_open_prs_without_write_access` | `5` | Keep in sync with `github.pull_requests.creation_cap.max_open_pull_requests` in `.asf.yaml`. |
| `open_pr_limit_url` | `https://github.com/apache/airflow/blob/main/contributing-docs/32_open_pull_request_limit.rst` | Link it from any triage comment that asks an author to focus on fewer PRs (e.g. the `Multiple flagged PRs` violation). |

Triage implications:

- Closing a PR or converting it to draft frees one of the author's
  slots. Mention that in `close` comments so the author knows the
  PR can be reopened once they have capacity.
- Do not propose closing PRs **only** because an author is at or
  above the limit — GitHub enforces the cap for new PRs, and the
  one-time closure of pre-existing PRs is done with the script, not
  the triage skill.

## Grace windows

Tunable thresholds. These values were calibrated for Airflow's
contributor traffic (~50–100 open PRs, triage sweep every 1–2
days).

| Concept | Default | Project value |
|---|---|---|
| Stale-draft close threshold (triaged) | 7 days | 7 days |
| Stale-draft close threshold (untriaged) | 14 days | 14 days |
| Inactive-open → draft threshold | 28 days | 28 days |
| Stale-review-ping cooldown | 7 days | 7 days |
| Stale-workflow-approval threshold | 28 days | 28 days |
| Stale-Copilot-review threshold | 7 days | 7 days |

## Feedback delivery

| Key | Value | Notes |
|---|---|---|
| `triage_feedback_channel` | `pr-body` | Deterministic quality-violation feedback for the `draft`, `comment` (deterministic-flag), and `close` actions is **folded into the PR description** as a managed marker block rather than posted as a comment. Editing a PR body does not notify subscribers, so the maintainer mailbox stays quiet (the [denoise rationale](../../skills/pr-management-triage/rationale.md#why-fold-feedback-into-the-pr-body-denoise)). Pings, `request-author-confirmation`, security-language, suspicious-changes, and stale-sweep messages always post a comment regardless — their purpose *is* to notify a human. Set to `comment` to revert to the legacy notifying-comment behaviour for all violation feedback. |
