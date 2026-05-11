<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — pr-management-triage configuration](#apache-airflow--pr-management-triage-configuration)
  - [Identifiers](#identifiers)
  - [Project-specific labels](#project-specific-labels)
  - [Grace windows](#grace-windows)

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
