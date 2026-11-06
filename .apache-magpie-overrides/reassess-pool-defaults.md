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

- [Apache Airflow — reassessment pool defaults](#apache-airflow--reassessment-pool-defaults)
  - [Pool: `open-eol`](#pool-open-eol)
  - [Pool: `reopened`](#pool-reopened)
  - [Pool: `stale-unresolved`](#pool-stale-unresolved)
  - [Pool: project-specific](#pool-project-specific)
  - [Pool-selection guidance](#pool-selection-guidance)
  - [Cross-references](#cross-references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — reassessment pool defaults

Named issue pools for [`issue-reassess`](../../skills/issue-reassess/SKILL.md)
sweep campaigns. Each pool is a query against `<issue-tracker>` that
surfaces a particular kind of candidate.

Express each pool as a query the tracker accepts (JQL for JIRA,
`gh search` for GitHub Issues, etc.). The skill picks one pool per
sweep; the user can override per-invocation.

This file extends the default-triage-pool query in
[`issue-tracker-config.md`](issue-tracker-config.md) with named
named-and-rationalised pools tuned to specific reassessment goals.

Airflow is on GitHub Issues, so every pool below is GitHub issue-search
syntax, usable as `gh search issues "<query>"` or
`gh api -X GET search/issues -f q='<query>'`. GitHub search has no
relative dates: `<cutoff-12m>` / `<cutoff-90d>` stand for the ISO date
12 months / 90 days before the sweep (e.g. `2025-09-26`); the skill
substitutes them. Sweeps skip issues labelled `pinned` or `security`,
and never reassess a vulnerability report on the public tracker (those go
to `security@airflow.apache.org`).

## Pool: `open-eol`

Open issues whose `fixVersion` is end-of-life. Often contains:

- Long-fixed-but-never-closed issues (silent fixes).
- Wishlists that the team has resisted.
- Real bugs that fell through the cracks at end-of-life.

Airflow has no `fixVersion`; the equivalent is the `affected_version:<X.Y>`
label (latest version the bug reproduces on, per
`ISSUE_TRIAGE_PROCESS.rst`). When a release line goes end-of-life its
labels are renamed with an `_eol_` prefix — all of Airflow 2.x
(EOL 2026-04-22) is now `_eol_affected_version:2.1` …
`_eol_affected_version:2.11`. Reassess each against `main`
(`breeze shell --use-airflow-version` for the old version) and ask the
reporter to reconfirm on Airflow 3 if it still reproduces.

```text
repo:apache/airflow is:issue is:open label:"_eol_affected_version:2.1","_eol_affected_version:2.2","_eol_affected_version:2.3","_eol_affected_version:2.4","_eol_affected_version:2.5","_eol_affected_version:2.6","_eol_affected_version:2.7","_eol_affected_version:2.8","_eol_affected_version:2.9","_eol_affected_version:2.10","_eol_affected_version:2.11" -label:pinned sort:updated-asc
```

When a further release line goes EOL, add its renamed
`_eol_affected_version:<X.Y>` labels to the comma-separated (OR) list.

JIRA example:
```text
project = <KEY> AND resolution = Unresolved AND
  fixVersion in releasedVersions() AND
  fixVersion was in unreleasedVersions() AND status = Open
```

## Pool: `reopened`

Issues that were closed and later reopened. Surfaces:

- Persistent wishlists the team keeps resisting (often classified
  `feature-request-disguised-as-bug` per the nature taxonomy in
  [`issue-reproducer/verdict-composition.md`](../../skills/issue-reproducer/verdict-composition.md)).
- True regressions where a fix was reverted or didn't stick.

GitHub records `state_reason: reopened` on such issues, and issue search
exposes it as `reason:reopened`:

```text
repo:apache/airflow is:issue is:open reason:reopened -label:pinned sort:updated-asc
```

JIRA example:
```text
project = <KEY> AND status changed FROM "Closed" TO "Open"
```

## Pool: `stale-unresolved`

Open issues with no activity in the last 12 months. Useful for
periodic hygiene sweeps to confirm-or-close.

Inactive `kind:bug` issues are already handled by the
`.github/workflows/recheck-old-bug-report.yml` bot: after 365 days without
activity it labels them `Stale Bug Report`, asks the author to recheck on
the latest version, and closes them 30 days later. This pool therefore
excludes issues the bot has already flagged, and mainly surfaces
`kind:feature` / `kind:task` / `kind:documentation` issues the bot never
touches:

```text
repo:apache/airflow is:issue is:open updated:<<cutoff-12m> -label:"Stale Bug Report" -label:pinned -label:security sort:updated-asc
```

JIRA example:
```text
project = <KEY> AND resolution = Unresolved AND
  updated < -52w
```

## Pool: project-specific

Adopters can add pools tuned to their specific concerns — e.g.,
issues lacking a component label, issues filed before a specific
major release, issues with specific keyword overlap.

Airflow pools:

- **`triage-backlog`** — issues still carrying `needs-triage` 90+ days
  after filing. `ISSUE_TRIAGE_PROCESS.rst`: while `needs-triage` remains,
  the triage team checks periodically whether the issue should be
  accepted, closed, or converted to a GitHub Discussion (Ideas / Q&A).

  ```text
  repo:apache/airflow is:issue is:open label:needs-triage created:<<cutoff-90d> sort:created-asc
  ```

- **`stale-bug-report`** — bugs the recheck bot has flagged and will
  close within 30 days. Reassessing them before closure separates
  silently fixed bugs (close as fixed, with evidence) from still-live
  ones (remove the label; the bot re-adds `needs-triage` on activity).

  ```text
  repo:apache/airflow is:issue is:open label:"Stale Bug Report" sort:updated-asc
  ```

- **`accepted-bugs-quiet`** — accepted bug reports gone quiet; the
  reassess default already declared in
  [`issue-tracker-config.md`](issue-tracker-config.md) (the triager
  removed `needs-triage`, so nobody is watching them).

  ```text
  repo:apache/airflow is:issue is:open label:kind:bug -label:needs-triage -label:"Stale Bug Report" updated:<<cutoff-90d> sort:updated-asc
  ```

`pending-response` issues are deliberately **not** a pool: the
`.github/workflows/stale.yml` bot already marks them stale after 14 days
without an author response and closes them 7 days later.

## Pool-selection guidance

The skill picks one pool per sweep. Hints for picking:

- **First-ever sweep** of an existing project: start with
  `open-eol` (highest density of silent fixes; fastest to clear).
- **Periodic hygiene** sweeps: rotate through pools each quarter.
- **Pre-release** check sweeps: `stale-unresolved` and any
  release-version-specific pool.
- **Specific concern** (e.g., complaint about wishlist accumulation):
  `reopened`.

For Airflow specifically: `open-eol` is small (about 25 open issues on
2026-09-26) and every hit is a 2.x-era report, so it is the natural first
sweep. For a pre-release sweep of an Airflow 3 minor, use
`label:"affected_version:<X.Y>"` for the release line being superseded.
Run `stale-bug-report` before the recheck bot's 30-day close window
expires so fixed bugs close with evidence rather than as "no response".

## Cross-references

- [`issue-tracker-config.md`](issue-tracker-config.md) — the
  default-triage pool (distinct from these reassess pools).
- [`reproducer-conventions.md`](reproducer-conventions.md) —
  evidence layout for each issue in a sweep.
