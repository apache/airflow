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

- [Apache Airflow — release trains, release managers, security team roster](#apache-airflow--release-trains-release-managers-security-team-roster)
  - [Release branches currently in flight](#release-branches-currently-in-flight)
  - [Current release managers](#current-release-managers)
  - [Known release-manager rotations](#known-release-manager-rotations)
  - [Release managers for releases currently relevant to the security tracker](#release-managers-for-releases-currently-relevant-to-the-security-tracker)
  - [Security team roster](#security-team-roster)
  - [What this means for sync and fix skills](#what-this-means-for-sync-and-fix-skills)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — release trains, release managers, security team roster

Fast-moving project state. Update every time a release ships, a new
release branch opens, or a security-team member joins / rotates off.

## Release branches currently in flight

Snapshot as of 2026-09-26, derived from the `upstream` branch list, the
open GitHub milestones, the `backport-to-*` labels and `.github/boring-cyborg.yml`.
Airflow ships several independent trains; each `dev/README_RELEASE_*.md` is the
source of truth for its process.

- **`main`** — Airflow core `3.4.0` (milestone `Airflow 3.4.0`); Task SDK and
  the Python client follow the core version line. Providers are released from
  `main` in date-identified waves (`providers/<YYYY-MM-DD>` tags; latest
  `providers/2026-09-22`). Helm Chart `2.0.0` (milestone `Airflow Helm Chart
  2.0.0`) is developed on `main` (see `dev/README_HELM_CHART2_DEV.md`).
  Also carries the Go / Java / TS SDK milestones (`Go SDK 1.0 GA`,
  `Java SDK 1.0 GA`, `TS SDK 1.0 Beta`).
- **`v3-3-test`** (cut to `v3-3-stable`) — patch branch for Airflow core
  `3.3.x` (with Task SDK `1.3.x`, Python client `3.3.x`). Last release `3.3.2`
  (2026-09-17). Next patch is `3.3.3` (milestone `Airflow 3.3.3`).
  **Default target for new core security fixes** (backport label
  `backport-to-v3-3-test`).
- **`chart/v1-2x-test`** — Helm Chart `1.2x.x` maintenance line (bug fixes,
  doc fixes and deprecation warnings only). Last release `helm-chart/1.22.0`
  (2026-06-04). Next is `1.23.0` (milestone `Airflow Helm Chart 1.23.0`).
  Where a chart fix targets depends on where the bug exists (see
  `dev/README_HELM_CHART2_DEV.md`); fixes on `main` that also apply to 1.2x
  get the `backport-to-chart/v1-2x-test` label.
- **`airflow-ctl/v0-1-test`** (cut to `airflow-ctl/v0-1-stable`) — airflowctl
  `0.1.x` line; releases are never cut from `main`. Last release
  `airflow-ctl/0.1.5` (2026-06-03). Backport label
  `backport-to-airflow-ctl/v0-1-test`.
- **`providers-fab/v1-5`** — FAB provider `1.5.x` maintenance branch
  (backport label `backport-to-providers-fab/v1-5`). All other providers
  release from `main` only; provider security fixes target `main`.
- **`v3-2-test` and older `vX-Y-test`** — no open milestone and no active
  `backport-to-*` label; treated as having no further releases planned.
  `v2-11-test` is explicitly EOL (label `_eol_backport-to-v2-11-test`).

## Current release managers

Two sources identify the release manager for a given cut:

1. The [Release Plan](https://cwiki.apache.org/confluence/display/AIRFLOW/Release+Plan)
   wiki page (release schedule for core, providers,
   Helm chart and airflowctl; `dev/verify_release_calendar.py` cross-checks it
   against the public release calendar).
2. The `[RESULT][VOTE]` thread on `dev@airflow.apache.org`
   (archive: <https://lists.apache.org/list.html?dev@airflow.apache.org>).
   The sender of the `[RESULT][VOTE] …` message **is** the release
   manager for that specific cut.

## Known release-manager rotations

Rotations are published on the
[Release Plan](https://cwiki.apache.org/confluence/display/AIRFLOW/Release+Plan)
wiki page (providers waves, core, Helm chart, airflowctl).

TODO: record the current per-train rotation roster (names + GitHub handles).

## Release managers for releases currently relevant to the security tracker

TODO: for each recently-shipped or upcoming release carrying security
fixes, record:

- the release name + date;
- the release manager (with email + GitHub handle);
- the source of that attribution (archive URL to the `[RESULT][VOTE]`
  thread);
- which CVEs shipped in it.

When this list becomes stale, the sync skill will surface it as a
blocker.

## Security team roster

TODO: name the private security tracker repository (not declared in
`.apache-magpie-overrides/project.md`). The **authoritative** source is the collaborator list of the
tracker repository — anyone listed as a collaborator, regardless of
permission level, is on the security team.

```bash
gh api repos/<tracker>/collaborators --jq '.[].login'
```

Snapshot (update in the same change as member joins / rotates):

> TODO: list of GitHub handles.

## What this means for sync and fix skills

Explicit defaults for the generic skills:

- Default milestone for a new patch-train security issue: `Airflow 3.3.3`
  (core). Helm chart: `Airflow Helm Chart 1.23.0`. Providers have no
  milestones — fixes ship in the next providers wave from `main`.
- Default backport labels: `backport-to-v3-3-test` (core / Task SDK),
  `backport-to-chart/v1-2x-test` (Helm chart 1.2x),
  `backport-to-airflow-ctl/v0-1-test` (airflowctl),
  `backport-to-providers-fab/v1-5` (FAB provider 1.5.x only). The
  `automatic-backport.yml` workflow strips the `backport-to-` prefix to get
  the target branch, so use the slash form; `backport-to-airflow-ctl-v0-1-test`
  (hyphen form) does not map to a branch.
- Legacy / do-not-use: `v3-2-test` and older core branches;
  `_eol_backport-to-v2-11-test` marks the retired 2.11 line.
- TODO: any other sync-surfaced blockers specific to this project.
