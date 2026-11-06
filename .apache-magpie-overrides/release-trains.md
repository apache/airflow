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

Snapshot as of 2026-09-26, from the Release Plan wiki page (read 2026-09-26)
cross-checked against the `[RESULT][VOTE]` senders on `dev@airflow.apache.org`
since 2026-03. GitHub handles are from `.github/CODEOWNERS`.

- **Airflow core (3.x), Task SDK, Python client** — Rahul Vats
  (`vatsrahul1001`), supported by Kaxil Naik (`kaxil`). Rahul sent every core,
  Task SDK and Python client `[RESULT][VOTE]` from 3.1.8 through 3.3.2.
  Planned next: `3.4.0` (feature freeze 2026-10-05, RC 2026-10-19, release
  2026-10-26). `3.3.3` is not yet on the wiki schedule.
- **Providers waves** — rotation Hussein Awala (`hussein-awala`), Jarek
  Potiuk (`potiuk`), Vincent Beck (`vincbeck`), Shahar Epstein (`shahar1`),
  Niko Oliveira (`o-nikolas`). Upcoming cuts per the wiki: 2026-09-22 Shahar
  Epstein (no `[RESULT]` yet), 2026-10-06 Jarek Potiuk, 2026-10-20 Hussein
  Awala, 2026-11-03 Niko Oliveira. Jens Scheffler (`jscheffl`) also ran
  waves in 2026-03 and 2026-05, but is not in the current rotation.
- **Helm chart** — rotation Jed Cunningham (`jedcunningham`), Jens Scheffler
  (`jscheffl`), Buğra Öztürk (`bugraoz93`), Jarek Potiuk (`potiuk`). Recent
  RMs: 1.20.0 Jens Scheffler, 1.21.0 Buğra Öztürk, 1.22.0 Jarek Potiuk. The
  wiki slots for weeks of 2026-06-15 (Jed Cunningham, "either 2.0 or another
  1.23.0"), 2026-07-20 and 2026-08-17 (both Buğra Öztürk) have no `[RESULT]`
  thread yet; `1.23.0` is still open.
- **airflowctl** — rotation Buğra Öztürk (`bugraoz93`), Jarek Potiuk
  (`potiuk`). Recent RMs: 0.1.3 and 0.1.5 Buğra Öztürk, 0.1.4 Jarek Potiuk.
  The wiki lists Buğra Öztürk for a possible `1.0.0` (week of 2026-06-22),
  which has not been voted on.
- **Java SDK** — Tzu-ping Chung (`uranusjr`); `1.0.0-beta1` released
  2026-07-13.
- **Airflow 2** — Jarek Potiuk (`potiuk`). `2.11.2` was voted 2026-03-14; the
  wiki marks the week of 2026-04-20 as the last Airflow 2 release.
- **apache-airflow-mypy** — Hussein Awala (`hussein-awala`). `0.1.0` was
  released 2026-06-12; later releases are on demand.

## Release managers for releases currently relevant to the security tracker

Snapshot as of 2026-09-26, covering releases since 2026-03. The RM is the
sender of the `[RESULT][VOTE]` thread linked below. Where that thread was sent
from a non-ASF address, the email listed is the RM's `@apache.org` address
from their own public CVE advisories on `users@airflow.apache.org`. CVE ids
and fixed versions come from those advisories
(<https://lists.apache.org/list.html?users@airflow.apache.org>). Helm chart
and airflowctl releases in this window carried no published CVEs.

**Airflow core (+ Task SDK).** RM for all of these: Rahul Vats,
`rahulvats@apache.org`, `vatsrahul1001`.

- **3.3.2** (voted 2026-09-17), <https://lists.apache.org/thread/8g8tgsk028gxfh1r741dzv63fkm0mbvh>:
  CVE-2026-75157, CVE-2026-82355, CVE-2026-86473, CVE-2026-75158.
- **3.3.1** (voted 2026-08-12), <https://lists.apache.org/thread/nqt073s4yg6cg8rjm15hxvlsm2xk9c8d>:
  CVE-2026-58076, CVE-2026-59244, CVE-2026-59242, CVE-2026-54183,
  CVE-2026-67260, CVE-2026-67587, CVE-2026-65017, CVE-2026-68968,
  CVE-2026-68969, CVE-2026-68970, CVE-2026-68971, CVE-2026-68076.
- **3.3.0** (voted 2026-07-06), <https://lists.apache.org/thread/d1bs8o1r98xwwcornpnfj62dntzg4gwn>:
  CVE-2026-33264, CVE-2026-49487, CVE-2026-48828, CVE-2026-49296,
  CVE-2026-48891, CVE-2026-48892.
- **3.2.2** (voted 2026-05-29), <https://lists.apache.org/thread/nloffbvgotm1bpvv7s46060wyytfyoky>:
  CVE-2026-40861, CVE-2026-40961, CVE-2026-40963, CVE-2026-41014,
  CVE-2026-49267, CVE-2026-41017, CVE-2026-41084, CVE-2026-42252,
  CVE-2026-42360, CVE-2026-42358, CVE-2026-42359, CVE-2026-45360,
  CVE-2026-45426, CVE-2026-46764, CVE-2026-48726, CVE-2026-49298,
  CVE-2026-45192.
- **3.2.1** (voted 2026-04-22), <https://lists.apache.org/thread/x1zs0wkbd98ckwnnnghnxstzo9fov5qx>:
  CVE-2026-38743, CVE-2026-40690.
- **3.2.0** (voted 2026-04-07), <https://lists.apache.org/thread/rorz8rq9dn3myvngotnk2xs1mjngvw2m>:
  CVE-2026-34538, CVE-2025-57735, CVE-2025-66236, CVE-2026-33858,
  CVE-2026-31987, CVE-2026-30912, CVE-2026-32690, CVE-2026-32228,
  CVE-2026-25917, CVE-2026-25219. It also covers two documentation-only
  advisories about unsafe examples: CVE-2025-54550 and CVE-2026-30898.
- **3.1.8** (voted 2026-03-11), <https://lists.apache.org/thread/k2pmyynlzz7jznw8fxkcqqb3mcnqo4mh>:
  CVE-2026-30911, CVE-2026-28779, CVE-2026-26929, CVE-2026-28563.
- **Upcoming:** `3.3.3` and `3.4.0` (planned for 2026-10-26), with Rahul Vats
  as RM per the wiki.

**Providers waves.** Each wave is paired with the advisories that its RM
announced right after it. The advisory names the fixed provider version, but
does not name the wave.

- **2026-09-09**, Vincent Beck, `vincbeck@apache.org`, `vincbeck`,
  <https://lists.apache.org/thread/1cby0k2cd5c6m96v5nmcy3vtd5dzbbnc>:
  fab 3.9.0 (CVE-2026-86466, CVE-2026-82310, CVE-2026-86462,
  CVE-2026-82311), keycloak 0.10.0 (CVE-2026-76187, CVE-2026-76186),
  apache-kafka 2.0.0 (CVE-2026-86792), akeyless 0.3.1 (CVE-2026-86465).
- **2026-08-25**, Niko Oliveira, `onikolas@apache.org`, `o-nikolas`,
  <https://lists.apache.org/thread/690qgkdrpg9x0ctmxo13dfpzqv0v1zy5>:
  fab 3.8.1 (CVE-2026-75156).
- **2026-08-06 / 2026-08-08**, Jarek Potiuk, `potiuk@apache.org`, `potiuk`,
  <https://lists.apache.org/thread/j44vmoh7z9b077zvn7vhk2fooycdmmpr>,
  <https://lists.apache.org/thread/r23ry6mjjs36o4s5y0zqqqqw8r6pd5qr>:
  amazon 9.34.0 (CVE-2026-68872), yandex 4.5.1 (CVE-2026-68871),
  microsoft-azure 14.1.0 (CVE-2026-68870), google 22.3.0 (CVE-2026-68868).
- **2026-07-22**, Shahar Epstein, `shahar@apache.org`, `shahar1`,
  <https://lists.apache.org/thread/zx2z4ddbs4mg0w9wmbb5yo727qhgsro8>:
  fab 3.7.3 (CVE-2026-59243).
- **2026-07-06**, Vincent Beck, `vincbeck@apache.org`, `vincbeck`,
  <https://lists.apache.org/thread/4m1jqkb6r4lq0qq8j2lppp0z1yq0xwsb>:
  git 0.4.1 (CVE-2026-58065), fab 3.7.2 (CVE-2026-59245).
- **2026-06-26 / 2026-07-01**, Shahar Epstein, `shahar@apache.org`, `shahar1`,
  <https://lists.apache.org/thread/1p87439ns9f14fbh75d3spl3dlhn5xrn>,
  <https://lists.apache.org/thread/q6pbq23t260fs3tkcok0vr1pbosg9wy3>:
  google 22.2.1 (CVE-2026-49297).
- **2026-06-16**, Shahar Epstein, `shahar@apache.org`, `shahar1`,
  <https://lists.apache.org/thread/xjz945gp3ltlxckkwol7mjsvsf4g1c3m>:
  ftp 3.15.1 (CVE-2026-49486).
- **2026-06-02 / 2026-06-08**, Jarek Potiuk, `potiuk@apache.org`, `potiuk`,
  <https://lists.apache.org/thread/zds5qxglnft3gv1qdzt8thbr6j3b26c7>,
  <https://lists.apache.org/thread/wbzxbyc0vqlc3rzrovy5nlqcy8gkr9cs>:
  samba 4.12.6 (CVE-2026-49818), sftp 5.8.1 (CVE-2026-50203).
- **2026-05-19**, Jens Scheffler, `jscheffl@apache.org`, `jscheffl`,
  <https://lists.apache.org/thread/2q2t3l5lxvsy01fbjl4bh5g0tcfhbn0f>:
  google 22.0.0 (CVE-2026-45361), fab 3.6.4 (CVE-2026-46745).
- **2026-05-05**, Vincent Beck, `vincbeck@apache.org`, `vincbeck`,
  <https://lists.apache.org/thread/yqo5xszpkflwk5ok16cp5mtzsmywhljz>:
  cncf-kubernetes 10.17.0 (CVE-2026-27173), amazon 9.28.0 (CVE-2026-42526).
- **2026-04-26**, Shahar Epstein, `shahar@apache.org`, `shahar1`,
  <https://lists.apache.org/thread/sxjpbp9gdnokomjlx8nqosh7l91c7tsn>:
  opensearch 1.9.1 (CVE-2026-43826), elasticsearch 6.5.3 (CVE-2026-41018).
- **2026-04-21**, Shahar Epstein, `shahar@apache.org`, `shahar1`,
  <https://lists.apache.org/thread/rmcdvbl3qm2gob3xdbv4sqbtgk87z8sq>:
  smtp 3.0.0 (CVE-2026-41016).
- **2026-04-08 / 2026-04-12**, Jarek Potiuk, `potiuk@apache.org`, `potiuk`,
  <https://lists.apache.org/thread/l8jn2zj1kw352kfjr3lyvjjr891opdfy>,
  <https://lists.apache.org/thread/t031q7379j0q0yqfk6jq82mz7qvj65ml>:
  keycloak 0.7.0 (CVE-2026-40948).
- **2026-03-24**, Jens Scheffler, `jscheffl@apache.org`, `jscheffl`,
  <https://lists.apache.org/thread/czl9jcy5nhd5kxzwlch6vkqwtyt9cqzh>:
  databricks, fixed version per the advisory (CVE-2026-32794).
- **2026-02-26 / 2026-03-03**, Jarek Potiuk, `potiuk@apache.org`, `potiuk`,
  <https://lists.apache.org/thread/8dj1yl3jgccfb6f4zf46qgvhlbs71drw>,
  <https://lists.apache.org/thread/pm3qwh6zz4x0m7r2vsrpdvlhy0qpfhl4>:
  http 6.0.0 (CVE-2025-69219), amazon 9.22.0 (CVE-2026-25604).
- **Not yet attributed:** hashicorp 4.8.0 (CVE-2026-97636) was announced on
  2026-09-24 by Jarek Potiuk, and no `[RESULT][VOTE]` thread pins it to a
  wave. The 2026-09-22 wave (Shahar Epstein per the wiki) has no `[RESULT]`
  yet.

When this list becomes stale, the sync skill will surface it as a
blocker.

## Security team roster

The private security tracker repository is intentionally not named in this
public file. Security-team members configure it in their personal, gitignored
`.apache-magpie-local/release-trains.md`, which takes precedence over this file
(lookup order: `.apache-magpie-local/`, then `.apache-magpie-overrides/`). The **authoritative** source is the collaborator list of the
tracker repository — anyone listed as a collaborator, regardless of
permission level, is on the security team.

```bash
gh api repos/<tracker>/collaborators --jq '.[].login'
```

Snapshot (update in the same change as member joins / rotates):

> Intentionally not recorded in this public file. Security-team members
> keep the roster snapshot in their personal, gitignored
> `.apache-magpie-local/release-trains.md`, which takes precedence per file
> (lookup order: `.apache-magpie-local/`, then `.apache-magpie-overrides/`).

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
- None beyond the above.
