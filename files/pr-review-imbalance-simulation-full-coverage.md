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

# Review-imbalance simulation — full-coverage run

Read-only simulation of the `pr-management-triage` review-imbalance step over the
**`ready for maintainer review`** queue of `apache/airflow` (284 open PRs).
**Nothing was posted or changed.** This run supersedes the earlier 7-area seeded
simulation: area coverage was extended from **7 to 29** areas, and the ADR
conformance step (§2c) was applied for the first time.

## Coverage

Adding 22 areas moved ready-queue coverage from 56% to 94%.

| | 7-area seed | 23 areas | 29 areas |
|---|---:|---:|---:|
| fully covered by an area | 25 | 25 | **108** |
| partially covered | 135 | 135 | 159 |
| **not covered at all** | **124 (44%)** | 124 | **17 (6%)** |

The largest single gap was `providers/` — ~21% of the queue, scoring `medium/medium`
via the fallback with no criteria or ADR check. The remaining 17 uncovered PRs are a
long tail (scattered `docs/*.rst`, test-only changes, a few top-level core modules).

## ReviewCost

| ReviewCost | 7-area seed | 29 areas |
|---|---:|---:|
| low | 106 | 67 |
| moderate | 65 | 39 |
| high | 40 | 36 |
| **extreme** | **73** | **142** |

**84 of 284** PRs escalated. The new areas driving it: `execution_time` 17,
`providers` 14, `utils` 14, `core_api` 9, `definitions` 9, `ui` 8, `dag_processing` 8,
`airflow-ctl` 6, `chart` 6, `shared` 6, `migrations` 4.

## Dispositions (balanced ruleset)

Over the **81** escalated non-committer PRs. `authorAssociation` confirmed none are
MEMBER/OWNER/COLLABORATOR: 45 CONTRIBUTOR, 12 FIRST_TIME_CONTRIBUTOR (+24 added with
the last six areas).

| verdict | lenient standing | strict in-area | + ADR conformance |
|---|---:|---:|---:|
| draft-back | 13 | 35 | **40** |
| discuss-first | 43 | 26 | 22 |
| CLOSE candidate | 14 | 14 | 14 |
| pass | 11 | 6 | 6 |

**75 of 81 leave the ready queue** — about 26% of the whole queue.

Standing was measured *in the reference area* per §4, using
`gh api "repos/apache/airflow/commits?author=<login>&path=<area>"` (full history,
path-filtered). Moving from an overall-merge proxy to real in-area counts
reclassified 21 PRs, including authors with 55 and 59 merged PRs and **zero**
commits in the area their PR touched.

## ADR conformance (§2c)

44 PRs checked against their area's `Accepted` ADRs — the 32 at `pass`/`discuss`
among the escalated set, plus 12 small-diff-exempted PRs in `critical`/`high` areas.

**39 conform · 5 contradicts-fixable · 0 deliberate reversals** (~11%).

| PR | area | ADR | violation |
|---|---|---|---|
| #69985 | migrations | 0001 | edits a **released** migration in place; its newsfragment concedes rows need out-of-band repair |
| #68180 | definitions | 0001 | flips a released `BaseOperator` default for every Dag that never set it |
| #65758 | chart | 0003 | new public value, no `chart/newsfragments/` entry |
| #64941 | providers | 0001 | dependency upper bound with no justification, issue, or URL at the cap site |
| #70026 | chart | 0003 + 0002 | 5-line template change; no newsfragment **and** rolls every worker pod on `helm upgrade` with an unchanged values file |

**#70026 was found only by checking small-diff-exempted PRs.** The exemption pins
`ReviewCost` to `low`, so the matrix passes it — but §6.1 exempts the *close path
only*, not the criteria gate. Small diffs in high-criticality areas are the
highest-yield ADR targets, not the safest.

Every ADR that fired belongs to the **released-contract / release-note discipline**
family. Not one structural boundary breach (no ORM-in-DFP, no core-import-in-shared,
no hand-edited generated client) — a signal about where this codebase actually drifts.

Five **inverse** cases were correctly *not* flagged, i.e. small diffs that remove an
existing violation: #68244 (set-based `EXISTS` + `skip_locked`), #65440 (removes an
improper `session.commit()`), #69632 (`ipv6` silently bound to `maxudpsize`), #66350
(`db_cleanup` config naming non-existent columns), #68415 (drops `type: ignore`).

## Aggressive ruleset — simulated, and not recommended

An aggressive profile was simulated: the **un-softened matrix** (every non-trusted
cell one step more severe), **ambiguity resolving downward** instead of upward, and
**halved small-diff ceilings**.

| verdict | balanced | aggressive |
|---|---:|---:|
| CLOSE candidate | 14 | **49** |
| draft-back | 35 | 26 |
| discuss-first | 26 | 6 |
| **pass** | 6 | **0** |

67 of 81 verdicts harden and the at-risk set passes **nothing**. The decisive
problem is *who* the 35 new CLOSEs land on: every one is `weak → weak`, not a
first-timer. They include contributors with **55**, **59**, **19**, **16** and **14**
merged PRs whose only fault is having no merged commits in the specific area their PR
touches. Closing those is a community-relations incident, not triage.

The two knobs compound badly. The softened `extreme` row exists precisely so that an
author with *some* standing is routed to a work-it-out path; strict in-area standing
simultaneously makes `weak` a far broader category than that row assumes. Hardening
the matrix while keeping strict in-area measurement is therefore not a small
tightening — it converts the step from triage into a gate.

Halving the small-diff ceilings is the one defensible part: exemptions drop 54 → 35,
pulling 19 PRs into the matrix — but 14 are `medium`-criticality noise and only 4 are
the `critical`/`high` cases worth the review cost. Running §2c on small-diff-exempted
PRs (already recommended above) achieves the same catch without the cost.

**Recommendation: keep the balanced ruleset.** If more pressure is wanted, apply it
by extending ADR coverage, not by hardening the matrix.

## Caveats

- Dispositions are the imbalance matrix plus §2c. Unmet `## Review criteria` (§2b)
  were not evaluated per-PR; they can only *raise* severity, so 40 draft-backs is a
  floor.
- The 14 CLOSE candidates are subject to the mandatory pre-CLOSE gates
  (linked-and-solved issue / solicited / maintainer already reviewing), which
  downgrade most to `discuss`.
- The committer exemption used `authorAssociation`; `maintainer-already-engaged`
  (§5.4) was not evaluated, so some passes are understated.
- ADR conformance covered 44 of the queue's PRs, not all 284.
