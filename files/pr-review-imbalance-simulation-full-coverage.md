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
**Nothing was posted or changed.**

This run supersedes the 29-area run. Three things changed: area coverage went
**29 → 36**, `AuthorStanding` was resolved for **every** PR in the queue rather
than only the escalated subset, and two new size rules were added to the skill —
the **contained-diff band** and the **first-contribution size guard**.

## Headline

Every PR in the queue now resolves — 284 of 284, no unknowns.

| verdict | before size rules | after | delta |
|---|---:|---:|---:|
| CLOSE candidate | 32 | 32 | +0 |
| draft-back | 86 | **68** | −18 |
| discuss-first | 74 | 70 | −4 |
| pass | 91 | **113** | +22 |

**100 PRs (35%) would actually leave the ready state** (`CLOSE` + `draft-back`);
another 70 get a question but stay open. That is a large fraction of the queue,
and it is the number to argue about before this step is ever run for real — see
[Is this too much?](#is-this-too-much) below.

All 32 `CLOSE` candidates are `none`-standing authors. No PR with any merged
history reaches `CLOSE` — the intended invariant, now verified across the whole
queue rather than a sample.

## Coverage

| | 7-area seed | 29 areas | 36 areas |
|---|---:|---:|---:|
| fully covered by an area | 25 | 108 | **108** |
| partially covered | 135 | 159 | 159 |
| **not covered at all** | **124 (44%)** | 17 | **17 (6%)** |

The last seven areas (per-provider: `google`, `amazon`, `cncf/kubernetes`,
`common/sql`, …) did not move the coverage counters, because `providers/` already
matched those files via the parent area. What they changed is *quality*: 44 PRs
sat under a single generic `providers` area, and the biggest slices —
`google` (17), `amazon`, `cncf/kubernetes` — now score against their own
criticality, ADRs, and criteria instead of one fallback.

The remaining 17 uncovered PRs are a long tail: scattered `docs/*.rst`,
test-only changes, a few top-level core modules.

## ReviewCost

The contained band is the only change here; area definitions are unchanged.

| ReviewCost | small-only | + contained band |
|---|---:|---:|
| low | 63 | **88** |
| moderate | 28 | 13 |
| high | 32 | 53 |
| **extreme** | **161** | **130** |

Size bands over the queue: **63 small (22%) · 65 contained (23%) · 155 oversized (55%)**.

**65 PRs get a one-tier cost reduction; 41 verdicts soften.** The band exists
because the small ceilings are extremely tight in `critical` areas — 10 lines and
**1 file** — so an ordinary focused fix touching two files was scored as if it
were a sweeping rewrite. Concrete cases it corrects: **#69544** (4 lines, 2 files)
and **#69769** (5 lines, 3 files), both previously `extreme`.

Unlike `small`, `contained` does **not** exempt the close path, so it buys
proportionality without opening a bypass.

## First-contribution size guard — fired once

Across the full queue the guard fired on **1 of 28** oversized first
contributions: **#66751** (306 lines, 2 files, `providers`), `discuss → draft-back`.

That is not a bug, it is arithmetic. The guard is capped at `draft-back` and
never touches `CLOSE`; but once an area is covered at `high`/`critical`, an
oversized diff already scores `high`/`extreme`, where the matrix emits `CLOSE`
for `none` standing. Only **3 PRs in the entire queue** sit in the band where the
guard can act — oversized *and* landing at `moderate`/`low` cost. It is a safety
net for low/medium-criticality areas, not a lever.

**A stricter variant was simulated and rejected.** Judging `none`-standing
authors against the next-stricter ceiling row changed exactly one verdict in the
queue — and changed it into a **new `CLOSE`** on a 49-line bot PR. No useful
discrimination, spent in the worst possible direction.

That result exposed a real defect, now fixed in the skill: **automation accounts
were being scored as first-time contributors.** Bots have no merged-PR history,
so they fell to `none` — the harshest row — for being bots. `AuthorStanding` now
skips them (1 of 18 `none`-standing authors in the sample was `jni-bot`).

## AuthorStanding across the whole queue

| standing | PRs |
|---|---:|
| weak | 143 |
| established | 70 |
| none | 63 |
| trusted | 7 |
| bot (unscored) | 1 |

Standing is measured *in the reference area* per §4, via
`gh api "repos/apache/airflow/commits?author=<login>&path=<area>"` (full history,
path-filtered). Only 7 PRs in the entire ready queue come from committers/PMC.

The `weak` bucket at 143 is the load-bearing number for the aggressive-ruleset
argument below: most of it is contributors with substantial overall history and
no commits in the specific area they touched.

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

## Is this too much?

35% of the ready queue leaving the ready state is a lot, and the honest reading is
that this number is driven by **coverage**, not by severity tuning. At 7 areas the
step touched a quarter of the queue; at 36 areas it touches a third — because most
of the queue now lands in a `high`/`critical` area rather than the `medium/medium`
fallback, and `extreme` is by far the largest cost bucket (130 of 283).

Two things argue this is still defensible:

- `draft-back` (68) is not a rejection. It says "this needs to be split or
  reworked before a maintainer can review it", and the work and the contributor
  are kept.
- The 32 `CLOSE` candidates are all `none`-standing *and* subject to the
  mandatory pre-CLOSE gates, which downgrade most of them to `discuss`.

What genuinely warrants a second look before running this for real is whether
`extreme` should be reachable so easily once nearly every file has a
high-criticality area over it. The contained band was a first correction in that
direction (161 → 130 `extreme`). Further tuning belongs in the cost matrix, not
in the disposition matrix.

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

(Measured over the 81-PR escalated subset of the earlier run, so these are not
directly comparable to the full-queue table above; the *direction* is what matters.)

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
  were not evaluated per-PR; they can only *raise* severity, so 68 draft-backs is a
  floor.
- The 32 CLOSE candidates are subject to the mandatory pre-CLOSE gates
  (linked-and-solved issue / solicited / maintainer already reviewing), which
  downgrade most to `discuss`.
- The committer exemption used `authorAssociation`; `maintainer-already-engaged`
  (§5.4) was not evaluated, so some passes are understated.
- ADR conformance covered 44 of the queue's PRs, not all 284.
- Standing is a point-in-time measurement (2026-07-20) and shifts as PRs merge.
