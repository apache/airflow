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
**`ready for maintainer review`** queue of `apache/airflow` (297 open PRs,
re-measured 2026-07-20). **Nothing was posted or changed.**

This supersedes the earlier runs. Since the last one, the per-area criteria and
ADRs were rebuilt from the project's own history — including the closed-unmerged
PRs that carry the reasons work gets refused — then **attacked adversarially** and
**measured against ~480 live open PRs**. That measurement is the important part of
this document, and it is new.

## Headline

Every PR in the queue resolves — 297 of 297 (one bot excluded from standing).

| verdict | PRs | share |
|---|---:|---:|
| CLOSE candidate | 37 | 12% |
| draft-back | 71 | 24% |
| discuss-first | 75 | 25% |
| pass | 113 | 38% |

**108 PRs (36%) would leave the ready state** (`CLOSE` + `draft-back`); another 75
get a question but stay open.

All 37 `CLOSE` candidates are `none`-standing authors. No PR with any merged
history reaches `CLOSE` — the intended invariant, verified across the whole queue.

**But see [What the measurement changed](#what-the-measurement-changed): these
dispositions are not safe to apply automatically, and the reason is now measured
rather than assumed.**

## Coverage and cost

Coverage: **280 of 296 (95%)** touch at least one of the 36 defined areas.

| ReviewCost | PRs |
|---|---:|
| extreme | 139 |
| high | 57 |
| moderate | 14 |
| low | 86 |

Size bands: **61 small (21%) · 70 contained (24%) · 165 oversized (56%)**.

The contained band (3× the small ceilings) exists because the small ceilings are
very tight in `critical` areas — 10 lines and **1 file** — so an ordinary focused
fix touching two files scored as if it were a sweeping rewrite. Unlike `small`,
`contained` does not exempt the close path.

Author standing across the queue: **6 trusted · 80 established · 143 weak ·
67 none**. Only six PRs in the entire ready queue come from committers or PMC.

## What the measurement changed

The rules were run against ~480 live open PRs and every firing classified as a
true positive, a false positive, or undecidable-from-a-diff. This is the number
that governs whether any of this can be automated.

| area group | starting false-positive rate | after repair |
|---|---:|---|
| core (models/jobs/utils/dag_processing/migrations/executors/serialization) | **40%** (19 of 48 firings) | fixed; not re-measured |
| providers/standard | **64%** (of a 65% firing rate) | projected ~20% firing, near-zero FP |
| cli + airflow-ctl | **80%** (4 of 5 firings) | projected 0 FP of 1 firing |
| task-sdk (execution_time, definitions) | 25–33% | fixed; not re-measured |
| execution_api, core_api, common, ui, chart, shared, dev, prek | 0–7% | already sound |

The failure mode was uniform, and five independent reviewers named it
identically: **Context sections that reason well, compressed into violation
bullets that lost their exceptions.** The evidence base was not the problem —
across ~250 sampled citations there were **zero fabrications**.

Rules were firing on work the project had already merged:

- an **index migration** (`0086`, via #61983) that fixed real scheduler heartbeat
  misses, condemned for lacking a benchmark;
- **standalone cookie hardening** (#62771, #65348, #66502, #67909 — all merged),
  condemned by a gate built on a single closure, with #66502 landing on the very
  file that closure was on, nine months later;
- the **removal of an N+1** (#57270, merged on query-count arithmetic alone),
  condemned by a performance rule whose sibling ADR *requires* removing N+1s;
- **every AIP-94 command port** — ~40% of the open `airflow-ctl` queue — condemned
  as "surface that already exists on `main`", which is precisely what a port is;
- a PR that **narrows** a bare `except:` (#69544), condemned by a bullet forbidding
  *widening*.

One rule was simply wrong about the code it governed: `jobs/adr/0005` claimed the
triggerer holds no ORM session, but `triggerer_job_runner.py` holds them in four
places and deliberately sets `_AIRFLOW_PROCESS_CONTEXT="server"` (merged #64022).
Its title and two bullets fired on `main` as it stood.

All of the above are repaired. The repairs themselves were then re-validated
against the same live PRs and confirmed to stand down correctly.

## ADR conformance (§2c)

44 PRs checked against their area's `Accepted` ADRs — the escalated set at
`pass`/`discuss`, plus small-diff-exempted PRs in `critical`/`high` areas.

**4 contradicts-fixable · 0 deliberate reversals.**

| PR | area | ADR | violation |
|---|---|---|---|
| #69985 | migrations | 0001 | edits a **released** migration (`0094`, in tag 3.2.0) in place; the migration completes but writes callback data in the wrong encoding, so a forward revision could re-encode — outside the "cannot complete" exception |
| #65758 | chart | 0003 | new public value (`Git-Sync` Dag mount path), no `chart/newsfragments/` entry |
| #64941 | providers | 0001 | adds `datafusion>=50.0.0,<52.0.0` — an upper bound with no justification, issue, or URL at the cap site |
| #70026 | chart | 0003 + 0002 | 5-line template change; no newsfragment **and** rolls every worker pod on `helm upgrade` with an unchanged values file |

**One finding from the previous run is retracted.** #68180 was reported as
"flips a released `BaseOperator` default for every Dag that never set it". It is
actually "Fix dead config: `scheduler.ignore_first_depends_on_past_by_default`" —
a config key documented in `config.yml` that **no code reads**. Restoring the
documented contract is a defect fix needing a newsfragment, not an AIP. The
repaired `definitions/adr/0001` now carves this out explicitly, and the rule no
longer fires. The original characterisation was wrong.

**#70026 was found only by checking small-diff-exempted PRs.** The exemption pins
`ReviewCost` to `low`, so the matrix passes it — but §6.1 exempts the *close path
only*, not the criteria gate. Small diffs in high-criticality areas are the
highest-yield ADR targets, not the safest.

## Where the rules do and do not earn their keep

Not every area produced a usable rule set, and that is recorded rather than
papered over.

- **`jobs` is inert.** Its five ADRs fired on **1 of 24** open PRs, and that firing
  was a false positive. The area with the highest criticality in the repo has
  rules that do nothing on the live queue — because its real cost is concurrency
  reasoning (lock retention, commit boundaries, load-dependent interleavings) that
  a diff does not expose. No triggers were invented to make it look busy.
- **`shared` (0 of 8), `dev` (0 of 6), `prek` (0 of 3)** fired on nothing.
- **`docs`** produced zero ADR-level true positives across 13 PRs; all four
  firings were PR-title nits.
- **The strongest single rule is duplicate-work detection** (`providers/adr/0005`):
  9 clusters, ~20 PRs, **zero false positives**, and it correctly abstained on both
  shapes designed to fool it — a deliberate per-provider split, and explicitly
  disclosed stacked PRs. That matters, because "superseded/duplicate" was the
  single largest principled-rejection reason in the mined corpus (119 PRs).

## Is this too much?

36% of the ready queue leaving the ready state is a lot, and the honest reading is
that the number is driven by **coverage**, not severity tuning. Most of the queue
now lands in a `high`/`critical` area rather than a `medium/medium` fallback, and
`extreme` is the largest cost bucket (139 of 296).

Two things argue it is still defensible: `draft-back` (71) is not a rejection —
it says the change needs splitting or reworking before review, keeping both the
work and the contributor; and the 37 `CLOSE` candidates are all `none`-standing
*and* subject to the mandatory pre-CLOSE gates, which downgrade most to `discuss`.

What warrants a second look is whether `extreme` should be reachable so easily now
that nearly every file has a high-criticality area over it. The contained band was
a first correction in that direction. Further tuning belongs in the cost matrix,
not the disposition matrix.

## Recommendation

**No area is safe to drive automatic `CLOSE`.** That is the measured conclusion of
five independent adversarial reviews and four validation passes, not a cautious
guess.

`draft-back` is defensible today for `execution_api`, `core_api`, `common`, `ui`,
`chart`, `shared`, `dev`, `prek`, and `dag_processing` — the areas whose measured
false-positive rate was 0–7%. `providers/standard` and `cli`/`airflow-ctl` should
be re-measured after the repairs before running anything automatic against them.

The material's real value is as **review context and draft-back justification**:
a specific, evidenced reason to send a change back, in an area where the reviewer
would otherwise have to reconstruct the decision from memory.

## Caveats

- Dispositions are the imbalance matrix plus §2c. Unmet `## Review criteria` (§2b)
  were not evaluated per-PR; they can only *raise* severity, so 71 draft-backs is a
  floor.
- The 37 CLOSE candidates are subject to the mandatory pre-CLOSE gates
  (linked-and-solved issue / solicited / maintainer already reviewing).
- The committer exemption used `authorAssociation`; `maintainer-already-engaged`
  (§5.4) was not evaluated, so some passes are understated.
- ADR conformance covered 44 of the queue's PRs, not all 297.
- Post-repair false-positive rates for the core and task-sdk groups are
  **projected from the fixes applied, not re-measured**. A second validation pass
  is the honest next step.
- Standing is a point-in-time measurement (2026-07-20) and shifts as PRs merge.
