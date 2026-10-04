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

- [Apache Airflow — pr-management-triage duplicate sweep](#apache-airflow--pr-management-triage-duplicate-sweep)
  - [Why this override exists](#why-this-override-exists)
  - [Where the sweep runs](#where-the-sweep-runs)
  - [D1 — Build candidate clusters](#d1--build-candidate-clusters)
  - [D2 — Verify by diff (mandatory)](#d2--verify-by-diff-mandatory)
  - [D3 — Complementary-not-duplicate guard](#d3--complementary-not-duplicate-guard)
  - [D4 — Pick the survivor](#d4--pick-the-survivor)
  - [D5 — Propose, approve, close](#d5--propose-approve-close)
  - [Comment template](#comment-template)
  - [Calibration](#calibration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage duplicate sweep

This file **adds a phase** to the
[`pr-management-triage`](../.apache-magpie/skills/pr-management-triage/SKILL.md)
skill (invoked as `/magpie-pr-management-triage`): detecting PRs that
duplicate an earlier open PR, and closing the redundant one in the
earlier one's favour.

## Why this override exists

The framework skill has **no duplicate-PR handling**. Every
occurrence of "duplicate" in `classify-and-act.md`, `rationale.md`
and `actions.md` refers to duplicate *proposals* — the skill not
re-suggesting the same action on the same PR — never to two PRs
fixing the same bug.

Duplicate PRs are nevertheless one of the most common closure
reasons in this project. In a sample of the 300 most recently
closed-unmerged `apache/airflow` PRs, "duplicates an earlier PR"
was the single largest maintainer-authored closure category
(#70802, #70437, #70621, #70656, #70678, #71136, #71612 among
others).

The sweep cannot be a decision-table row. That table is a **pure
function of per-PR state** from one batched GraphQL query, with no
network calls and no cross-PR comparison. Duplicate detection needs
both. So it runs as a separate pass, in the same position as
[`stale-sweeps.md`](../.apache-magpie/skills/pr-management-triage/stale-sweeps.md).

## Where the sweep runs

After the decision table has classified every PR in the batch, and
before the interaction loop presents actions. Its proposals join the
normal one-at-a-time approval queue.

Scope it to the **open** PR set already fetched for the batch — do
not issue a fresh repo-wide search. A PR the decision table routed
to `skip` is still eligible to be a duplicate.

## D1 — Build candidate clusters

Cluster on **changed-file overlap AND title-token similarity**.
Either signal alone produces mostly noise.

1. For each PR, take `files.nodes.path` and drop **churn paths** —
   files nearly every PR touches, which manufacture false overlap:

   ```text
   newsfragments/          uv.lock              pyproject.toml
   */provider.yaml         */__init__.py        docs/
   */index.rst             CHANGELOG*           */changelog.rst
   .pre-commit-config.yaml AGENTS.md            CLAUDE.md
   generated/known_airflow_exceptions.txt
   scripts/ci/prek/validate_operators_init_exemptions.txt
   dev/breeze/doc/images/
   ```

   The last three are Airflow-specific ledgers and generated
   artefacts. `validate_operators_init_exemptions.txt` in particular
   is touched by *every* operator-`__init__` PR and, left in, pairs
   unrelated provider changes together.

2. Two PRs are a candidate pair when **both** hold:
   - file overlap `>= 0.5` of the smaller surviving file set, and
   - title-token similarity `>= 0.34` (lowercase words of 4+ chars,
     minus project filler: `airflow`, `when`, `with`, `from`,
     `that`, `this`, `into`, `have`, `than`, `fix`, `fixes`, `make`,
     `does`, `only`, `used`, `using`, `adds`, `task`).

3. Cluster candidates transitively, then sort each cluster by
   `createdAt` ascending.

**D1 output is a hypothesis, never a verdict.**

## D2 — Verify by diff (mandatory)

For every cluster, fetch `gh pr diff <n>` for each member and
compare the **production-code hunks** (ignore tests, newsfragments
and changelog entries for the subsumption judgement; use them only
as tie-breakers in D4).

Never propose a closure on D1's clustering alone. This step is not
optional and not skippable under a `--yes`-style flag.

## D3 — Complementary-not-duplicate guard

Two PRs touching the same function are **not** duplicates when each
fixes a part the other leaves broken. Closing either loses work.

Declare a duplicate only when one diff's production change is a
**superset** of the other's. If neither subsumes the other, drop the
cluster and surface it as a note instead — the two authors may need
to coordinate, which is a maintainer's call, not triage's.

Worked examples from the 2026-08-17 sweep:

- **Complementary** — #70250 fixed `datetime.datetime` in
  `_python_type_from_string`; #71328 fixed `datetime.date` in the
  same map. Neither subsumed the other. (Both were still closed,
  because a *third* PR, #70249, subsumed both.)
- **Not duplicates at all** — #68125 ("Fix backfill completion race
  during creation") row-locks `DagModel` in `_create_backfill`;
  #68729 ("Fix backfill completion race") rewrites the
  `mark_backfills_complete` query for `IN_FLIGHT` rows. Near-identical
  titles, different functions, different races.

## D4 — Pick the survivor

Default: **earliest `createdAt` wins.** This is the project's stated
convention — parallel work is allowed and "better PR wins", but
contributors are asked to check for an existing PR first
(`contributing-docs/04_how_to_contribute.rst`).

Override the default only when the later PR is materially better on
one of:

| Tie-breaker | Beats "earliest" when |
|---|---|
| Completeness | It fixes cases the earlier one leaves broken (async twin, second code path, extra type) |
| Mergeability | The earlier one is `CONFLICTING` and the later one is `MERGEABLE` |
| CI | The earlier one is red on non-flaky legs and the later one is green |
| Tests / newsfragment | The earlier one has none and the later one does |

When the tie-breakers **split** — earliest is better on one axis,
later on another — do not auto-resolve. Surface both to the
maintainer and let them choose.

## D5 — Propose, approve, close

One PR at a time, through the standard interaction loop. Each
proposal shows: the two PR refs, the concrete subsumption evidence
from D2, and the D4 tie-breaker that decided it.

Nothing is posted without explicit human approval — the
[confirm-before-posting rule](../CLAUDE.md) applies in full. Re-check
that both PRs are still `OPEN` immediately before closing; abort the
proposal if either changed since the batch was fetched.

Close with `gh pr close <n> --repo apache/airflow --comment "$(cat <body-file>)"`
so the explanation and the closure land together.

## Comment template

Name the surviving PR, give the concrete reason, and credit the work.
Never close with a bare "duplicate".

```markdown
Thanks for this, and sorry to close it.

This duplicates #<survivor>, which <specific overlap — the same change
to the same function / the same file set>, and was opened on <date>,
<N> days before this one.

#<survivor> also carries <what this PR lacks — the async twin, tests,
a newsfragment, the second code path>.

Airflow does allow parallel work on the same problem — "better PR
wins" — but the convention is to check for an existing PR before
starting. No criticism intended; the diagnosis here was right.
Closing in favour of #<survivor>.

---

_Drafted by an AI assistant and may contain mistakes — if you think
this call is wrong, say so and the PR will be reopened; a human
maintainer has the final word._

Drafted-by: <Agent Name and Version>; reviewed by @<handle> before posting
```

When the survivor is *not* the earliest PR, say so explicitly and
own it — the earlier author waited longer and is losing the
tie-break. See #67600 for the wording used when a ten-week-older PR
was closed in favour of a greener, more complete one.

Both footers are required by
[`AGENTS.md`](../AGENTS.md) and the per-project memory conventions:
the AI disclaimer and the `Drafted-by:` attribution.

## Calibration

From the 2026-08-17 run over 238 open PRs (review-requested ∪
mentions):

| Stage | Clusters / PRs |
|---|---|
| D1 file-overlap only, no churn filter | 88 close candidates |
| D1 with churn filter + title similarity | 15 clusters |
| D2 diff verification | **5 genuine duplicates** |

The heuristic was wrong for roughly 94% of its raw candidates. Budget
for D2 accordingly, and treat any run that closes PRs without
per-pair diff reading as a defect in the sweep, not a time saving.
