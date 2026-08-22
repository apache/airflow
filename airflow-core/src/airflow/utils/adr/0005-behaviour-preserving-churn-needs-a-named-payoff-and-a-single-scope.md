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

# 5. Behaviour-preserving churn needs a named payoff and a single scope

Date: 2026-07-20

## Status

Accepted

## Context

This directory attracts cleanup: old, grep-able, and full of a decade of house
styles, so a pattern scan lights it up. The resulting PRs are easy to generate (a
regex, linter, or model produces them by the dozen), plausible, and cheap for the
author — none of which they are for the project. Reviewers' reasons for closing
them are consistent across years:

**A rewrite that compiles to the same thing is not an improvement.** A sweep
replacing `os.makedirs`/`os.mkdir` with `Path.mkdir` was declined because
`Path.mkdir` calls `os.mkdir` underneath, so building a `Path` for it is extra
work for no benefit; `Path` earns its place only where joining/parents/globbing
are used. A "remove excessive imports" PR (`utils/sqlalchemy.py`,
`utils/log/file_task_handler.py`) drew a blunt "this is useless". Idiom preference
is not a reason to touch a primitive.

**The current form is often deliberate.** Replacing `print` with `logger.info` in
`utils/db.py` ran into the prints being intentional — `db` commands run before
logging is configured, where the output is the interface. A whitespace-stripping
sweep across 39 files broke tests asserting on the strings it "cleaned". Where
compat shims and dialect fallbacks are load-bearing (`adr/0004`), a cosmetic diff
is exactly what removes an unwritten invariant.

**The cost is downstream, not in the diff.** Churn rewrites `git blame` on
primitives people bisect through, conflicts with every open PR and backport branch
on the same lines, and a 60-file sweep is unreviewable as one unit — a behaviour
change hidden in hunk 200 is invisible.

None of this makes mechanical change forbidden — the project runs repository-wide
migrations regularly (the `from __future__ import annotations` conversion,
ruff-rule adoptions, license headers). Those name and agree the payoff first, use
one tool rather than one contributor's judgement, and split into ownable scopes.
The `airflow/utils` annotations conversion was closed precisely so it could land
that way — an agreed, split-up migration on the dev list — not a standalone
60-file PR.

## Decision

- **Name the payoff, in the PR body, before touching a line that does not change
  behaviour.** A measured cost, a class of bug it makes impossible, a lint rule
  it lets the project turn on, or a deletion it unblocks. "More idiomatic",
  "cleaner", "modernises", and "consistent with the rest of the file" are not
  payoffs on their own here.
- **Do not rewrite a construct into one that lowers to the same call.** Prefer
  the API whose capabilities are actually used; do not pay to construct an object
  for a single method that the plain call already provides.
- **Assume the current form is deliberate until history says otherwise.** Before
  changing a `print` to a log call, a message, a warning class, an exception, or
  a seemingly redundant conversion, check `git log -S` / `git blame` and say what
  you found — several of these predate configured logging or exist for a caller
  the diff does not show.
- **One mechanism, one scope, one PR.** A mechanical change is applied by a
  single stated tool or rule, and is split so each PR covers a scope a reviewer
  can own. Do not combine two unrelated code-smell fixes, and do not mix a
  behaviour change into a mechanical sweep — if the sweep touches behaviour
  anywhere, that hunk is its own PR.
- **A repository-wide sweep is agreed before it is written.** For anything that
  spans this directory broadly, get agreement in an issue or on the dev list
  first, and prefer landing it as an enforced rule (a ruff rule, a prek hook) so
  it stays true, rather than as a one-off pass that decays.
- **Type-checking and lint fixes are held to the same bar as any other change.**
  Silencing an error by widening a type, adding an `Any`, or deleting an
  assertion changes what the checker guarantees for every caller; fix the
  contract or explain why the suppression is correct.

## Consequences

- Real cleanups take more work to justify, and some nice-to-have modernisation
  never lands here — the intended trade where a wrong line is wrong in every
  process.
- First-time contributors are pushed toward work with a demonstrable payoff;
  reviewers should say so early rather than letting a churn PR sit.
- Wanted mechanical improvements land as enforced rules, holding for new code
  instead of being re-applied every year.
- Backport branches and long-running PRs conflict less, and `git blame` keeps
  pointing at the commit that decided something.
- The bar is subjective at the margin: "named payoff" is a judgement call, and
  stating it in the PR body is what makes the argument possible at all.

A change **violates** this decision when it:

- rewrites working code in this directory with no behaviour change and no payoff
  stated in the PR body;
- swaps a construct for an equivalent that lowers to the same underlying call
  (`Path.mkdir` for `os.mkdir`, a comprehension for an equivalent loop, an
  f-string for an equivalent `str()`) purely on style grounds;
- changes a `print`, log level, message, or warning class without stating in the
  PR body why the current form was chosen;
- applies two or more unrelated mechanical fixes in one PR, or spans a large
  number of files without prior agreement on the sweep;
- mixes a behaviour change into an otherwise mechanical diff;
- suppresses a type or lint error by widening a signature, introducing `Any`, or
  deleting a check, rather than fixing what the tool found;
- carries no test change at all while claiming to fix something, or breaks
  existing tests that assert on the exact form being "cleaned up".

## Evidence

The pre-restructure paths in these PRs (`airflow/utils/...`) are the same files
that now live under `airflow-core/src/airflow/utils/`.

- #30404 — "Remove excessive imports" (`utils/sqlalchemy.py`, `utils/timezone.py`,
  `utils/log/file_task_handler.py`): closed, a reviewer said it bought nothing.
- #34385 — `os.makedirs`/`os.mkdir` → `Path.mkdir`: declined; `Path.mkdir` calls
  `os.mkdir` underneath, `Path` is for when its other features are used.
- #33863 — print → `logger` in `utils/db.py`: stalled on the prints being
  intentional.
- #34121 — trailing-whitespace strip across 39 files (`utils/db_cleanup.py`,
  `utils/orm_event_handlers.py`): closed, tests disagreed.
- #33130 — regex code-smell sweep over 60 files (`utils/file.py`,
  `utils/dag_cycle_tester.py`, `utils/log/file_task_handler.py`): closed as too
  large to review.
- #26320 — `airflow/utils` to `__future__.annotations`: closed so it could land as
  the agreed, split-up dev-list migration, not one standalone sweep.
- #34321 — `str()` idiom sweep across 17 files (`utils/process_utils.py`,
  `utils/timeout.py`): did not survive review.
