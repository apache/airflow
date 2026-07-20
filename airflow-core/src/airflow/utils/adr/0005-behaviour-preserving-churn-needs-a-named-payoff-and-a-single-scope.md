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

This directory attracts cleanup. It is old, it is grep-able, and it is full of
code written across a decade of different house styles — so a pattern scan over
the repository lights it up more brightly than almost anywhere else. The
resulting PRs are easy to produce (a regex, a linter, a code-smell tool, or a
model can generate them by the dozen), plausible to read, and cheap for the
author. They are none of those things for the project.

Several such changes have been closed here, and the reviewers' reasons are
consistent across years and across reviewers.

**A rewrite that compiles to the same thing is not an improvement.** A sweep
replacing `os.makedirs`/`os.mkdir` with `Path.mkdir` was declined because
`Path.mkdir` calls `os.mkdir` underneath: constructing a `Path` purely to call it
is extra work for no benefit, and `Path` earns its place only where its other
capabilities (joining, parents, globbing) are actually used. A PR removing
"excessive imports" across the tree, `utils/sqlalchemy.py` and
`utils/log/file_task_handler.py` included, drew a blunt "this is useless".
Neither change was wrong; both were rejected because idiom preference is not a
reason to touch a primitive.

**The current form is often deliberate.** A PR replacing `print` with
`logger.info`/`logger.error` in `utils/db.py` ran into the observation that the
prints were intentional — `db` commands run before logging is configured, and in
contexts where the output is the interface. A whitespace-stripping sweep across
39 files broke tests that were asserting on the strings it "cleaned". In a
directory where compat shims and dialect fallbacks are load-bearing (see
`adr/0004`), a cosmetic diff is exactly the shape of change that removes an
invariant nobody wrote down.

**The cost is not in the diff, it is in everything downstream.** Churn here
rewrites `git blame` on the primitives people bisect through, conflicts with
every open PR and every active backport branch touching the same lines, and
consumes review attention that the fan-in of this directory says should be spent
carefully. A 60-file sweep is also unreviewable as one unit: the reviewer either
rubber-stamps it or re-derives each hunk, and a genuine behaviour change hidden
in hunk 200 is invisible either way.

None of this makes mechanical change forbidden — the project performs
repository-wide migrations regularly (the `from __future__ import annotations`
conversion, ruff-rule adoptions, license headers). What distinguishes those is
that the payoff was named and agreed first, the mechanism was one tool rather
than one contributor's judgement, and the work was split into scopes someone
could own. The `airflow/utils` annotations conversion was closed precisely so it
could land that way — as part of an agreed, split-up migration announced on the
dev list — rather than as a standalone 60-file PR.

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

- Real cleanups take more work to justify, and some genuinely nice-to-have
  modernisation never lands here. That is the intended trade for a directory
  where a wrong line is a wrong line in every process.
- Contributors looking for a first change are pushed away from this directory and
  towards work with a demonstrable payoff. Reviewers should say that plainly and
  early rather than letting a churn PR sit.
- Mechanical improvements that *are* wanted land as enforced rules, so they hold
  for new code instead of being re-applied every year.
- Backport branches and long-running PRs conflict less, and `git blame` on the
  primitives keeps pointing at the commit that actually decided something.
- The bar is subjective at the margin: "named payoff" is a judgement call, and a
  reviewer can decline a change another would accept. Stating the payoff in the
  PR body is what makes that argument possible at all.

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

- #30404 — "Remove excessive imports", touching `utils/sqlalchemy.py`,
  `utils/timezone.py` and `utils/log/file_task_handler.py`: closed with a
  reviewer stating outright that the change bought nothing.
- #34385 — replacing `os.makedirs`/`os.mkdir` with `Path.mkdir`: declined because
  `Path.mkdir` calls `os.mkdir` underneath, so building a `Path` for it is extra
  work with no benefit; two reviewers agreed `Path` is for when its other
  features are used.
- #33863 — "Replace print by logger.info/error in db util": a one-file cosmetic
  change to `utils/db.py` that stalled on the point that the prints were
  intentional.
- #34121 — "Refactor: remove trailing whitespace from strings" across 39 files
  including `utils/db_cleanup.py` and `utils/orm_event_handlers.py`: closed with
  the tests disagreeing with the change.
- #33130 — a regex-driven code-smell sweep over 60 files including
  `utils/file.py`, `utils/dag_cycle_tester.py` and
  `utils/log/file_task_handler.py`: the author was asked to split it and closed
  it as too large to review.
- #26320 — "Convert `airflow/utils` to `__future__.annotations`": closed so the
  conversion could land as part of the agreed, split-up repository-wide migration
  coordinated on the dev list, rather than as one standalone sweep.
- #34321 — "Refactor usage of `str()`" across 17 files including
  `utils/process_utils.py` and `utils/timeout.py`: another broad idiom sweep that
  did not survive review.
