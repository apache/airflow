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

# 4. Establish the defect on `main` and the absence of an existing mechanism before changing a primitive

Date: 2026-07-20

## Status

Accepted

## Context

More changes to this directory are closed for being unnecessary than for being
wrong — a distinctive pattern from what these modules are: old, heavily-used, and
full of branches whose reasons the diff does not show. Three variants recur.

**The defect does not reproduce on `main`.** A user reports a problem against a
release; a contributor fixes plausible-looking code that was already fixed, or
never broken, on `main`. A PR adding a `default_pool` creation call to the
fresh-database upgrade path was closed once a reviewer pointed at `db.py` where
`_run_upgradedb()` already makes that call unconditionally; the reproduction was
against an older release, never re-checked.

**The mechanism already exists under a different name.** A new
`airflow db would-migrate` command was written and closed once a reviewer noted
`airflow db check-migrations -t 0` already exits non-zero and reports both head
revisions. The CLI surface is large enough that "does this already exist?" has a
non-obvious answer.

**The branch being removed is load-bearing.** A SQLite fast path in
`with_row_locks` to avoid `FOR UPDATE` compilation reasoned correctly about SQLite
but about a guard already there — `use_row_level_locking` and the dialect checks
are precisely why the helper deliberately returns an *unlocked* query when locking
is unavailable. Simplifying such a branch trades an invisible correctness property
for readability.

The cost asymmetry makes this a decision: a reviewer verifying a change to
`session.py`, `db.py`, or `sqlalchemy.py` is *needed* must reconstruct the history
the author skipped across hundreds of call sites and three dialects. A PR that
establishes need up front converts that into a two-minute check.

## Decision

- **Reproduce the defect on current `main`, and say so in the PR.** State the
  commit or branch, the backend, and the observed failure. A report filed against
  a released version is a starting point, not evidence.
- **Search for the existing mechanism before adding one.** Before a new helper,
  CLI command, config option, or guard: grep this directory, the CLI definitions,
  and the docs for the capability, and name in the PR body what you found and why
  it is insufficient.
- **Do not remove or "simplify" a branch until you can state why it was added.**
  Dialect fallbacks, `TYPE_CHECKING` guards, compat shims, and defensive
  conversions are guilty-until-proven-innocent in this directory. If `git log -S`
  and `git blame` do not explain it, ask.
- **Link the change to a demonstrated failure, not a hypothesis.** Hardening,
  extra validation, and defensive error handling on a path with no reported
  failure add cost to every caller for a problem nobody has.
- **Fix the layer that produced the problem, not the primitive that surfaced
  it.** Widening a helper, dropping a model, or relaxing a constraint to make a
  symptom go away moves the defect rather than removing it.

## Consequences

- Authors do more before opening a PR — reproduce on `main`, search prior art,
  read history — a fraction of what a reviewer would otherwise repeat, and work
  only the author can do cheaply.
- Some real bugs take longer to fix, because a contributor who cannot reproduce on
  `main` files an issue instead; a clean reproduction beats a speculative patch.
- Defensive changes that would have masked a defect elsewhere do not land, so the
  underlying defect stays visible.
- The reviewer's first question is mechanical — *shown to fail on `main`? existing
  mechanism ruled out?* — triaging an unqualified PR in minutes.

A change **violates** this decision when it:

- describes a failure only against a released version, with no statement that it
  was reproduced on current `main`;
- adds a helper, CLI command, or config option that duplicates an existing one,
  without naming the existing one and explaining the gap;
- adds a guard, retry, or validation to a path where no failure has been
  observed, on the grounds that it could theoretically fail — a change whose
  stated failure mode is a trust-boundary violation with a described attack path
  is in scope;
- removes a dialect branch, compat shim, or defensive conversion without stating
  what it was for;
- adds a second call to something the surrounding code already does
  unconditionally;
- drops, widens, or bypasses a model, column, or constraint to make a symptom
  disappear, when the code that produced the bad value is the actual defect;
- carries no test that fails without the change — the mechanical form of "this
  fixes something real".

A reviewer should ask, before reading the implementation: *where does this fail
on `main`, and what existing mechanism was ruled out?*

## Evidence

- #63558 — `default_pool` on fresh-database upgrade: closed; `_run_upgradedb()` on
  `main` already calls it unconditionally, so the PR was a duplicate; reproduction
  was against an older release.
- #58584 — `airflow db would-migrate`: closed; `airflow db check-migrations -t 0`
  already provides it, exit code and head revisions included.
- #67587 — SQLite fast path in `with_row_locks`: `use_row_level_locking` and the
  dialect fallbacks already return an unlocked query, so the branch was a second
  copy.
- #69520 — dropping `LogTemplate`: closed; the model drives log-file suffixes and
  historical-log reading well beyond the reported case. Author refiled against the
  root cause.
- #54511 — defensive handling / index for concurrent rendered-field inserts:
  closed; reported once in years, and the project waits for a pattern before
  hardening a hot path.
- #64304, #66249 — repeated `db clean` / `dag_version` FK / MySQL metadata-lock
  attempts, duplicates of each other and of open work, none reconciled against
  `main`.
