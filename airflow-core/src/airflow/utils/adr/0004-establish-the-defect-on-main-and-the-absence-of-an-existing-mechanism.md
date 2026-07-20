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
wrong. That is a distinctive pattern, and it comes from what these modules are:
old, heavily-used, and full of branches that exist for reasons the diff does not
show.

Three variants recur.

**The defect does not reproduce on `main`.** A user reports a problem against a
released version; a contributor reads the report, finds plausible-looking code,
and fixes it. But the path was already fixed, or was never broken on `main` — the
proposed guard is a second, duplicate call. A PR adding a `default_pool` creation
call to the fresh-database upgrade path was closed after a reviewer pointed at
the lines in `db.py` where `_run_upgradedb()` already makes that call
unconditionally; the reproduction had been done against an older release and
never re-checked.

**The mechanism already exists under a different name.** A new
`airflow db would-migrate` CLI command was written, reviewed, and closed once a
reviewer noted that `airflow db check-migrations -t 0` already exits non-zero and
reports both head revisions when migrations are pending. Nothing was wrong with
the implementation; it simply duplicated a command nobody involved had thought to
look for. This directory and its CLI surface are large enough that "does this
already exist?" is a real question with a non-obvious answer.

**The branch being removed is load-bearing.** The dialect fallbacks in
`with_row_locks` and `nulls_first`, the compat shims, and the seemingly-dead
defensive paths look like clutter and are not. A PR proposing a SQLite fast path
in `with_row_locks` to avoid `FOR UPDATE` compilation was reasoning correctly
about SQLite but about a guard that already exists — `use_row_level_locking` and
the dialect checks are precisely why the helper deliberately returns an *unlocked*
query when locking is unavailable. Simplifying such a branch trades an invisible
correctness property for readability.

The cost asymmetry is what makes this a decision rather than a preference. A
reviewer verifying that a change to `session.py`, `db.py`, or `sqlalchemy.py` is
*needed* has to reconstruct the same history the author skipped, across hundreds
of call sites and three dialects. A PR that establishes need up front converts
that into a two-minute check.

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

- Authors do more work before opening a PR: reproducing on `main`, searching for
  prior art, and reading history. That work is a fraction of what a reviewer
  would otherwise repeat, and it is work only the author can do cheaply.
- Some real bugs take longer to fix, because a contributor who cannot reproduce
  on `main` files an issue instead of a PR. An issue with a clean reproduction is
  more useful here than a speculative patch.
- Defensive changes that would have masked a defect elsewhere do not land, and
  the underlying defect stays visible.
- The reviewer's first question becomes mechanical — *shown to fail on `main`?
  existing mechanism ruled out?* — which lets an unqualified PR be triaged in
  minutes rather than sitting in the queue.

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

- #63558 — creating `default_pool` when upgrading a fresh database with an
  explicit revision: closed after a reviewer showed that `_run_upgradedb()` on
  `main` already calls it unconditionally, and that the PR added a duplicate call;
  the reported behaviour came from an older release.
- #58584 — a proposed `airflow db would-migrate` CLI command: closed once a
  reviewer pointed out that `airflow db check-migrations -t 0` already provides
  exactly that, exit code and head revisions included.
- #67587 — a SQLite fast path in `with_row_locks` to avoid `FOR UPDATE`
  compilation: the helper's existing `use_row_level_locking` and dialect
  fallbacks already return an unlocked query, so the branch being added was a
  second copy of a guard that was already there.
- #69520 — dropping the `LogTemplate` model: closed once reviewers established
  the model is used well beyond the reported case — it drives log-file suffixes
  passed to workers and the reading of historical logs from remote storage. The
  author then filed a PR against the actual root cause.
- #54511 — defensive handling and an index for concurrent rendered-field inserts:
  closed because the failure had been reported once in several years of the
  feature existing; the project waits for a pattern before hardening a hot path.
- #64304, #66249 — repeated attempts at the `db clean` / `dag_version` foreign-key
  and MySQL metadata-lock failure, several of them duplicates of each other and of
  already-open work, none reconciled against what was already on `main`.
