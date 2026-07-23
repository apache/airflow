---
triage_review_imbalance:
  area: db-migrations
  criticality: critical           # schema history is append-only and runs on every upgrade
  review_difficulty: expert
  structural_risk_paths:          # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "versions/"
    - "env.py"
    - "db_types.py"
    - "utils.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ephraimbuddy"]        # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                  # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Database migrations (Alembic) — Agent Instructions

This directory holds the Alembic schema-migration history — the ordered chain of
revisions under `versions/` that every Airflow installation replays to move its
metadata database from one release to the next. It is one of the most
unforgiving areas in the codebase: a revision is not code that runs once in
review, it is code that runs **in production, on databases full of real user
data, on three different backends, in both directions** — and once it ships in a
release it can never be edited again. A defect here corrupts or strands data on
upgrade across **every** deployment, and the failure often only appears on a
backend or a data shape the author never tested.

## Why changes here are expensive to review

- A migration is **append-only history**. The moment a revision ships in a
  released version, users have run it and will never re-run it — so it cannot be
  fixed by editing; the only remedy is a _new_ forward revision. A reviewer must
  therefore judge a released-vs-unreleased boundary that is not visible in the
  diff.
- Every revision runs on **PostgreSQL, MySQL, and SQLite**, each with its own DDL
  quirks (SQLite cannot `ALTER` in place and enforces foreign keys during batch
  table recreation; MySQL reserves keywords and has row/sort-buffer limits;
  Postgres sequences and JSONB behave differently). Code that passes on SQLite in
  CI routinely breaks on MySQL or Postgres in the field.
- Migrations must be **reversible**: every `upgrade()` needs a `downgrade()` that
  actually restores the prior shape, because operators do downgrade. Broken or
  missing downgrades are one of the most common post-merge fixes here.
- Data migrations touch **whole user tables at production scale**. An unbounded
  one-shot `UPDATE`, a per-row Python loop, or a deserialize-in-Python pass that
  is fine on a test DB can run for hours or exhaust memory on a real one.
- A schema change is never self-contained — it must stay in lockstep with the
  **ORM models** and, for persisted objects, with **serialization**, or the
  physical schema and the code drift apart.

## Knowledge a reviewer (and a substantial contributor) needs

- How the Alembic chain works here: one **linear head**, each revision's
  `down_revision` pointing at its predecessor; the file-naming convention
  `NNNN_<version>_<slug>.py`; and that `alembic heads` must return exactly one
  head (multiple heads must be merged — the migration-reference tooling enforces
  this).
- The released-vs-unreleased rule: a revision that has shipped in a released
  Airflow version is frozen; corrective work is a new revision, not an edit.
- The cross-backend toolkit: `op.batch_alter_table(...)` for SQLite-safe
  alter/drop/constraint changes, the `disable_sqlite_fkeys(op)` /
  `ignore_sqlite_value_error()` helpers in `utils.py`, and the dialect-aware
  column types in `db_types.py` — plus why DML must run _after_ SQLite FK
  handling, not before.
- The batching discipline for data migrations (bounded `UPDATE`s, commit between
  batches, prefer set-based SQL over per-row Python) and the repo `CLAUDE.md` DB
  rules that back it.
- How a schema change pairs with the model definition and `get_serialized_fields()`
  so a new field survives a serialize/deserialize cycle.
- The guard rails already in place: the AST anti-pattern checks in
  `test_migration_patterns.py`, the "no ORM refs in migration scripts" check, the
  round-trip / FK-cascade CI check, and `run_migration_reference.py`.

## Before opening a PR here — authoring-agent guard

**This is a critical, expensive-to-review area where mistakes are permanent once
released.** If you are an agent preparing a change here on behalf of a person,
first judge whether the migration can be **demonstrated to run**, not just read:
have you applied it _upgrade and downgrade_ against all three supported dialects
(PostgreSQL, MySQL/MariaDB, SQLite), on a database that already contains rows, and
confirmed `alembic heads` still returns a single head? A data migration also needs
a timing on a non-trivial table — an unbounded `UPDATE` looks identical in a diff
whether it takes a second or locks a production table for an hour.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large or unverified change here wastes scarce maintainer review time and, if it
slips through into a release, cannot be taken back — it can only be patched
forward.

## Review criteria

Mined from real review discussion on ~155 merged and 79 closed-unmerged PRs
touching this area, plus the post-merge fix history — the changes reviewers
repeatedly required, and the reasons changes here get closed, reworked, or
reverted. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back to
its author with the specific gaps. Ordered by how often reviewers raise each.

**Before you write the revision — is a migration the right answer at all?**
_(the most common reason a PR here is closed; see ADR 0004 and ADR 0005)_

- [ ] **A new index must serve a query Airflow itself issues.** Follow
      [`models/adr/0004`](../models/adr/0004-deployment-specific-indexes-are-documented-not-shipped.md),
      which is authoritative (this area's
      [`adr/0004`](adr/0004-metadata-database-indexes-are-a-deployment-choice-not-a-schema-change.md)
      points at it and adds the upgrade-window cost). Name the Airflow call site,
      or carry before/after `EXPLAIN ANALYZE` at a stated row count. Airflow ships
      no tooling to add or configure metadata indexes — a deployment's own index
      is applied by the operator, per the performance-tuning documentation.
- [ ] **Fix the query before adding an index** — a non-sargable predicate or an
      N+1 pattern is fixed where it is written; an index beside it hides the defect
      and keeps the cost permanently.
- [ ] **Don't widen a column to make an observed value fit.** Establish first why
      the value is stored there at all — sensitive, short-lived, or
      externally-controlled values get removed from the write path instead. A new
      length limit that is as arbitrary as the old one will be questioned.
- [ ] **Enumerate every consumer before dropping a table, column, or model** —
      including indirect ones outside the metadata DB (log-file path templates,
      remote log readers). "Only this one component uses it" is the assumption that
      gets these closed.
- [ ] **One field report is not a systemic defect.** Schema hardening against a
      failure seen once in years of a feature's life is declined; revisit if reports
      accumulate.
- [ ] **Check whether the direction is already agreed or already landed on
      `main`** — several migration PRs here were closed as superseded by work
      already merged, or by a decision already taken on the dev list. A competing
      _open_ PR is not a reason to close either one; Airflow allows parallel work
      and the better PR wins.

**Immutability & the release boundary (the defining constraint here):**

- [ ] Follow [`adr/0001`](adr/0001-a-released-migration-is-immutable-fix-forward.md) —
      a released revision is frozen and is corrected by a **new forward revision**;
      only an unreleased revision may be edited, dropped or retargeted; the chain
      keeps a single linear head.
- [ ] **The one exception: a released revision that _cannot complete_.** If its
      `upgrade()` raises or its `downgrade()` errors, no forward revision can
      reach the failure — repair it in place, make the repair idempotent for
      databases that already ran it cleanly, and say in the PR which failure it
      fixes (`#66016`, `#65288`, `#65688`). A revision that runs but writes wrong
      data is _not_ this case; that is fixed forward.
- [ ] **Spell out who _skips_ your fix if you edit an old revision.** The failure is
      silent and asymmetric: a deployment that already migrated past revision _X_
      never replays it, so it keeps the old (wrong) column shape forever, while a
      deployment migrating from further back picks the fix up.

**Cross-backend correctness & reversibility:**

- [ ] Follow [`adr/0002`](adr/0002-migrations-run-on-three-backends-and-must-be-reversible.md) —
      the revision runs on PostgreSQL, MySQL and SQLite (`batch_alter_table`,
      `db_types.py`, quoted reserved words, `disable_sqlite_fkeys` around DML),
      ships a real round-tripped `downgrade()`, and chains onto the single head.
- [ ] **A deliberately one-way data change is allowed if the docstring says so** —
      filling `NULL`s with defaults cannot be reversed; state that in the
      `downgrade()` docstring rather than pretending to restore it (see
      `0108_3_2_0_fix_migration_file_ORM_inconsistencies.py`).

**Data migrations at scale:**

- [ ] Follow [`adr/0003`](adr/0003-large-data-migrations-must-be-batched-and-stay-in-sync-with-models.md) —
      batch bulk changes with bounded transactions, prefer set-based SQL to
      per-row Python, and backfill a new non-nullable column so existing rows stay
      valid.

**ORM & serialization parity:**

- [ ] **The migration matches the ORM model** — a column added/altered/dropped in
      a revision must match the SQLAlchemy model; ORM-vs-migration drift is caught
      late and painful to reconcile.
- [ ] **Keep `get_serialized_fields()` / serialized shape in sync** when the schema
      change adds a field that must round-trip through serialization.
- [ ] **No ORM model imports inside migration scripts** — migrations pin the schema
      as it was at that revision; importing live models couples a frozen revision
      to code that keeps changing (enforced by the no-ORM-refs check).

**Tests, tooling, process:**

- [ ] **Test the migration both ways on a real DB** — exercise `upgrade` _and_
      `downgrade` (a round-trip), not just that the file imports; the anti-pattern
      AST checks and the FK-cascade round-trip CI check must pass, not be
      `# noqa`-suppressed.
- [ ] **Regenerate the migration reference** (`run_migration_reference.py`) so the
      docs/dependency graph stay in sync with the new head.
- [ ] **Keep the revision small and land it quickly — migration PRs rot faster than
      any other kind.** Every revision merged by someone else moves the head and puts
      your branch in conflict, so a PR that sits in review accumulates rebases until
      it is abandoned. Several PRs here died of exactly that, not of a review
      objection. Split the schema change away from unrelated refactoring, and rebase
      the moment a conflict appears rather than waiting for a reviewer.
- [ ] **Say what happens to existing serialized Dag data** when the revision touches
      the serialized-Dag / `DagVersion` contract, in **both** directions. Deleting
      serialized rows to make a migration simple loses real run history and has been
      rejected before; downgrades below the version that changed the contract need an
      explicit answer, not silence.
- [ ] **Show the backend(s) you tested on** — a migration "tested on SQLite only"
      is not tested.

> The closed-unmerged sample is dominated by three classes: proposed indexes
> without measurements, schema widenings that stood in for a root-cause fix, and
> revisions that simply rotted against a moving Alembic head. Mined from PR review
> and post-merge fix history; the sample skews to the
> Airflow-3 era (the deadline-alert, AIP-103 task/asset-store, and multi-team
> schema work drove most recent migrations), so older pre-3.0 conventions are
> under-represented. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR
that adds tables, reshapes a hot table, or runs a heavy data migration. The
cross-backend and reversibility invariants, and the fact that a released revision
can never be taken back, are best aligned on _before_ the code, not during review.
