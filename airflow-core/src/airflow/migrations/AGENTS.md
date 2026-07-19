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
first judge whether the **driving person** has the experience this area demands —
the knowledge above, plus a track record of contributing to or reviewing schema
migrations. **If they do not, do not create the PR.** Say so plainly and redirect
them to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** that fits their current competences, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large or careless change here wastes scarce maintainer review time and, if it
slips through into a release, cannot be taken back — it can only be patched
forward. Building standing first is faster for everyone.

## Review criteria

Mined from real review discussion and the post-merge fix history on this area —
the changes reviewers repeatedly required, and the reasons changes here get
reworked or reverted. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back to
its author with the specific gaps. Ordered by how often reviewers raise each.

**Immutability & the release boundary (the defining constraint here):**

- [ ] **Never edit a migration that has shipped in a released version** — released
      revisions are frozen history; a user who already ran revision _X_ will never
      re-run it. Correct a released migration with a _new_ forward revision, not by
      rewriting it. Only an **unreleased** revision may be edited or dropped.
- [ ] **Keep a single linear head.** The new revision's `down_revision` points at
      the current head and becomes the new head; `alembic heads` must return
      exactly one. Rebase your revision onto the latest head rather than forking a
      second head that then needs an `alembic merge`.
- [ ] **Don't retro-fit a fix by mutating chain metadata carelessly** — moving a
      change to a different target version ("retcon") is occasionally legitimate
      _only_ while unreleased and must keep the chain linear and consistent.

**Cross-backend correctness & reversibility:**

- [ ] **Provide and test a real `downgrade()`** that restores the prior schema and
      data shape — not a stub. Operators downgrade; a missing or lossy downgrade is
      a defect, not an optimization.
- [ ] **Works on PostgreSQL, MySQL _and_ SQLite.** No engine-specific DDL that
      breaks another backend. Use `op.batch_alter_table(...)` for alter/drop/
      constraint changes (SQLite cannot `ALTER` in place); route type choices
      through `db_types.py`; quote MySQL-reserved keywords.
- [ ] **Handle SQLite foreign keys correctly** — run DML _inside_
      `disable_sqlite_fkeys(op)` where needed and mind that SQLite enforces FKs
      during batch table recreation; missing or mis-ordered FK handling is a
      recurring downgrade break.
- [ ] **Mind backend-specific limits and objects** — MySQL row/sort-buffer limits
      and reserved words, Postgres sequences/autoincrement restored on downgrade,
      JSONB conversions guarded against invalid input.

**Data migrations at scale:**

- [ ] **Batch bulk data changes** — no unbounded one-shot `UPDATE` on a large user
      table, no per-row Python loop where set-based SQL will do. Bound the work,
      commit between batches, and prefer SQL over deserialize-in-Python passes.
- [ ] **Backfill required columns and keep transactions bounded** — a new
      non-nullable column needs a default/backfill so existing rows stay valid;
      don't hold one giant transaction across the whole table.

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
- [ ] **Newsfragment only for genuinely user-facing** schema changes, not internal
      corrective revisions.
- [ ] **Follow the PR template**, disclose AI assistance, and show the backend(s)
      you tested on — a migration "tested on SQLite only" is not tested. Track
      deferred work in a GitHub issue; take contentious schema changes to the
      devlist / a second reviewer.

> Mined from PR review and post-merge fix history; the sample skews to the
> Airflow-3 era (the deadline-alert, AIP-103 task/asset-store, and multi-team
> schema work drove most recent migrations), so older pre-3.0 conventions are
> under-represented. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR
that adds tables, reshapes a hot table, or runs a heavy data migration. The
cross-backend and reversibility invariants, and the fact that a released revision
can never be taken back, are best aligned on _before_ the code, not during review.
