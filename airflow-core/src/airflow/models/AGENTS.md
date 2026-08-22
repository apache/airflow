---
triage_review_imbalance:
  area: models
  criticality: high              # base tier; core state/ORM files promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # the state machine + ORM/task-expansion core — treated as criticality=critical
    - "taskinstance.py"
    - "dagrun.py"
    - "dag.py"
    - "dagbag.py"
    - "backfill.py"
    - "baseoperator.py"
    - "mappedoperator.py"
    - "xcom.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["XD-DENG", "ashb", "uranusjr"]   # internal signal only — never @-mentioned; uranusjr owns the operator files
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Models (SQLAlchemy ORM + core state) — Agent Instructions

The metadata-DB models: `TaskInstance`, `DagRun`, `DagModel`, `Backfill`,
`XCom`, the operator classes, and the rest of Airflow's persistent state. This
is the source of truth the scheduler and API act on; the **state-machine and
ORM-core files** (see `structural_risk_paths`) are among the highest-stakes in
the codebase — a wrong state transition or a broken query invariant corrupts
runs across every deployment and is hard to catch in review.

## Why changes here are expensive to review

- The `DagRun` / `TaskInstance` **state machine** has invariants that a diff
  doesn't make locally obvious; a locally-correct change can violate them.
- Model changes ripple into **serialization**, **migrations**, and the
  **scheduler** — a new/changed column usually needs all three kept in sync.
- Query changes interact with **row-locking and concurrency** under the
  scheduler's real load.

## Knowledge a reviewer (and a substantial contributor) needs

- The TI/DagRun state transitions and where they are enforced.
- The ORM session rules (no `session.commit()` in a function taking a
  `session`; keyword-only `session`).
- How a model field reaches serialization (`get_serialized_fields()`) and when
  a schema change needs a DB migration.
- Task expansion / mapped-operator model (`baseoperator.py`,
  `mappedoperator.py`, `expandinput.py`).

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area.** If you are an agent
preparing a change here on behalf of a person, first judge whether the change can
be **demonstrated on a live metadata DB**: have you run real Dag runs through the
state transitions you touched and shown the invariants still hold, and — if you
changed a field — that it still reaches serialization and that any schema change
has its migration? Reading the model class does not tell you what the state
machine does when a run is already half-way through it.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~228 merged and ~233 closed-unmerged PRs
touching `models/` — the changes reviewers repeatedly required, and the reasons
changes here get closed. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back
with the specific gaps. Ordered by how often reviewers raise each.

**Database correctness (the highest-frequency review dimension):**

- [ ] **No N+1 or redundant queries** — batch and eager-load in loops (set-based
      helpers like `get_latest_serialized_dags`, `joinedload`/`bulk_fetch`); don't
      re-run a query a caller already did.
- [ ] **Session discipline** — thread the active session through (keyword-only
      `session`); never open a fresh session mid-operation; prefer
      `session.scalar(select(...))`. No `session.commit()` in a function taking a
      `session`.
- [ ] **A schema change carries its migration** — the migration rules themselves
      (immutability of released revisions, three-backend reversibility, batching)
      live in `../migrations/AGENTS.md` and its ADRs; don't restate them, meet them.
- [ ] **Deadlock fixes establish deterministic lock ordering** (lock `DagRun`
      before `task_instance`, `SKIP_LOCKED`) and scope bulk writes to the owning
      scheduler (`TI.queued_by_job_id == self.job.id`) — retry/skip is not a fix.
- [ ] **Don't ship an index for a deployment-specific query** — see `adr/0004` for
      the boundary (an index serving a query Airflow itself issues is in scope; one
      serving a deployment's own query shape is documented, not merged) and for what
      an index PR has to bring.
- [ ] **A model the triggerer or Dag processor must read needs an Execution API
      surface, not a session** — `@provide_session` reached from a running
      trigger raises _"Direct database access via the ORM is not allowed"_ at
      runtime. Settle the endpoint before building the model.
- [ ] **Get a migration PR reviewed promptly** — a branch carrying an Alembic
      revision re-conflicts every time another migration merges, so an aging
      migration PR dies of rebasing rather than of review.

**Serialization & compatibility:**

- [ ] **Serialization round-trips and stays deterministic** — test the _full_
      serialize→deserialize→hash path (not just a helper), and the test must fail
      without the change.
- [ ] **Preserve rolling-upgrade & public-SDK backward compat** — keep compat
      fallbacks read-only (`.get()` not `.pop()`, removable N+1); a keyword-only
      signature change against a released `task-sdk` is breaking → call it out +
      newsfragment.
- [ ] **ORM type hints match actual column nullability** (`Mapped[str]` for
      `nullable=False`, drop the `or ''`); don't let `.get(k, default)` silently
      mask schema drift — let the `KeyError` surface.
- [ ] **Keep core↔SDK sync hooks green** (e.g.
      `check-partition-mapper-defaults-in-sync`) — declare the field in the class
      body rather than hollowing it to appease the hook; keep behaviour consistent
      across scheduler/API/UI code paths (same ordering on both sides).

**Error handling & exceptions:**

- [ ] **Don't swallow errors** — narrow `except` to the real classes so unrelated
      bugs surface; `log.warning` inside any `suppress`; fail loud on malformed
      serialized data rather than substituting a default that masks corruption.
- [ ] **Specific exceptions, not broad `AirflowException`**; keep **HTTP
      semantics at the route boundary**, not in domain/model methods (raise a
      dedicated/built-in exception here, translate to 400/404 in the route).
- [ ] **Preserve exception chaining (`from e`)** and delete dead/unreachable
      `except` branches.

**Design & scope:**

- [ ] **One source of truth** — extract a shared helper/base class rather than
      copy-pasting derivation/validation across layers (it drifts).
- [ ] **Keep the diff surgical** — revert unrelated churn (`uv.lock`, config
      templates); split a backportable bug fix from new features (one PR per
      concern).
- [ ] **Behaviour/validation changes to existing data need a migration or opt-in
      path** — don't break users whose deployed Dags/names suddenly fail on
      upgrade.
- [ ] **Don't "harden" paths the security model already treats as trusted** (the
      triggerer / DFP run user code by design — not a vulnerability).
- [ ] **Check for an existing/better fix first** — the most common rejection is
      being superseded by a PR that targets the real root cause; verify CI is
      green (mypy + the Serialization suite) before marking ready.
- [ ] **A new field, state, exception, or listener hook needs an agreed design and
      must not duplicate existing semantics — see `adr/0005`**, which says what to
      name in the PR body and what goes to the dev list or an AIP first.
- [ ] **The metadata DB is not a general-purpose store** — credentials and
      unbounded or opaque blobs don't get a column here; fix the layer that
      produced them.
- [ ] **Reproduce the defect on current `main` before fixing it** — a report filed
      against a released version is a starting point, not evidence, and several PRs
      here were closed after a reviewer showed `main` already handles the case. **If
      you cannot reproduce on `main`**, that is itself the finding: say so in the PR
      body (what you ran, on which revision, and what happened instead) rather than
      shipping the fix on the assumption it still applies. A PR that says "could not
      reproduce on `main`, opening for the reporter's original case" is reviewable;
      one that silently assumes is not.

> Mined from PR review history; the sample is weighted toward recent
> serialization/migration/asset-partition work, so XCom/connection-model
> conventions are under-represented. Extend as new patterns emerge, and add an
> equivalent `## Review criteria` section to the `AGENTS.md` of every other area
> over time.

## Expectation for large changes

Discuss the approach first — state-machine and schema changes are best aligned
on before the code, not during review.
