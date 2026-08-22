---
triage_review_imbalance:
  area: scheduler-jobs
  criticality: high              # base tier; hot runners promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "scheduler_job_runner.py"
    - "triggerer_job_runner.py"
    - "dag_processor_job_runner.py"
    - "job.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "XD-DENG"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Jobs (scheduler / triggerer / Dag-processor runners) — Agent Instructions

This directory holds the long-running job runners — most importantly the
scheduler job — that drive Airflow's core control loop. It is one of the
highest-blast-radius areas in the codebase: a subtle mistake here can stall,
duplicate, or corrupt task execution across **every** deployment, and the
failure modes are concurrency-dependent and hard to reproduce in review.

## Why changes here are expensive to review

- The scheduler loop mutates shared state under **row-level locking**; correct
  changes must preserve the locking and commit boundaries or risk deadlocks and
  lost updates under real concurrency.
- The **DagRun → TaskInstance state machine** has invariants that are not
  locally obvious from a diff — a change that looks correct in isolation can
  violate them.
- Behaviour is timing- and load-dependent, so tests rarely surface a regression
  that only appears at production scale.

## Knowledge a reviewer (and a substantial contributor) needs

- The scheduler main loop, `SchedulerJobRunner`, and how it batches DB work.
- Row-locking / `with_row_locks` semantics and the "no `session.commit()` in
  functions that take a `session`" rule.
- The DagRun/TI state transitions and the executor hand-off boundary
  (the scheduler **never** runs user code).
- Why bulk writes in the loop must be batched with `LIMIT` (see the db-cleanup
  batching pattern referenced in the repo `CLAUDE.md`).

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area.** If you are an agent
preparing a change here on behalf of a person, first judge whether the change can
be **demonstrated** rather than argued: can you run a scheduler against a real
metadata DB, reproduce the scheduling behaviour you claim is wrong, and show the
loop still behaves under _concurrent_ schedulers — the case where locking and
batching bugs actually appear? Reasoning about the diff is not enough here; the
questions reviewers ask ("what happens when two schedulers hit this row at once?",
"how long does this transaction hold the lock?") are answered by running it.
**If you cannot demonstrate it, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## How the ADRs in this area are meant to be used

The five ADRs under `adr/` are **review guidance, not a mechanical gate.** A
validation pass over the open queue found they fire on almost nothing — and the
one firing they did produce was a false positive. That is the expected result,
and it has deliberately not been "fixed" by adding rules that would fire more
often.

The reason is that the real cost of a change here is concurrency reasoning that a
diff does not show: whether a lock is still held where it must be, whether a
commit boundary moved, whether an interleaving that only appears under production
load is still safe. None of that is visible as a pattern a rule can match, so a
rule tuned to fire more would mostly fire wrongly. Read these ADRs as the
questions to carry into the review, and expect the judgement to come from the
reviewer rather than from a bullet matching.

## Review criteria

Mined from real review discussion on ~160 merged and ~138 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back
to its author with the specific gaps. Ordered by how often reviewers raise each.

**Before you start — is this the right change, at the right layer?**

- [ ] Fix the **root cause, not a workaround** — retrying, catching, or skipping
      past a deadlock/race is not an acceptable fix. For lock contention, fix the
      lock ordering / use `FOR UPDATE SKIP LOCKED` with deterministic PK order so
      the locks are _avoided_, not reacted to.
- [ ] **Prove the diagnosis on a live setup first** — the scheduler is a vital
      hot path; substantial changes need a real repro and logs/screenshots/
      benchmarks. Many rejected PRs "fixed" a misdiagnosed config issue or benign
      log output. **If you cannot stand up that setup**, do not open the PR
      anyway: report the observation instead (what you saw, on which version,
      with which configuration, and what you already ruled out) and say
      explicitly that you could not reproduce it. An observation someone else can
      reproduce is worth more than a mechanism nobody can check — see
      `../dag_processing/adr/0004` for why this is the one case where an issue,
      not a PR, is the right artefact.
- [ ] **Don't add scheduler knobs, complexity, or deployment-specific
      mitigations** when existing controls (`max_active_runs`, ordering) already
      apply — tuning the scheduler is already hard; every deployment pays for
      added per-loop cost.
- [ ] **No user code in the scheduler hot loop**, and don't make the triggerer do
      the scheduler's job. A conceptual shift in how scheduling works needs an
      AIP / devlist discussion, not a bare PR.

**Concurrency & DB correctness (the highest-cost review dimension here):**

- [ ] Reason explicitly about **HA multi-scheduler races**: two schedulers must
      not grab the same rows — claim disjoint work with
      `with_for_update(skip_locked=True)` + a deterministic `order_by` tiebreaker,
      and enforce caps atomically (a bare read-then-write `count==0` check
      double-dispatches).
- [ ] **No orphaned state if the process dies mid-transaction** — order commits so
      a crash between commit and `apply_async()` can't leave a task stuck in
      queued/running.
- [ ] **Batch bulk DELETE/UPDATE with `LIMIT`, commit between batches, index the
      filter columns** — never an unbounded bulk write in the loop (holds row
      locks, stalls the main loop). No `session.commit()` inside a function that
      takes a `session`.
- [ ] **One query, not N+1** — push filtering/counting/ordering into SQL; no
      per-row/per-asset query or extra round-trip in the loop (use the batched
      `get_latest_serialized_dags(...)`-style helpers, `order_by(...)[0]` instead
      of a second `max()` query).
- [ ] **Respect executor slot capacity, pools, and concurrency limits** — never
      oversubscribe executors; route executor lookup through the single canonical
      path (`_try_to_load_executor()`), don't duplicate it.
- [ ] **A rewrite of the task-selection query must be proven on MySQL and against
      starvation.** A strategy that reads well on PostgreSQL can be an order of
      magnitude slower on MySQL, and any approach whose fairness depends on the
      query planner will starve some pool/priority combination — enumerate the
      orthogonal limits (pool slots, per-Dag concurrency, priority) and show the
      starvation cases you tested.
- [ ] **Don't add an index to a core table for one deployment's query shape** —
      deployment-specific indexes are documented for operators to apply, not
      shipped (see `../models/adr/0004`); bring before/after timings at a stated
      row count if you believe an Airflow query needs one.

**Boundaries (these are architecture invariants, not preferences):**

- [ ] **The user-code side of the triggerer and the DFP must not gain server
      DB/secret access** — the DFP parse subprocess and the triggerer's async
      **runner subprocess** run `_AIRFLOW_PROCESS_CONTEXT=client` and reach state
      through the Execution API. The `TriggererJobRunner` **supervisor** is
      server-side and holds ORM sessions on purpose; don't "fix" that.
- [ ] **The async trigger runner holds no ORM session and loads no Dag-bundle
      code** (see `adr/0005`) — no `@provide_session` or model query reachable
      from a running trigger (the runtime raises), and no bundle initialization,
      bundle-root `sys.path` injection, or archive loading for trigger imports.
      Triggers come from `sys.path`; state comes from the Execution API.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes so DB errors reach `@retry_db_transaction` and refactor bugs
      surface. `suppress(BaseException)` is too broad (eats `SystemExit`/`KeyboardInterrupt`).
- [ ] **Imports at module top**; local imports only for genuine circular-import /
      back-compat reasons (and say why). **No heavy work at import time** (provider
      discovery, file I/O, `cached_property` that triggers discovery).
- [ ] **Action-verb / intent-revealing names**; no shadowing builtins (`id`,
      `print`); underscore-prefix non-public members; branch on a
      property/method, not `isinstance`.
- [ ] **No duplicated logic** — reuse/generalize existing helpers; three copies of
      a derivation _will_ drift.
- [ ] **Explicit resource cleanup** (`close()` at the call site, guard double-close)
      rather than `__del__`/GC-dependent teardown.
- [ ] **Right severity** — user-configured conditions warn/degrade, they don't
      crash the scheduler; translate domain errors to the correct HTTP status at
      API boundaries (don't let a DB error surface as a 500).

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new path** — for scheduler work that means going
      through real serialize/deserialize, not constructing the task in-process and
      skipping it.
- [ ] **Backward compatibility** for public/semi-public interfaces — check whether
      it shipped in a release (can't ret-con a released migration — add a new one),
      version-gate, add a compat shim, and keep `get_serialized_fields()` in sync
      so new fields survive serialization.
- [ ] **Config hygiene** — scheduler-only knobs go in `[scheduler]`, consistent
      units, correct `version_added`.
- [ ] **Show evidence of testing** — low-effort / near-duplicate parallel PRs get
      closed here in particular, because the review cost is paid by the few people
      who can review this area. Take contentious semantics to the devlist or a
      second reviewer.

> Mined from PR review history; the sample skews to roughly the last year, so
> older scheduler-internals conventions are under-represented. Extend as new
> patterns emerge, and add an equivalent `## Review criteria` section to the
> `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large
PR. The concurrency and state-machine invariants are best aligned on _before_
the code, not during review.
