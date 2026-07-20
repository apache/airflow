---
triage_review_imbalance:
  area: ti-deps
  criticality: high              # base tier; the scheduler-hot dep framework + trigger-rule dep promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "deps/"
    - "dep_context.py"
    - "deps/trigger_rule_dep.py"
    - "dependencies_deps.py"
  codeowners_ref: ".github/CODEOWNERS"
  # No dedicated CODEOWNERS line covers ti_deps/ — it inherits the jobs/scheduler
  # owners (/airflow-core/src/airflow/jobs/ @ashb @XD-DENG), since these deps are
  # evaluated inside the scheduler loop. `experts` therefore combines those owners
  # with the dominant recent ti_deps author (git log author histogram).
  experts: ["ashb", "XD-DENG", "uranusjr"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Task-instance dependencies (ti_deps) — Agent Instructions

This directory holds the task-instance dependency rules — each a `BaseTIDep`
subclass in `deps/` — that the scheduler evaluates to decide whether a task
instance may be queued or run. A `DepContext` carries the flags and finished-TI
state a check may consult; each dep yields `TIDepStatus(dep_name, passed,
reason)` records, and a task advances only when **every** dep in the relevant
set (`RUNNING_DEPS`, `SCHEDULER_QUEUED_DEPS`, `REQUEUEABLE_DEPS`) passes. These
predicates run per task instance inside the scheduler's hot loop, from
serialized and DB state — so a defect here can wrongly hold back or wrongly
release tasks across **every** deployment, or slow the whole scheduling loop.

## Why changes here are expensive to review

- A dep is evaluated **inside the scheduler control loop**, once per candidate
  task instance. A check that adds an extra query or an unbatched per-upstream
  lookup multiplies across the whole cluster's tasks and slows scheduling for
  everyone — cost that is not visible from a diff that "just adds one check."
- The **met / not-met semantics** are a state-machine contract: `DepContext`
  flags (`ignore_all_deps`, `ignore_task_deps`, `ignore_ti_state`,
  `ignore_depends_on_past`, the reschedule/retry-period ignores) and the
  `IGNORABLE` / `IS_TASK_DEP` class markers decide when a dep is skipped. A
  change that flips one of these can silently release tasks that should wait, or
  strand tasks that are actually ready.
- Several deps (trigger rules, mapped-task expansion, `depends_on_past`,
  teardown scope) encode invariants that are **not locally obvious** — the
  correct answer depends on upstream/prior-run state that the diff does not
  show, and the failure mode (a stuck or double-dispatched task) surfaces only
  at scale.

## Knowledge a reviewer (and a substantial contributor) needs

- The `BaseTIDep` contract: `_get_dep_statuses()` yields `TIDepStatus` records;
  `get_dep_statuses()` wraps it and applies the `IGNORABLE` / `IS_TASK_DEP`
  short-circuits against the `DepContext`; `_passing_status` / `_failing_status`
  build the results. New deps integrate _through_ this contract.
- `DepContext` and its flags — what each `ignore_*` means, that it is meant to
  be **side-effect-free** (the `flag_upstream_failed` mutation is a documented
  wart, not a licence to add more), and how `finished_tis` is populated once.
- The dep **sets** in `dependencies_deps.py` (`RUNNING_DEPS`,
  `SCHEDULER_QUEUED_DEPS`, `REQUEUEABLE_DEPS`) and which context evaluates which.
- That these checks run in the scheduler, which **never runs user code** (see
  `../../jobs/adr/0001`) — deps read serialized/DB data only, they do not import
  author modules or evaluate user-supplied predicates.
- The repo `CLAUDE.md` DB rules — keyword-only `session`, no `session.commit()`
  inside a function that takes a `session`, push counting/filtering into batched
  SQL (as `trigger_rule_dep.py` does with `func.count(...).group_by(...)`).

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area that sits directly on the
scheduler hot path.** If you are an agent preparing a change here on behalf of a
person, first judge whether the change can be **demonstrated as a scheduling
outcome**: can you build the Dag shape that exercises the dep, run it, and show
which task instances become runnable before and after — including the cases the
dep is supposed to _block_? A dep evaluates in every scheduler loop for every
candidate task instance, so "it returns the right status for my example" also has
to mean "it costs no extra query per row".
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on the ~46 merged and ~53 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed.
The sample is smaller than the scheduler/DFP areas, so treat the ordering as
indicative rather than exhaustive. **If you are preparing a change here, treat
this as a pre-flight checklist and fix every applicable item _before_ opening the
PR.** Triage applies the same list: a PR that lands with unmet items is drafted
back to its author with the specific gaps. Ordered by how often reviewers raise
each.

**Met / not-met semantics & the dep contract (the defining concern here):**

- [ ] **Integrate through `BaseTIDep`, don't bypass it** — a new dependency is a
      `BaseTIDep` subclass that yields `TIDepStatus` via `_get_dep_statuses()`
      with a clear `reason`; it does not reach around the framework to set state
      or short-circuit scheduling elsewhere.
- [ ] **Set `IGNORABLE` / `IS_TASK_DEP` deliberately** — a task-specific dep
      (trigger rule, `depends_on_past`) is `IS_TASK_DEP`; a dep that must _never_
      be skipped stays non-ignorable. Getting these wrong silently changes when
      the dep is honoured under `ignore_*` contexts (backfill, clear, retry).
- [ ] **Don't silently flip met/not-met** — a change to a trigger-rule count, a
      skip rule, or a reason string must preserve the passing condition for cases
      it doesn't intend to change; reasons are user-visible and must state _why_
      a task is held.
- [ ] **Add a new dep to the right set(s)** in `dependencies_deps.py`
      (`RUNNING_DEPS` / `SCHEDULER_QUEUED_DEPS` / `REQUEUEABLE_DEPS`) — a dep that
      exists but is in no set is never enforced; one in the wrong set gates the
      wrong transition.

**Mapped tasks — the highest-failure seam here (from the closed-PR record):**

- [ ] **Evaluate against the matching map index, not the aggregate.** Inside a
      mapped task group, an instance's upstreams are the upstream instances at
      _its own_ index. Folding all indexes into a counted state is the single
      most common defect in this area, and the most common reason an attempted
      fix is closed (see `adr/0004`).
- [ ] **A fix in this seam ships with a test that reproduces the failure.**
      Untested attempts at mapped trigger-rule, skip, and deadlock bugs are
      closed rather than merged on inspection — review cannot distinguish a real
      fix from a plausible one without the reproduction. At least one closed PR
      here died precisely on an unanswered request for a test case.
- [ ] **State the behaviour when upstream expansion count differs** from the
      downstream one, and when an upstream mapped instance has been removed — a
      rule that counts upstreams without accounting for removal produces a
      permanently unsatisfiable condition.
- [ ] **Skip propagation is per index too** — branch and short-circuit operators
      upstream of a mapped group skip matching indexes, not the whole group.
- [ ] **Check for an in-flight fix before starting.** This defect class attracts
      several independent simultaneous attempts; prefer building on the existing
      PR.

**Fixing `TriggerRuleDep` — the single most common wrong-location mistake:**

- [ ] **Find the dep that owns the broken assumption before adding a guard.**
      `TriggerRuleDep` assumes the instance is already expanded and its upstreams
      resolve at its own index. When that assumption fails, the fix is the dep
      responsible for establishing it (`MappedTaskIsExpanded`) or its membership
      in `dependencies_deps.py` — not a compensating branch in
      `trigger_rule_dep.py`. #32701 had the diagnosis right and was closed on the
      location. See `adr/0006`.
- [ ] **Never trade the early decision for a wait.** Fast fail and fast skip are
      contract, not optimisation: a downstream waits only until the rule's answer
      is determined, not until every upstream reaches a terminal state. #34138 was
      refused for exactly this substitution.
- [ ] **Say what your counting change does to the deadlock cases.** The current
      shape of the upstream counting encodes fixes for permanently-unsatisfiable
      conditions. A change must state the behaviour for removed, unexpanded, and
      absent upstreams, and reviewers have required reproducing the original
      deadlock before extending it (#33915).
- [ ] **State the effect on the paths you did not target.** This dep runs for
      every task instance — a mapped-task-group fix says what it does to
      non-mapped tasks, setup/teardown, and plain task groups.
- [ ] **Expect a better-placed competing fix to win.** Several attempts here were
      closed in favour of an alternative that fixed the same defect at the owning
      dep (#33570 → #33903, #32701, #33820).

**New behaviour goes through existing deps and existing state:**

- [ ] **Derive from state the system already records** before adding a field.
      A proposal to keep a Dag paused during backfill had its new flag removed in
      review in favour of `DagRun.backfill_id` plus the existing unpause path
      (see `adr/0005`).
- [ ] **A new user-facing dependency parameter goes to the devlist first.** Two
      independent attempts at a `depends_on_previous_task_ids`-style cross-run
      parameter were closed; the discussion was about whether the gap over
      `depends_on_past` justifies a second concept, not about the code.
- [ ] **Reshaping what an existing construct means** — task groups, trigger
      rules, run identity — is discussed before the code. A task-group-retries
      proof of concept was closed as needing a dev-list thread first.
- [ ] **The dep set is not a plugin point.** A proposal to let users register
      custom `TIDep` classes was closed (#37778): it hands control to user code in
      the scheduler's hottest path, and review's position was that it would need
      documented use cases, performance guidance for dep authors, a way to detect
      misbehaving deps, and an AIP or design document stating the end goal — the
      cost is the mechanism, not the diff.

**Scheduler-loop cost (paid per task instance, every cycle):**

- [ ] **No N+1 / per-upstream query in a dep** — push counting and filtering into
      one batched SQL statement (the trigger-rule dep aggregates with
      `select(..., func.count(...)).group_by(...)`), not a Python loop that
      queries per upstream / per map-index.
- [ ] **Reuse `DepContext.finished_tis`** rather than re-fetching finished task
      instances inside a dep; prefer lazy iteration and early return so a cheap
      failing case short-circuits before expensive work.
- [ ] **Keep the dep cheap and bounded** — it runs in the latency-sensitive
      scheduling loop; new per-cycle work is paid across the whole cluster, so a
      substantial cost increase needs justification (and ideally a benchmark).

**Purity & DB correctness:**

- [ ] **Deps are side-effect-free** — evaluating a dependency must not mutate
      persistent state as a shortcut. `flag_upstream_failed` is a documented,
      contained exception; do not add new state mutations, and don't let
      `DepContext` leak mutations between evaluations.
- [ ] **Keyword-only `session`; no `session.commit()`** inside a function that
      takes a `session` (positional-session slips have been a recurring catch in
      this exact directory).
- [ ] **No user code, no import of author modules** — deps run in the scheduler,
      which must never evaluate a user-supplied predicate or custom `TIDep`
      class; read serialized/DB data only (see `../../jobs/adr/0001`).

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes so DB errors reach retry logic and refactor bugs surface.
- [ ] **Imports at module top**; local imports only for genuine circular-import
      reasons (and say why). **No heavy work at import time.**
- [ ] **Action-verb / intent-revealing names**; no shadowing builtins; reuse
      existing helpers rather than a third copy of a state-derivation that will
      drift.
- [ ] **Right severity** — a dep reports _not met_ with a reason; it degrades the
      task's readiness, it does not crash the scheduler.

**Tests, compatibility, process:**

- [ ] Test **exercises the real dep through the framework and fails without the
      change** — construct the `TaskInstance` / `DagRun` state and assert on
      `get_dep_statuses()` / `is_met()` output (passed + reason), not a helper
      called in isolation; mocks use `spec`/`autospec`; assert on structured
      `caplog`, not substrings; use `@pytest.mark.parametrize` for the
      trigger-rule / state matrices.
- [ ] **Backward compatibility** for dep behaviour that deployments rely on — a
      change to when a task becomes runnable is user-visible; version-gate and
      call it out.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (a new trigger rule or a changed runnable condition), not internal refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of
      testing — low-effort / mass-AI-generated / near-duplicate parallel PRs get
      closed. Track deferred work in a GitHub issue; take contentious semantics
      (a new trigger rule, a changed skip rule) to the devlist / a second reviewer.

> Mined from PR review history. The merged sample skews to the Airflow-3 era (the
> SDK/serialization split reshaped how deps read Dag structure); the
> closed-unmerged sample reaches back to 2022, and the `TriggerRuleDep` guidance
> above comes largely from it — those rejections predate the move to
> `airflow-core/src/`, but the deps and the fast-decision behaviour they concern
> are unchanged. The frequency ordering is approximate. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
A new dependency, a changed trigger rule, or any shift in met / not-met
semantics is best aligned on _before_ the code, because it changes when tasks
run across every deployment and is hard to unwind once released.
