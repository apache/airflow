---
triage_review_imbalance:
  area: executors
  criticality: high              # base tier; the executor interface + loader promoted to `critical`
  review_difficulty: expert
  structural_risk_paths:         # the executor interface providers depend on, and the loader
    - "base_executor.py"
    - "executor_loader.py"
    - "executor_constants.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["XD-DENG", "ashb", "o-nikolas", "pierrejeambrun", "hussein-awala", "dheerajturaga"]   # internal only — never @-mentioned
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Executors — Agent Instructions

The core executor interface and the built-in executors. `base_executor.py`
defines the contract that **every provider executor** (Celery, Kubernetes,
Edge, …) implements, so a change to that interface or to `executor_loader.py`
ripples out of core into all of them. Slot accounting, task adoption, and the
scheduler hand-off live here — mistakes oversubscribe workers or strand tasks.

## Why changes here are expensive to review

- `base_executor.py` is a **provider-facing interface**; changing it can break
  out-of-tree and in-tree provider executors.
- Slot/parallelism accounting must stay correct — the scheduler relies on it to
  not oversubscribe executors.
- Task **adoption** and event-buffer handling interact with the scheduler loop
  and crash-recovery.

## Knowledge a reviewer (and a substantial contributor) needs

- The `BaseExecutor` contract and which methods providers override.
- `executor_loader.py` and the single canonical executor-lookup path.
- Multi-executor support and how team/executor routing works.
- Slot accounting, `_try_to_load_executor`, and the scheduler hand-off.

## Before opening a PR here — authoring-agent guard

**This is a provider-facing, expensive-to-review interface area.** If you are an
agent preparing a change here on behalf of a person, first judge whether the
change can be **demonstrated against real executors**: have you run tasks through
more than one executor implementation — including at least one provider executor
that subclasses `BaseExecutor` — and shown slot accounting and the scheduler
hand-off still hold? This is an interface other distributions implement, so a
change that only works for the executor you happened to test breaks providers
that are not in this repo.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~90 merged and 40 closed-unmerged
executor PRs. Provider executors (Celery/K8s/ECS/Batch/Lambda/Edge) inherit
`BaseExecutor` and **mix versions with core** — so interface stability is the
dominant, repeated review concern. **If you are preparing a change here, treat
this as a pre-flight checklist and fix every applicable item _before_ opening
the PR.** Triage applies the same list: a PR that lands with unmet items is
drafted back with the specific gaps. Ordered by how often reviewers raise each.

**Interface stability & back-compat (the #1 review concern here):**

- [ ] **Never rename/remove `BaseExecutor` public fields or method
      signatures.** Latest provider must work on older core and vice-versa; use
      `@property` shims or additive-only changes. Renaming to "unify" is an
      instant rejection.
- [ ] **Gate new behaviour behind explicit `AIRFLOW_V_3_x_PLUS` version
      guards**, not bare `try/except` — so it's obvious when the shim can be
      removed at the next min-version bump. Match the version in code and
      comment exactly.
- [ ] **Follow the established ECS/Celery back-compat pattern** for new
      executor/callback support (version-guarded `TypeAlias`, `queue_workload`
      fallback, `supports_callbacks`) — reviewers point new PRs at those.
- [ ] **Deprecate renamed entrypoints** (warn + alias) rather than breaking
      them while providers migrate.

**Scheduler correctness:**

- [ ] **Don't over-subscribe slots** — any new workload type does slot/capacity
      accounting across all executors and teams (`slots_available`); honour
      `pools` / `max_active_tasks_per_dag`, not just `parallelism`.
- [ ] **Use the single canonical `_try_to_load_executor()`** for executor
      lookups — never duplicate loader logic (duplicates drift and miss team +
      class-name-match support).
- [ ] **Multi-executor/team routing is per-target-executor (per-TI)**, not a
      scheduler-wide `any(...)` gate — heterogeneous deployments are supported.
- [ ] **Preserve callback/executor state transitions the scheduler relies on**
      (don't collapse QUEUED→SUCCESS/FAILED and orphan the `RUNNING` handler).

**Generalize, don't copy-paste:**

- [ ] Put generic per-executor logic in **`BaseExecutor`** (inherit) — a feature
      that forces touching all 4-5 executor implementations gets pushback.

**Don't hang unrelated subsystems off the executor:**

- [ ] **`BaseExecutor` and `ExecutorLoader` are not a plugin bus.** A subsystem
      that happens to be per-deployment or per-provider — DB managers, migration
      hooks, auth wiring — does not get a hook on the executor interface or
      auto-discovery through the loader. It goes through its own seam (a CLI
      command group, a provider entry point). Every executor implementation
      would otherwise inherit a method it has no reason to have.
- [ ] **Keep the loader narrow.** Executor lookup resolves an executor and
      nothing else; validation of teams, bundles, or other state does not belong
      in loader or parser paths that run on every start-up.
- [ ] **Executors never load Dag-bundle code** (see `adr/0005`). A callback or
      executor helper must be importable from `sys.path` outside any bundle;
      bundle identity is threaded to the worker as data (`BundleInfo`,
      `version_data`) and resolved there. The equivalent rule for the triggerer
      is owned by `../jobs/adr/0005` — a triggerer bundle-loading PR is judged
      there, not here.
- [ ] **Fix the leak where the concern lives.** A secret surfacing in task logs
      is fixed in the masker or by keeping the field out of the workload schema
      — not by filtering the executor's log path, which fixes one route out of
      several.

**Payload, security, config:**

- [ ] **Keep the serialized workload payload minimal and size-aware** — it
      travels through JWT, K8s/ECS/Batch/Lambda argv (`ARG_MAX` ~128 KB) and
      Celery/SQS bodies (256 KB). Don't ship the whole serialized Dag if one
      task suffices; keep executor-side fields (`priority_weight`, `pool_slots`)
      out of the schema.
- [ ] **No secrets in logs/results/`__repr__`** — use `type(e).__name__`, not
      raw exception text (DB drivers leak connection strings); `Field(repr=False)`
      on token-bearing schemas.
- [ ] **Config knobs placed consistently** (`[scheduler]` for scheduler-only)
      and documented accurately — don't claim a global cap that HA schedulers
      make per-scheduler.

**Tests & process:**

- [ ] Deadlock/row-lock/perf fixes need a **before/after repro under concurrent
      load** + a regression/e2e test; graceful escalation (SIGTERM→SIGKILL).
      **If you cannot generate that load**, say so in the PR body and give what you
      do have — the executor's own logs, the state transitions you observed, the
      configuration — rather than presenting inspection as measurement. A stated
      gap is reviewable; an unstated one wastes the review.
- [ ] Prefer real workload objects; don't turn non-DB tests into DB tests.
- [ ] For interface extensions, **open a draft and align direction with the code
      owners first.**
- [ ] **Idiom sweeps across the executor tree get scrutiny.** (`type(self)` →
      `self.__class__`, bulk import rewrites.) Each site here has its own history
      and some have edge cases a sweep silently changes, so a mechanical change
      across many executors needs its occurrences justified rather than counted.
      Whether a given sweep is worth its risk is a reviewer's call, not a rule.
- [ ] **Stack dependent work on the branch it depends on.** A PR opened against
      `main` that carries someone else's unmerged commits is unreviewable and
      gets closed — target the dependency branch, or wait for it to land.

> Mined from PR review history; the area currently skews toward workload-type /
> back-compat concerns (AIP-92 executor-callbacks + async connection-testing are
> in flight). Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss first — interface changes affect every provider executor and need
coordination before the code.
