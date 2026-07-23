---
triage_review_imbalance:
  area: triggers
  criticality: high              # base tier; base trigger contract + shared-stream engine promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "base.py"
    - "shared_stream.py"
    - "callback.py"
  codeowners_ref: ".github/CODEOWNERS"
  # No dedicated CODEOWNERS line exists for airflow-core/src/airflow/triggers/ —
  # the code that actually runs these triggers, triggerer_job_runner.py, is
  # jobs-owned (@dstandish @hussein-awala). `experts` below combines those
  # runner owners with the most active triggers/ author; it is an internal
  # signal only — never @-mentioned in drafted PR text.
  experts: ["dstandish", "hussein-awala", "ramitkataria"]
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Triggers (async deferral) — Agent Instructions

This directory holds the async `Trigger` base classes (`base.py`:
`BaseTrigger`, `BaseEventTrigger`, `TriggerEvent`) and the shared-stream
fan-out engine (`shared_stream.py`) that the **triggerer** evaluates. A
deferred task hands the triggerer a serialized trigger; the triggerer
reconstructs it and drives its `run()` coroutine on a **shared asyncio event
loop** alongside hundreds of other triggers, firing a `TriggerEvent` back to
resume the task. One triggerer process serves many deferred tasks across
**every** deployment, so a single misbehaving trigger — one that blocks the
loop, leaks a connection, or fails to reconstruct — degrades deferral for
tasks that have nothing to do with it.

## Why changes here are expensive to review

- The triggerer multiplexes many triggers' `run()` coroutines onto **one
  event loop**. A synchronous network/file/DB call, a `time.sleep`, or a
  CPU-bound spin in one trigger stalls **every** deferred task on that
  process — and the diff that introduces it usually looks like an ordinary
  function call, not an obvious blocking one.
- Triggers cross a **serialize / reconstruct boundary**: they are persisted
  as `(classpath, kwargs)` and rebuilt in a different process (on defer, on
  HA duplication, on redistribution between triggerers). Whether the kwargs
  survive that round-trip — JSON-serializable, deterministic, no live object
  or closure smuggled across — is frequently **not** visible from the diff.
- The trigger runner executes **user-authored trigger code** isolated the
  same way the Dag File Processor is. Whether a change keeps that code on the
  client / Execution-API side, or quietly hands it a server DB session, is
  not obvious locally. And event delivery is **at-least-once**, so a change
  that assumes a `TriggerEvent` fires exactly once breaks silently under
  restart / redistribution / HA.

## Knowledge a reviewer (and a substantial contributor) needs

- The `BaseTrigger` contract: `serialize()` returns
  `(classpath, kwargs)` sufficient to re-instantiate the trigger elsewhere;
  `run()` is an **async generator** that `yield`s a `TriggerEvent` when its
  condition fires and returns when finished; `cleanup()` and `on_kill()` are
  the two lifecycle hooks — and they are **not** interchangeable (see below).
- The triggerer runs many `run()` coroutines cooperatively on **one** event
  loop; a trigger must never block it and must be **cancellation-safe** — the
  runner cancels `run()` on shutdown, timeout, and redistribution.
- `cleanup()` runs on **every** exit (success, timeout, shutdown, user kill)
  and is for releasing this trigger's **local** resources; `on_kill()` runs
  **only** when a user explicitly acts on the task and is the one place to
  cancel **external** work. Cancelling external jobs in `cleanup()` kills
  in-flight work on every triggerer restart.
- `TriggerEvent` payloads must be JSON-serializable and carry a **stable
  identifying value**, because HA can run the same trigger in two places and
  events are deduplicated on that value.
- The isolation contract: the async trigger runner subprocess runs with
  `_AIRFLOW_PROCESS_CONTEXT=client` and reaches server state through the
  supervisor / Execution API (`InProcessExecutionAPI`), **not** a direct ORM
  session — mirroring the DFP.
- The shared-stream / ack-mode design in `shared_stream.py`:
  `shared_stream_key()` opts sibling triggers into one poll loop;
  `open_shared_stream()` / `filter_shared_stream()` / the ack-mode producer
  fan raw events out with at-least-once, redeliver-on-restart semantics.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area that runs user code on
a shared event loop across a trust boundary.** If you are an agent preparing a
change here on behalf of a person, first judge whether the change can be
**demonstrated on a running triggerer**: have you deferred a real task, watched
your trigger run alongside others on the shared event loop, and shown what happens
when it is cancelled, when the triggerer restarts mid-wait, and when the coroutine
blocks? One synchronous call in this area stalls every deferred task in the
deployment, and nothing in the diff shows which call blocks.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on the ~26 merged and ~30 closed-unmerged PRs
that have touched this area — the changes reviewers repeatedly required, and the
reasons changes here get reworked or reverted. The sample is **small**, so treat
this list as indicative rather than exhaustive. **If you are preparing a change here, treat
it as a pre-flight checklist and fix every applicable item _before_ opening
the PR.** Triage applies the same list: a PR that lands with unmet items is
drafted back to its author with the specific gaps. Ordered by how often
reviewers raise each.

**Async event-loop discipline (the defining concern here):**

- [ ] **`run()` must not block the shared event loop** — no synchronous
      network / file / DB call, no `time.sleep`, no CPU-bound spin. `await`
      real async I/O, and push unavoidable blocking work to an executor. One
      trigger's blocking call stalls **every** deferred task on that
      triggerer, not just its own.
- [ ] **Be cancellation-safe and leak-free** — the runner cancels `run()` on
      shutdown, timeout, and redistribution. Let `CancelledError` propagate
      (don't bury it in a broad `except`), and release **every** connection /
      task / socket in `cleanup()`, which runs on every exit. A trigger that
      leaves a background task or open connection behind accumulates them
      across the process's lifetime.
- [ ] **Don't conflate `cleanup()` with `on_kill()`** — `cleanup()` runs on
      _every_ exit and must only free **local** resources; `on_kill()` runs
      _only_ on explicit user action and is the sole place to cancel
      **external** work. Cancelling an external job from `cleanup()` kills
      in-flight work on every triggerer restart or rolling deploy.
- [ ] **Right severity** — a broken trigger degrades _that_ trigger's task;
      it must not crash the triggerer or take sibling triggers down with it.

**Serialization parity (triggers are persisted as `(classpath, kwargs)`):**

- [ ] **`serialize()` returns `(classpath, JSON-serializable kwargs)` that
      re-instantiate the trigger in another process** — no live objects, DB
      sessions, open connections, or closures smuggled through kwargs. The
      trigger is rebuilt from `(classpath, kwargs)` alone on defer, on HA
      duplication, and on redistribution.
- [ ] **kwargs and identity must be deterministic** — derive
      `shared_stream_key()` and the `TriggerEvent` identifying value from
      configuration fields, never from `time.time()` / `uuid.uuid4()`. HA runs
      the same trigger in two places and deduplicates on a stable value.
- [ ] **Subclass constructors must call `super().__init__()`** — a trigger
      that skips it is missing runner-injected state (`_task_instance`,
      `trigger_id`) and crashes the triggerer when it is reconstructed.
- [ ] **Keep the serialize / reconstruct round-trip in sync** when adding a
      field or touching kwargs — template-field filtering and `trigger_kwargs`
      must survive the round-trip, and a field added to the trigger must
      actually reach the reconstructed instance.

**Changing what a trigger or callback receives (the most revert-prone seam):**

- [ ] **Name the route the new data arrives by** — serialized kwargs, an
      Execution API fetch at execution time, or persisted state — and why the
      other two do not fit. This path has been merged-then-reverted twice; a diff
      confined to the trigger class looks complete and is not (see `adr/0004`).
- [ ] **Prefer fetching at execution time over persisting context.** Storing
      execution context in the metadata database so a callback can read it later
      is the shape this area has repeatedly moved away from; three successive
      attempts to build rendering on stored context were withdrawn.
- [ ] **Say which field set is rendered, and when.** An operator's
      `template_fields` and its trigger's attributes are distinct sets — rendering
      one against the other has already produced a defect.
- [ ] **Review the Execution API route and token scope with the change**, not
      after, when the triggerer must call something new.
- [ ] **Show end-to-end evidence** across defer, serialize, reconstruct, fetch,
      and resume. A unit test on the trigger class is not sufficient here, and a
      working local demonstration has not been either.
- [ ] **Confirm the current state of this path before building on it** — the most
      recent merged design in this seam has been reverted, so it is not
      necessarily the one in effect.

**Sync and deferrable paths stay separate:**

- [ ] **Do not consolidate a sensor's deferrable and non-deferrable logic into one
      implementation.** A PR merging `ExternalTaskSensor`'s two paths was turned
      away on the principle that mixing sync and async logic does more harm than
      good — the shared result is neither properly async nor simply synchronous
      (#34464). Where a deferrable path needs synchronous work, wrap it with
      `sync_to_async` and accept that the wrapped method is then unusable from the
      sync implementation.
- [ ] **New machinery on `BaseTrigger` must be adoptable by everything that
      already subclasses it.** An attempt to add a default deferrable-continuation
      entry point was withdrawn by its author because back-compat made it unusable
      in practice, and shipping it flag-guarded — core code carrying switches that
      say "do not use this yet" — was judged worse than not shipping it (#31691).
      State how existing core and provider triggers adopt the change, including
      what it does to subclasses that override the method with a different
      signature.

**Isolation & Execution-API boundary; at-least-once delivery:**

- [ ] **The trigger runner runs user code isolated** — it must not gain a
      direct server DB session. Reach server state through the Execution API /
      supervisor and do not remove or loosen the `_AIRFLOW_PROCESS_CONTEXT=client`
      guard on the runner path.
- [ ] **`TriggerEvent` handling must tolerate at-least-once delivery** — the
      same event can be delivered more than once across triggerer restart,
      redistribution, or HA duplication. Make task resumption idempotent;
      never assume exactly-once.
- [ ] **Reach injected state through the injected accessor** — where the
      runner injects an accessor (e.g. `asset_state_store` /
      `AssetStateStoreAccessors` on `BaseEventTrigger`), consume it rather
      than opening a new metadata-DB query inside the trigger.

**Code quality reviewers consistently require:**

- [ ] **Imports at module top**; guard heavy type-only imports (e.g.
      `jinja2`, ORM models) behind `TYPE_CHECKING` — this is a multi-process,
      import-cost-sensitive path.
- [ ] **Don't swallow exceptions with a broad `except`** — narrow to real
      classes, and never let a bare `except` eat `CancelledError` and defeat
      the runner's cancellation.
- [ ] **Action-verb / intent-revealing names**; no shadowing builtins; reuse
      existing helpers rather than a third copy of a derivation that will
      drift.

**Tests, compatibility, process:**

- [ ] Test **exercises the real serialize → reconstruct → `run()` path and
      fails without the change** — not one that constructs the trigger
      in-process and skips serialization; `await` `run()` in async tests;
      mocks use `spec`/`autospec`; assert on structured `caplog`, not
      substrings.
- [ ] **Backward compatibility** for the persisted `(classpath, kwargs)`
      shape and the public `BaseTrigger` interface — you can't ret-con a
      serialized shape already shipped in a release; version-gate and keep the
      round-trip in sync.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing**
      changes (not internal refactors).
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of
      testing — low-effort / mass-AI-generated / near-duplicate parallel PRs
      get closed. Track deferred work in a GitHub issue; take contentious
      semantics to the devlist / a second reviewer.

> Mined from PR review history; the sample here is **small** and the merged half
> skews to the Airflow-3 era — the AIP-86 async deadline-callback work and the
> shared-stream / ack-mode redesign dominate it. The closed-unmerged half reaches
> back to 2022 but is overwhelmingly inactivity closures and PRs superseded by a
> replacement, so it yields little on top of the merged record. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large
PR. The event-loop, serialization, and isolation invariants are best aligned
on _before_ the code, not during review.
