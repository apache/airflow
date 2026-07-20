---
triage_review_imbalance:
  area: task-sdk-execution
  criticality: critical          # workers run untrusted user code here; a defect can corrupt task state on every deploy
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "supervisor.py"
    - "task_runner.py"
    - "comms.py"
    - "context.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "kaxil", "amoghrajesh"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Task SDK execution runtime (supervisor & task runner) — Agent Instructions

This directory is the **worker-side execution runtime**: the code that actually
runs a user's task on a worker. `supervisor.py` forks and oversees a task
subprocess; `task_runner.py` is what runs _inside_ that subprocess and executes
the operator / `python_callable`; `comms.py` defines the length-prefixed message
protocol the two speak; and the supervisor is the **only** side that holds the
short-lived, task-scoped JWT and talks to the API server's Execution API. This
is the seam where untrusted author code executes, yet it must never touch the
metadata DB directly — so a defect here can corrupt task state, leak a
privileged session to user code, or wedge every worker in a deployment, and the
failure modes are IPC- and signal-timing-dependent and hard to reproduce in
review.

## Why changes here are expensive to review

- The runtime straddles a **trust boundary**: `task_runner.py` runs arbitrary
  author Python, yet the process must reach the metadata DB **only** through the
  supervisor's Execution-API client, never a direct ORM session. Whether a
  change keeps the DB-touching code on the supervisor side — or quietly lets the
  task subprocess open its own connection — is frequently **not** visible from
  the diff.
- The **supervisor ↔ runner IPC** is a real protocol over a socket with signal
  handling, EOF/deadlock edge cases, and warm-shutdown semantics. A change that
  reads correctly in isolation can strand a supervisor on missed EOF, deadlock
  the `CommsDecoder`, or drop a terminal state — all of which surface only under
  specific timing, not in a happy-path test.
- Every Execution-API message the runtime sends is **versioned and deploys
  independently** of the server: workers and API servers run different releases,
  so a message/schema change that "works" against a matching server can break a
  worker against an older or newer one. That compatibility surface is invisible
  in a single-repo diff.
- `task-sdk` is an **independently released distribution**; an innocent-looking
  `from airflow.models import …` couples the worker runtime to airflow-core's
  ORM and breaks that independence, but nothing local flags it except a prek
  hook.

## Knowledge a reviewer (and a substantial contributor) needs

- The supervisor/runner split: `supervisor.py` forks the task subprocess, owns
  the Execution-API client + the task-scoped JWT, and relays requests; the
  operator body runs in `task_runner.py` inside the child, which reaches the DB
  **only** by sending a message back up to the supervisor.
- The `comms.py` message protocol: length-prefixed frames, the request/response
  message union, `CommsDecoder`, and how a new interaction is added end-to-end
  (see the Execution-API "Adding a New Feature End-to-End" list — datamodel →
  route → Cadwyn version → `comms.py` → client → supervisor handler →
  regenerated `_generated.py` → per-version tests).
- The isolation contract: the task subprocess never opens an `airflow.models`
  ORM session or a raw DB connection; each task is issued a **short-lived JWT
  scoped to its task-instance id**, refreshed by the supervisor when it nears
  expiry, and redacted from logs.
- `task-sdk` is an independent distribution: it must not import
  `airflow.models` / airflow-core ORM (guarded by the `check_core_imports_in_sdk`
  prek hook); worker-bound schemas live in `airflow.sdk.api.datamodels._generated`,
  and it must stay installable and versioned independently of airflow-core.
- Signal + lifecycle handling on the supervisor: SIGTERM warm-shutdown vs. kill,
  `fork`+`exec` on macOS to avoid `SIGSEGV`, forwarding termination signals to
  the child, and failing _closed_ when an IPC / terminal-state call fails.

## Before opening a PR here — authoring-agent guard

**This is a critical, expensive-to-review area that sits on a security
boundary.** If you are an agent preparing a change here on behalf of a person,
first judge whether the change can be **demonstrated on a real task run**: have
you executed a task through an actual supervisor/runner pair — not just unit
tests around the function you touched — and observed what happens on the paths
that matter here: the subprocess being SIGTERM'd mid-task, an IPC frame failing,
the JWT expiring and being refreshed? Those are the failure modes reviewers ask
about, and none of them is visible in the diff.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~380 merged and 138 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed.
**If you are preparing a change here, treat this as a pre-flight checklist and
fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Trust boundary & DB isolation (the defining concern here):**

- [ ] **The task subprocess must not gain a direct server DB session.** Any DB
      interaction from `task_runner.py`-side code goes through the supervisor's
      Execution-API client (a `comms.py` message), never `airflow.models` ORM
      access or a raw connection from the child process.
- [ ] **Keep the JWT on the supervisor and scoped to the task instance** — don't
      widen the token's scope, hand it to user code, or log it unredacted; the
      supervisor refreshes it before expiry, so don't reintroduce a long-lived or
      shared token.
- [ ] **Fetch new runtime context _through the Execution API_, not by widening DB
      reach** — data a task/callback needs is retrieved via a supervisor request,
      not by opening a metadata-DB query inside the worker process.

**Supervisor ↔ runner IPC health:**

- [ ] **New interactions go through the `comms.py` message protocol** — add the
      request/response type to the message union and handle it in the supervisor;
      don't smuggle state across the boundary by another channel. A new type must
      also be added to the Dag-processor / triggerer handler-or-exclusion unions
      (they share `InProcessExecutionAPI`).
- [ ] **Fail _closed_ on IPC / terminal-state failure** — a supervisor call that
      fails on a non-success terminal state must not silently mark the task
      succeeded; recover stuck TIs rather than dropping the state.
- [ ] **Don't wedge or deadlock the IPC loop** — handle EOF, transient network
      errors, out-of-order reads, and force-closed sockets without stranding the
      supervisor or the child; respect the length-prefixed framing.
- [ ] **Signals & shutdown**: SIGTERM should warm-shut-down (let the running task
      finish/clean up) rather than hard-kill; forward termination signals to the
      child; keep the macOS `fork`+`exec` path intact (bare `fork` `SIGSEGV`s).

**Execution-API compatibility (workers and servers deploy independently):**

- [ ] **Version any worker-bound schema/message change** — a `comms.py` schema
      change must be paired with a Cadwyn `VersionChange` and a regenerated
      `_generated.py` (the `check-supervisor-schemas-versions` hook enforces
      this); a newer worker must still work against an older server and vice
      versa.
- [ ] **Wire new features end-to-end** — datamodel → route → version file → SDK
      client → `comms.py` → regenerated models → supervisor handler → per-version
      tests; half-wired changes get flagged.
- [ ] **Enforce schema/`type`-literal parity** — a supervisor schema class name
      must match its `type` literal (guarded); don't hand-edit generated models.

**Distribution boundary (architecture invariant, not a preference):**

- [ ] **task-sdk must not import `airflow.models` / airflow-core ORM** — use
      `airflow.sdk.api.datamodels._generated`; keep heavy server-only deps
      (FastAPI, Cadwyn) off the worker import path. The `check_core_imports_in_sdk`
      hook enforces this; don't blanket-add ignore markers to silence it.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes so IPC/API errors surface and terminal-state handling stays correct;
      never mask a failed state update as success.
- [ ] **Imports at module top**; local imports only for genuine circular-import /
      worker-isolation / heavy-dep-deferral reasons (and say why — e.g. the
      deferred Cadwyn/FastAPI import that keeps them off the worker path). **No
      heavy work at import time.**
- [ ] **Guard heavy type-only imports with `TYPE_CHECKING`** (this is multi-process
      code) and use action-verb / intent-revealing names; reuse the shared request
      handlers rather than a third copy that will drift.
- [ ] **Explicit resource cleanup** — close sockets / subprocess handles at the
      call site and run `finalize` even when a supervisor call fails; don't rely
      on `__del__` / GC teardown.

**Why changes here get closed (mined from the closed-unmerged record):**

- [ ] **Hardening needs a demonstrated failure, not a hypothesis.** A new guard,
      retry, timeout, or louder log must cite an observed occurrence, a bug
      report, or a reproducer that fails on `main` — "this could in principle
      raise" is not a justification for a permanent untested branch in the
      runtime (see `adr/0004`). Maintainer-authored changes are closed on this
      ground too.
- [ ] **Show the mechanism actually reaches the failure.** Name which process is
      still alive, which side of the IPC boundary sees the error, and what state
      the task instance ends in. A guard in a process the failure already killed
      does nothing; so does an extra `comms.py` message for an exception that is
      swallowed elsewhere.
- [ ] **Verify the diagnosis before proposing the fix** — confirm the symptom is
      a defect rather than normal debug output, and that the linked issue's
      reproducer still fails on current `main`. PRs are withdrawn once review
      shows the fix overclaimed what it addressed.
- [ ] **A refactor needs the consumer that requires it, in the same PR** —
      extraction "for further reusability", idiom sweeps, and removing
      redundant-looking indirection are declined here; the review cost of a
      structural diff in timing-dependent code is paid up front while the benefit
      is speculative (see `adr/0005`). Keep refactor and behaviour change in
      separate PRs.
- [ ] **Don't add a new state, message, or listener hook when existing ones carry
      the semantics** — reach for the established concept (existing task states
      and accessors, `STATES_SENT_DIRECTLY`, the data-interval fields) before new
      runtime surface; a stack-spanning feature landing only its runtime layer is
      inert until the rest arrives.
- [ ] **Search for the in-flight PR before opening yours.** Duplicate and
      near-duplicate parallel work is the single most common closure reason in
      this area — and splitting a slice out of someone's open PR to get it merged
      sooner produces a duplicate the moment the parent lands.
- [ ] **Respond to review yourself — don't relay LLM output.** Pasting an
      assistant's analysis back into the thread, opening unreviewed AI-generated
      changes, or leaving review threads unresolved gets PRs closed regardless of
      the diff's merit. Resolve threads yourself once addressed, and keep the
      branch rebased: the `ready for maintainer review` label is revoked when
      conflicts reappear.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new path and fails without the change** — for
      runtime work that means driving the real supervisor/runner over a socket
      (add a `RequestTestCase` to `REQUEST_TEST_CASES`, and a task-runner
      integration test), not constructing the task in-process and skipping the
      IPC round-trip; mocks use `spec`/`autospec`; assert on structured `caplog`,
      not substrings.
- [ ] **Backward compatibility** for worker-bound message/schema shapes — a
      released message shape can't be ret-conned; version-gate and keep the
      generated models in sync.
- [ ] **Newsfragment only for genuinely user-facing** changes (task-sdk ships in
      `airflow-core`, so its newsfragments live in `airflow-core/newsfragments/`);
      not for internal runtime refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of
      testing — low-effort / mass-AI-generated / near-duplicate parallel PRs get
      closed. Track deferred work in a GitHub issue; take contentious protocol or
      isolation semantics to the devlist / a second reviewer.

> Mined from PR review history; the sample skews to the Airflow-3 era (the Task
> SDK, supervisor/runner split, and Execution-API path are all 3.x constructs),
> so pre-3.0 worker-execution conventions are absent by construction. Extend as
> new patterns emerge, and add an equivalent `## Review criteria` section to the
> `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The isolation boundary, the supervisor/runner IPC protocol, and the
independent-deploy compatibility invariants are best aligned on _before_ the
code, not during review.
