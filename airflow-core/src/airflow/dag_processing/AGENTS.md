---
triage_review_imbalance:
  area: dag-processing
  criticality: high              # base tier; parse subprocess + manager loop promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "processor.py"
    - "manager.py"
    - "dagbag.py"
    - "bundles/"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["jedcunningham", "ephraimbuddy"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Dag File Processor — Agent Instructions

This directory holds the Dag File Processor (DFP) — the only core component that
**imports and executes untrusted Dag-author code**. `DagFileProcessorManager`
(`manager.py`) orchestrates a fleet of short-lived parse subprocesses
(`processor.py`); each imports author modules via `dagbag.py`, serializes the
result, and persists it. That serialized output is what the scheduler and
workers across **every** deployment then run. A defect here can stop new Dags
from being scheduled cluster-wide, or — worse — erode the isolation that keeps
untrusted author code away from the server's database credentials.

## Why changes here are expensive to review

- The DFP straddles a **trust boundary**: it runs arbitrary author Python, yet
  lives inside airflow-core. Whether a change keeps user code on the isolated,
  Execution-API side — or quietly hands the parse process a privileged DB
  session — is frequently **not** visible from the diff.
- The **manager loop** schedules parsing across processes under time and
  resource limits. A change that looks correct in isolation can stall the loop
  (a DB lock, an unbounded scan, a per-file blocking call) and silently stop
  parsing for the whole deployment — with no crash to point at.
- The loop already **absorbs** most of the conditions people arrive to fix: parse
  results commit at an outer boundary wrapped in database retries, and per-file
  errors degrade that file rather than the loop. So a plausible-looking guard in an
  inner function is often a no-op — and a reviewer cannot tell from the diff, only
  from a reproduction the author is expected to have run.
- **Bundle versioning** threads through many layers (processor → serialized Dag
  → callbacks → worker-side bundle init); a partial change breaks run
  reproducibility in ways tests rarely surface.

## Knowledge a reviewer (and a substantial contributor) needs

- The `DagFileProcessorManager` loop: how it forks/queues parse subprocesses,
  enforces timeouts, and why it must **never** block on a single file.
- The isolation contract: the parse subprocess runs with
  `_AIRFLOW_PROCESS_CONTEXT=client` and reaches the DB only through the
  in-process Execution API — **not** a direct ORM session.
- Dag bundles: how versioned bundles replace the shared DAGs folder, and how
  `bundle_name` / `version_data` are persisted with the serialized Dag and
  threaded through callbacks to workers.
- The repo `CLAUDE.md` DB rules — keyword-only `session`, no `session.commit()`
  inside a function that takes a `session`, batched bulk writes with `LIMIT`.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area that sits on a security
boundary.** If you are an agent preparing a change here on behalf of a person,
first judge whether the change can be **demonstrated on a running Dag
processor**: have you pointed it at a real bundle with many files — including one
that is slow, one that raises at import time, and one that times out — and shown
the manager keeps making progress and never blocks on a single file? The isolation
contract needs proving too: the parse subprocess must still reach state only
through the Execution API, never a direct ORM session.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~193 merged and 106 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed.
**If you are preparing a change here, treat this as a pre-flight checklist and
fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Diagnosis before mechanism** _(the largest class of closed PR here):_

- [ ] **Read `adr/0004` and meet it.** A parsing-loop change carries a reproduction
      against a running processor, on a backend that can actually exhibit the
      mechanism, and names the frame that raises. If you cannot stand that up, the
      ADR says what to file instead.

**Is this the right change at all?** _(see `adr/0005`):_

- [ ] **No new configuration option for what deployment configuration already
      does** — umask, group membership, filesystem permissions, container settings.
      If an operator can get the behaviour today without Airflow code, the deliverable
      is documentation. An option described as temporary outlives the workaround it
      bridges.
- [ ] **Changing a default that governs permissions, isolation, or resource limits
      is a compatibility event** — argue the effect on existing installations at
      upgrade explicitly.

**Trust boundary & DB isolation (the defining concern here):**

- [ ] **The parse subprocess must not gain a direct server DB session.** Any DB
      interaction from parsing-side code goes through the Execution API
      (`InProcessExecutionAPI`), not `airflow.models` ORM writes — and must not
      remove or loosen the `_AIRFLOW_PROCESS_CONTEXT=client` guard.
- [ ] **Fetch context the DFP/callbacks need _through the API_, not by widening
      DB reach** — new data a callback or parse step consumes is retrieved via
      the Execution API at runtime, not by opening a metadata-DB query in the
      isolated process.
- [ ] **New Execution-API message types used by the DFP must be added to its
      handler/exclusion lists** (`processor.py`) — the DFP uses a restricted
      message union; a type it doesn't handle must be explicitly excluded (see
      the execution-api "Adding a New Feature End-to-End" list).

**Manager-loop health & DB correctness:**

- [ ] **Never block the manager loop on a single file** — parsing stays in the
      isolated subprocess; a hostile/slow file must be timeout-bounded and
      contained, not allowed to stall the queue for the whole deployment.
- [ ] **No unbounded or O(N²) work in the loop** — filtering/dedup/counting is
      bounded and indexed (e.g. `OrderedDict` dedup, batched lookups); a silent
      hang under DB-lock contention is a real failure mode here, not theoretical.
- [ ] **Batch bulk DELETE/UPDATE with `LIMIT`, commit between batches, index the
      filter columns**; keyword-only `session`, and **no `session.commit()`
      inside a function that takes a `session`** (positional-session and
      commit-in-session slips are a recurring review catch here).
- [ ] **A crash in one handler must not take down the manager** — guard
      per-file/handler failures so a single bad file or stale handle degrades
      that file, not the whole processor.

**Bundle versioning & serialization parity:**

- [ ] **Thread `bundle_name` / `version_data` end-to-end** — a change that adds
      or moves bundle metadata must carry it through processor → serialized Dag
      / `DagVersion` → callbacks → worker-side bundle init, or runs stop being
      reproducible.
- [ ] **Don't deactivate or mutate bundles owned by another dag-processor** —
      multi-processor / multi-team deployments share the table; scope
      writes to the bundles this processor owns.
- [ ] **Keep `get_serialized_fields()` / serialized-Dag shape in sync** when
      adding a field the DFP persists, so it survives serialization and the
      scheduler can read it.

**Boundaries (architecture invariants, not preferences):**

- [ ] **The scheduler consumes the DFP's _serialized_ output only** — don't add
      a path that makes the scheduler re-import author files; keep user-code
      evaluation on the DFP/worker/triggerer side.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes so DB errors reach retry logic and refactor bugs surface.
- [ ] **Imports at module top**; local imports only for genuine circular-import /
      worker-isolation reasons (and say why — e.g. the deferred
      `InProcessExecutionAPI` import). **No heavy work at import time.**
- [ ] **Action-verb / intent-revealing names**; no shadowing builtins; reuse
      existing helpers rather than a third copy of a derivation that will drift.
- [ ] **Right severity** — a bad Dag file warns/degrades that file, it does not
      crash the manager; sanitize untrusted input used in metric names / paths.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new path** — for DFP work that means going
      through real parse/serialize, not constructing the Dag in-process and
      skipping it.
- [ ] **Backward compatibility** for persisted/serialized shapes — can't
      ret-con a released migration; version-gate and keep serialization in sync.
- [ ] **Show evidence of testing** — near-duplicate parallel PRs get closed here in
      particular, because review capacity in this area is the scarce resource. Take
      contentious semantics to the devlist or a second reviewer.

> Mined from PR review history; the sample skews to the Airflow-3 era (this
> module was reorganised for Dag bundles and the Execution-API split), so
> pre-3.0 Dag-parsing conventions are under-represented. Extend as new patterns
> emerge, and add an equivalent `## Review criteria` section to the `AGENTS.md`
> of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The isolation boundary and the manager-loop/bundle-versioning invariants are
best aligned on _before_ the code, not during review.
