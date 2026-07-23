---
triage_review_imbalance:
  area: shared-libraries
  criticality: high              # base tier; the security/reliability primitives promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "secrets_masker/"
    - "logging/"
    - "serialization/"
    - "configuration/"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "amoghrajesh", "potiuk"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Shared libraries — Agent Instructions

This directory holds the **shared libraries** — small, self-contained Python
distributions (`apache-airflow-shared-*`: logging, serialization, configuration,
secrets_masker, secrets_backend, observability, timezones, module_loading,
plugins_manager, …) that provide foundational primitives reused across
distributions. Each library is a separate distribution with its own
`pyproject.toml`, `src/airflow_shared/<name>/`, and tests; its sources are
**symbolically linked** into the consumers that use it (e.g.
`airflow-core/src/airflow/_shared/`, `task-sdk/src/airflow/sdk/_shared/`), and
those links plus the per-consumer dependency wiring are maintained by `prek`
hooks. Because a single library feeds **many** distributions at once, a defect
here has the widest blast radius in the repository — a bad change to
`secrets_masker` or `logging` ships to every component simultaneously.

## Why changes here are expensive to review

- A change is made **in one place but lands in every consumer** through the
  symlinks. The diff shows one library, but the impact is airflow-core _and_
  task-sdk _and_ providers — a reviewer has to reason about all of them.
- These are **foundation/leaf** libraries: they sit below airflow-core and the
  Task SDK and must not depend back on them. Whether a change keeps that boundary
  (no `airflow.*` import creeping into `shared/`) is easy to miss in review and is
  what keeps the libraries independently packageable.
- Several of them are **security- or reliability-critical** and run everywhere:
  `secrets_masker` is the last line of defence against leaking secrets into logs;
  `logging`/`observability` sit on every process's hot path and must degrade, not
  crash the host.
- Consumers can vendor **different versions** of a shared library, so a change
  cannot assume all consumers upgrade in lockstep — the compatibility surface is
  invisible in a single-repo diff.

## Knowledge a reviewer (and a substantial contributor) needs

- The distribution layout enforced by `check-shared-distributions-structure`:
  each library is `apache-airflow-shared-<name>` with
  `src/airflow_shared/<name>/` (implicit namespace package — **no** top-level
  `__init__.py`) and a `tests/<name>/` package.
- The symlink model: editing a file under a consumer's `_shared/` edits the real
  file in `shared/<lib>/` (they are symlinks), so tests must be run in the
  **shared library itself**, not only in the consumer.
- The import boundary: `shared/` code must not import `airflow` core/SDK —
  enforced by `check-airflow-imports-in-shared` (with a small, explicit allow-list
  of exceptions).
- The repo `CLAUDE.md` "Shared libraries" section, and which consumers link each
  library (so you can reason about who a change affects).

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, widest-blast-radius area — one library feeds every
distribution.** If you are an agent preparing a change here on behalf of a
person, first judge whether the change can be **demonstrated across every
consumer**: have you enumerated which distributions link this library, run the
shared library's own tests _and_ the tests of each consumer that symlinks it, and
confirmed the import boundary still holds? A change here is not scoped to the
directory it lives in — the file you edited is the same file every consumer sees.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the blast radius is easier to bound, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~185 merged and 58 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed.
**If you are preparing a change here, treat this as a pre-flight checklist and
fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Distribution boundary & fan-out (the defining concern here):**

- [ ] **Change the library at its source and run _its_ tests** — a change lands
      in every consumer through the symlinks, so the shared distribution's own
      `tests/<name>/` must exercise it; don't rely only on a consumer's tests.
- [ ] **Keep the distribution structure intact** — `apache-airflow-shared-<name>`,
      `src/airflow_shared/<name>/` (no top-level `__init__.py`), `tests/<name>/`,
      correct `build-system` / hatch wheel targets (guarded by
      `check-shared-distributions-structure`); update every consumer's dependency
      wiring, not just one.
- [ ] **Don't quietly widen a library's public surface or dependencies** — a new
      third-party dependency in a shared library is paid by _every_ consumer that
      links it; justify it.
- [ ] **Agree the scope before creating a new shared library or moving code into
      one.** What belongs in `shared/` — and how it interacts with provider-side
      code — is settled on the dev list or an issue first. A large extraction
      proposed directly as a PR gets closed and redone against an agreed scope,
      because the boundary, not the code motion, is the reviewable decision.
- [ ] **A new shared surface must be justified against what already exists.** A
      new hook, class, or metric that an existing one can express is closed —
      reviewers ask what the existing surface cannot do before they consider the
      addition, and "zero new surface" beats a small new one.

**Vendor SDK initialisation belongs here, not in consumers:**

- [ ] **Consumers must not _initialise_ a telemetry/vendor SDK.** Airflow is many
      independent processes — scheduler, worker, Dag processor, triggerer — with
      no shared in-memory constructor, so each must initialise the SDK itself, and
      that initialisation lives once in the shared library
      ([`adr/0004`](adr/0004-vendor-sdk-initialisation-is-centralised-in-the-shared-library.md)).
      What is centralised is provider/exporter/sampler/resource setup, not the
      vendor API: `trace.get_tracer(...)`, propagators and `StatusCode` are
      ordinary application code and appear at a couple of dozen sites across
      `airflow-core` and `task-sdk` today.
- [ ] **Don't remove an abstraction here because it looks like indirection** —
      the wrappers over vendor SDKs exist to carry cross-process setup and
      multi-consumer versioning. Removing one is a design change: establish what
      replaces the initialisation path before deleting it.

**Import boundary (architecture invariant, not a preference):**

- [ ] **`shared/` must not import `airflow` core/SDK.** No back-import of
      airflow-core or task-sdk from a shared library — it would create a
      circular dependency and break independent packaging (`check-airflow-imports-in-shared`
      enforces this; don't add to its allow-list to sidestep a real violation).
- [ ] **Move shared logic _out_ of core into the library, not the reverse** — when
      a primitive is needed on both sides, it lives in `shared/` and both sides
      link it; don't re-introduce a core/SDK dependency to "reuse" something.

**Load-bearing safety (these libraries run everywhere):**

- [ ] **`secrets_masker` must never let a secret through** — redaction has to walk
      nested/compound structures and survive round-trips; a change that narrows
      what gets masked, or exposes a value on a new path, is a security
      regression, not a refactor.
- [ ] **`logging` / `observability` must degrade, not crash** — a malformed log
      format, a missing field, or a metrics-emitter error must not take down the
      host process; keep emitters defensive and metric names sanitized.
- [ ] **`serialization` output stays deterministic and round-trips** — a change
      must deserialize what older/newer consumers serialized (cross-version), and
      must not make serialized output order- or environment-dependent.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`**; **imports at module top**
      (local imports only for genuine circular-import reasons, and say why);
      **action-verb / intent-revealing names**; reuse existing helpers rather than
      a third copy that will drift.
- [ ] **Guard heavy type-only imports with `TYPE_CHECKING`** — shared libraries are
      imported into multi-process, import-time-sensitive code paths.

**Tests, compatibility, process:**

- [ ] Test **lives in the shared library and fails without the change**; mocks use
      `spec`/`autospec`; assert on structured `caplog`, not substrings.
- [ ] **Backward / cross-version compatibility** — consumers may ship different
      versions of the library, so a released shape/behaviour can't be ret-conned;
      version-gate and keep a compat path.
- [ ] **No repo-wide mechanical style sweeps.** A formatting or idiom change
      applied across shared code is closed unless the underlying rule is already
      agreed — reviewers have disagreed with the rule itself and dropped it by
      lazy consensus rather than accept the sweep, and mechanical rewrites
      (`type(self)` → `self.__class__` and similar) are rejected as change for
      its own sake with real edge cases behind them.
- [ ] **Check for an existing PR and confirm the bug still reproduces.** Closures
      here are dominated by "already fixed" and by two contributors fixing the
      same defect at once; a competing PR is not justified by the other one
      having a failing test.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of testing
      across affected consumers — low-effort / mass-AI-generated / near-duplicate
      parallel PRs get closed, and an unreadable or clearly generated description
      gets the PR closed on its own. Take contentious boundary or security
      semantics to the devlist / a second reviewer.

> Mined from PR review history; the sample skews to the Airflow-3 era (the
> shared-library distributions were extracted during the airflow-core / task-sdk
> split), so pre-split conventions are absent by construction. Extend as new
> patterns emerge, and add an equivalent `## Review criteria` section to the
> `AGENTS.md` of every other area over time.

### What these documents are currently good for

A validation pass ran this area's ADRs against the live open-PR queue and found
**zero firings across 8 PRs**. That is recorded here deliberately, as a finding
rather than a gap: the shared tree's open work — mostly metric and hook
additions — sits comfortably inside the boundaries these ADRs draw, so the
documents currently function as _review guidance and onboarding context_, not as
a mechanical gate. Do not read the absence of firings as a reason to sharpen the
rules until they catch something. Inventing a trigger to make the ADRs "earn
their keep" would produce false positives against merged work and destroy the
signal this measurement carries.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The fan-out across consumers, the import boundary, and the cross-version
compatibility of these primitives are best aligned on _before_ the code, not
during review.
