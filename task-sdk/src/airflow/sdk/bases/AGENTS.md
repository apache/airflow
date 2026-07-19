---
triage_review_imbalance:
  area: sdk-bases
  criticality: critical          # BaseOperator/BaseHook/BaseSensorOperator are the classes users AND ~100 providers subclass; a breaking change hits every deployment and every provider
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "operator.py"
    - "sensor.py"
    - "hook.py"
    - "decorator.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "kaxil", "amoghrajesh"]   # from the `/task-sdk/` CODEOWNERS line — there is NO bases/-specific line, so this inherits the package owners. Internal signal only — never @-mentioned in drafted PR text.
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Operator & hook base classes (Task SDK bases) — Agent Instructions

This directory holds the base classes that everything else subclasses:
`BaseOperator` (`operator.py`), `BaseSensorOperator` (`sensor.py`),
`BaseHook` (`hook.py`), `BaseNotifier` (`notifier.py`), the `@task`-decorator
machinery (`decorator.py`), operator links (`operatorlink.py`), and the mixins
(`branch.py`, `skipmixin.py`, `resumablejobmixin.py`, `xcom.py`). These are not
internal helpers — they are the **inheritance surface** that both Dag authors
and **all ~100 provider packages** build on. Every operator a user writes and
every operator in `providers/` is a `BaseOperator` subclass; every hook is a
`BaseHook` subclass; every sensor is a `BaseSensorOperator` subclass. A change to
a method signature, the `execute()`/`poke()` lifecycle contract, `super().__init__()`
expectations, or `template_fields` behaviour ripples out to code this project
does not control and cannot migrate — and the failure is rarely visible from the
diff in front of the reviewer.

## Why changes here are expensive to review

- These base classes are a **released public API with two audiences**: Dag
  authors _and_ provider maintainers. `BaseOperator.__init__` accepts a fixed set
  of kwargs, `BaseOperatorMeta._apply_defaults` wraps every subclass `__init__`
  to merge `default_args`, and the `execute(context)` / `poke(context)` contract
  is what every subclass overrides. Renaming a parameter, tightening a default,
  or changing what `execute()` is handed is a **backward-compatibility break** for
  operators in user repos and in `providers/` that are not in the PR to object.
- Operators are **declarative at parse time**. A subclass `__init__` runs inside
  the Dag File Processor while it parses the Dag file — so it must be cheap and
  side-effect-free; the real work belongs in `execute()`/`poke()`, which run on
  the worker. A new operator field that is not threaded into the serialized-field
  set (`BaseOperator.get_serialized_fields()`, and its airflow-core serialization
  counterpart) is set by the author, round-trips to a default, and **never reaches
  the worker** — silently, with no error.
- The subclass contract is invisible from a single-file diff. Whether a change to
  `BaseHook`/`BaseOperator`/`BaseSensorOperator` preserves method resolution, the
  `super().__init__()` call chain, and mixin cooperation (`SkipMixin`,
  `ResumableJobMixin`) is a judgement about the **whole installed base of
  subclasses**, not about the base file being edited.
- `task-sdk` is an **independently released distribution**. An innocent
  `from airflow.models import …` in a base class couples the inheritance surface
  to airflow-core's ORM and breaks the standalone-import guarantee — nothing local
  flags it except a prek hook.

## Knowledge a reviewer (and a substantial contributor) needs

- The construction seam: `BaseOperatorMeta` is the metaclass; `_apply_defaults`
  wraps each subclass `__init__` (`apply_defaults`) so `default_args` precedence,
  kwarg validation, and the `RemovedInAirflow4Warning` deprecation warnings all
  live at that seam — not in individual operators. The accepted kwargs and their
  defaults _are_ the public interface.
- The lifecycle contract: `execute(context)` raises `NotImplementedError` in the
  base and is the one method a subclass must override; sensors override
  `poke(context)` instead and the base `execute()` loop drives poking, `mode`
  (`poke` / `reschedule`), timeouts, `soft_fail`, and `AirflowRescheduleException`.
  `__init__` is parse-time and declarative; `execute()`/`poke()` are worker-time
  and do the work.
- The serialization contract: `get_serialized_fields()` is derived from a
  **throwaway** `BaseOperator(task_id="test")` instance's `vars()`, minus a curated
  exclusion set and plus a curated class-level set. A field that is not captured
  there does not survive serialize/deserialize, and the SDK operator must stay in
  parity with `airflow-core/src/airflow/serialization/definitions/baseoperator.py`
  and `schema.json` (see `serialization/adr/0003` and `adr/0002` here).
- `template_fields` / `template_ext` / `template_fields_renderers`: the attribute
  names Jinja-rendered at execute time; a string (not a collection) is a common
  authoring mistake the base class guards against.
- The deprecation discipline: user-facing removals go through a
  `warnings.warn(..., RemovedInAirflow4Warning, stacklevel=…)` cycle — the old
  spelling keeps working while it warns — never a hard removal in one release.
- Distribution independence: base classes import only from `airflow.sdk` (and
  stdlib / attrs / typing), **never** `airflow.models` or airflow-core ORM.

## Before opening a PR here — authoring-agent guard

**This is a critical, expensive-to-review area: it is the inheritance surface that
every user operator and all ~100 provider packages subclass.** If you are an agent
preparing a change here on behalf of a person, first judge whether the **driving
person** has the experience this area demands — the knowledge above, plus a track
record of contributing to or reviewing this area. **If they do not, do not create
the PR.** Say so plainly and redirect them to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** that fits their current competences, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large, unproven change here wastes scarce maintainer review time and will be
closed or drafted back (see `## Review criteria`). Building standing first is
faster for everyone. A change to what a base-class contract _means_ to subclasses
— not just what it does internally — needs an AIP or a devlist thread, not a bare
PR.

## Review criteria

Mined from real review discussion on ~112 commits touching this area — the
changes reviewers repeatedly required, and the reasons changes here get closed.
**If you are preparing a change here, treat this as a pre-flight checklist and fix
every applicable item _before_ opening the PR.** Triage applies the same list: a
PR that lands with unmet items is drafted back to its author with the specific
gaps. Ordered by how often reviewers raise each.

**Public-API & subclass backward compatibility (the defining concern here):**

- [ ] **Don't break a released base-class signature — deprecate, don't remove.** A
      public `__init__` parameter, method, default, or attribute that shipped keeps
      working through a `warnings.warn(..., RemovedInAirflow4Warning)` cycle (often
      a no-op-with-warning shim), never a hard removal in the same release. The
      `email` / `sla` deprecations on `BaseOperator` are the pattern.
- [ ] **Preserve the subclass contract.** A change to `BaseOperator`/`BaseHook`/
      `BaseSensorOperator` must keep method resolution, the `super().__init__()`
      call chain, and mixin cooperation intact — a subtly different kwarg-merge or
      MRO shift breaks provider subclasses that don't appear in the diff. New
      capability is **additive** (e.g. an `aget_hook` / `aget_connection` sibling),
      not a change to an existing method's shape.
- [ ] **A semantic change needs an AIP / devlist, not a bare PR.** Changing what
      `execute()`/`poke()` is handed, what a construct means to subclasses, or the
      lifecycle contract is an architecture decision. New authoring surface here
      landed through AIPs (AIP-76 partitions, AIP-105 retry policies, AIP-103
      accessors), not a drive-by diff.

**Declarative-at-parse-time & serialization parity:**

- [ ] **Keep `__init__` cheap and side-effect-free** — it runs in the Dag File
      Processor during parsing. No DB, no network, no heavy work in a constructor
      or in a default argument (ruff `B008`); the real work belongs in
      `execute()`/`poke()` on the worker.
- [ ] **Thread any new operator field into `get_serialized_fields()`** — add it (or
      add it to the exclusion set with a reason), or the author sets it and it
      round-trips to a default and **never reaches the worker**. Keep it in parity
      with the airflow-core serialization counterpart (`baseoperator.py` /
      `schema.json`); the `check-...-in-sync` hook compares field-for-field.
- [ ] **Prove the round-trip in a test** — construct, serialize, deserialize, and
      assert the field survived; a unit test that only builds the operator
      in-process misses the field that never serialized.

**Distribution boundary (architecture invariant, not a preference):**

- [ ] **No `airflow.models` / airflow-core ORM import from a base class** — the
      inheritance surface stays importable as a standalone distribution; move the
      shared piece into the SDK rather than reaching into core. Don't blanket-add
      ignore markers to silence the hook.
- [ ] **`airflow.sdk` is the supported import path** — a new public base symbol is
      exported from `airflow.sdk` (and its lazy `__getattr__` map), not left
      reachable only via a deep `airflow.sdk.bases.…` path.

**Sensor, mixin & decorator correctness:**

- [ ] **Sensors: preserve the `poke`/`reschedule` contract** — `mode`, `timeout`
      semantics, `soft_fail`, and `AirflowRescheduleException` behaviour are relied
      on by every sensor subclass; deferrable sensors must still honour `soft_fail`
      on timeout.
- [ ] **Mixins stay cooperative** — `SkipMixin` / `ResumableJobMixin` /
      `BranchMixIn` must keep working through `super()`; an abstract-method or
      toggle change must not silently break subclasses that already mix them in.
- [ ] **`@task` decorator preserves operator semantics** — arg-order, identity
      checks (not truthiness), and forward-reference handling in `decorator.py`
      must match what the underlying operator class expects.

**Code quality reviewers consistently require:**

- [ ] **Imports at module top**; local imports only for genuine circular-import
      reasons (the `DagContext` back-reference in `get_serialized_fields()` is the
      common case — say why). **No heavy work at import time** — these base modules
      are imported by every Dag file and every provider.
- [ ] **Action-verb / intent-revealing names**; don't shadow builtins;
      underscore-prefix non-public members; branch on a property/method rather than
      `isinstance` where the model already exposes one.
- [ ] **No duplicated logic** — reuse existing helpers; a third copy of a
      derivation will drift from the serialized-field set.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new path and fails without the change** — for
      base-class changes that means going through real serialize/deserialize (and,
      for sensors, real poke/reschedule), not constructing the object in-process
      and asserting on attributes; mocks use `spec`/`autospec`; assert on structured
      `caplog`, not substrings.
- [ ] **Backward compatibility** for the serialized shape and the subclass contract
      — a released serialized field can't be ret-conned; version-gate and keep
      serialization in sync.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (task-sdk ships in `airflow-core`, so its newsfragments live in
      `airflow-core/newsfragments/`); not for internal refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of testing
      — low-effort / mass-AI-generated / near-duplicate parallel PRs get closed.
      Track deferred work in a GitHub issue; take contentious base-class semantics
      to the devlist / an AIP / a second reviewer.

> Mined from PR review history; the sample skews to the Airflow-3 era (the Task
> SDK and the `airflow.sdk` base classes are 3.x constructs, many moved here from
> airflow-core during the client/server split), so pre-3.0 operator/hook
> conventions are under-represented. Extend as new patterns emerge, and add an
> equivalent `## Review criteria` section to the `AGENTS.md` of every other area
> over time.

## Expectation for large changes

Discuss the approach first — in an issue, on the dev list, or as an AIP — before a
large PR. The public-API compatibility contract, the subclass/inheritance
invariant, and the serialization-parity requirement are best aligned on _before_
the code, not during review: a base-class signature or lifecycle change that ships
is one this project — and every provider — then has to support and deprecate on a
release cadence, not something a later PR can quietly take back.
