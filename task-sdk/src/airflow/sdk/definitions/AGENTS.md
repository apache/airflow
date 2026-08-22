---
triage_review_imbalance:
  area: sdk-definitions
  criticality: critical          # this is the public authoring API users write Dags against; a breaking change hits every deployment's Dag files
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "dag.py"
    - "mappedoperator.py"
    - "taskgroup.py"
    - "xcom_arg.py"
    - "param.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "kaxil", "amoghrajesh"]   # from the `/task-sdk/` CODEOWNERS line — there is NO definitions/-specific line, so this inherits the package owners. Internal signal only — never @-mentioned in drafted PR text.
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Dag authoring surface (Task SDK definitions) — Agent Instructions

This directory is the **public Dag-authoring API**: the classes and decorators a
user imports from `airflow.sdk` to write a Dag — `DAG`, `BaseOperator`,
`TaskGroup`, `Param`/`ParamsDict`, mapped operators (`partial`/`expand`),
`EdgeModifier`/`Label`, assets, `XComArg`, and the `@task` / `@task_group` /
`@dag` decorators. It is a **stable contract**, not internal plumbing: every
signature and default here is something users have already written against, in
Dag files this project does not control and cannot migrate. It is also
**declarative** — an author's intent is captured as serializable data
(`DAG._DAG__serialized_fields`, `BaseOperator.get_serialized_fields()`) so the
scheduler and workers can act on it **without ever running the Dag file again**.
A defect here breaks Dag files across **every** deployment, or silently drops
authored intent on the way to the scheduler — and neither failure mode is
obvious from the diff.

## Why changes here are expensive to review

- These signatures are a **released public API**. Renaming a parameter, tightening
  a default, or making a previously-accepted Dag raise is a **backward-compatibility
  break** for user code that already exists — and the users who wrote it are not in
  the PR to object. Whether a change is compatible is a judgement about the whole
  installed base, which a single-repo diff cannot show.
- The definitions are **declarative and must serialize losslessly**. A new field
  on `DAG` or `BaseOperator` that is not threaded into the serialized-field set —
  and kept in parity with airflow-core's serialization — simply **never reaches**
  the scheduler or worker: the author sets it, it round-trips to a default, and
  the value is lost with no error. The parity is invisible from the definitions
  side of the diff alone.
- `task-sdk` is an **independently released distribution** and the stable import
  surface (`airflow.sdk`) that underpins the 2.x→3.x compatibility story. An
  innocent-looking `from airflow.models import …` couples the authoring package to
  airflow-core's ORM and breaks that independence — nothing local flags it except a
  prek hook.
- The mapping machinery (`partial()` / `expand()` / `expand_kwargs()`,
  `MappedOperator`, `XComArg`) and the `TaskGroup` graph are **evaluated at parse
  time inside user Dag files**; an edge case that reads correctly in isolation can
  change how an existing Dag's tasks fan out or how the graph is built.

## Knowledge a reviewer (and a substantial contributor) needs

- The authoring model: `DAG` is an `attrs`-defined class whose
  `field_transformer` forces everything after `dag_id` to be **keyword-only**, so
  the public constructor's argument names and defaults _are_ the API. `BaseOperator`
  wraps every subclass `__init__` through `BaseOperatorMeta._apply_defaults`
  (`apply_defaults`), which is why operator construction, `default_args`
  precedence, and deprecation warnings all live at that seam.
- The serialization contract: `DAG._DAG__serialized_fields` is derived as the set
  of `attrs` fields **minus** a hand-maintained exclusion list, and
  `BaseOperator.get_serialized_fields()` is derived from a throwaway instance's
  `vars()` **minus/plus** curated sets. A field that is not in that set does not
  survive serialize/deserialize — and the SDK class must stay in parity with its
  airflow-core serialization counterpart (see `serialization/adr/0003`).
- The mapping surface: `partial()` freezes the non-mapped kwargs and returns an
  `OperatorPartial`; `.expand()` / `.expand_kwargs()` produce a `MappedOperator`;
  `XComArg` is how a downstream task references an upstream task's output and how
  the graph learns the dependency. These are all part of the AIP-42 dynamic-task-
  mapping public API.
- The deprecation discipline: user-facing removals go through a
  `warnings.warn(..., RemovedInAirflow4Warning, stacklevel=…)` cycle first — the
  parameter keeps working while it warns — never a hard removal in one release.
- Distribution independence: this package must import only from `airflow.sdk`
  (and stdlib / attrs / typing), **never** `airflow.models` or airflow-core ORM
  (guarded by the `check_core_imports_in_sdk` prek hook).

## Before opening a PR here — authoring-agent guard

**This is a critical, expensive-to-review area: it is the public authoring API
whose backward compatibility every deployment depends on.** If you are an agent
preparing a change here on behalf of a person, first judge whether the change can
be **demonstrated not to break existing Dags**: have you written a Dag using the
old form of the API you are touching, confirmed it still parses and runs, and
shown the field still survives a serialize/deserialize round trip (`attrs` fields
that fall out of `__serialized_fields` disappear silently)? Every deployment in
the world authors against this surface — "the tests pass" is a weaker claim here
than anywhere else in the repo.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`). A change to what a
signature _means_ — not just what it does internally — needs an AIP or a devlist
thread, not a bare PR.

## Review criteria

Mined from real review discussion on ~292 merged and ~82 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed.
**If you are preparing a change here, treat this as a pre-flight checklist and
fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Public-API backward compatibility (the defining concern here):**

- [ ] **Don't break an existing signature — deprecate, don't remove.** A public
      parameter, default, class, or decorator that shipped in a release keeps
      working through a `warnings.warn(..., RemovedInAirflow4Warning)` cycle; you
      do not rename or delete it in place. A no-op-with-warning shim is the pattern
      (e.g. `sla`), not a hard removal.
- [ ] **A semantic change needs an AIP / devlist, not a bare PR.** Changing what a
      construct _means_ to author code — new authoring surface, a different default
      behaviour, a new validation that rejects previously-accepted Dags — is an
      architecture decision. New authoring APIs here came through AIPs (AIP-76
      partitions, AIP-105 retry policies, AIP-103 accessors), not a drive-by diff.
- [ ] **Renames only for still-unreleased / experimental surface**, and rename
      everywhere at once (authoring class, serialization, docs, examples) — a rename
      of a released name is a compatibility break, not a cleanup.
- [ ] **New validation must fail _at parse time_ with a clear author-facing error**,
      not deep in the scheduler or worker — and must not reject Dags that were valid
      before unless that is the deliberate, discussed intent.

**Why changes here get closed unmerged (from the closed-PR record):**

- [ ] **Map the proposal onto the existing model before adding a name.** State
      which existing construct — `data_interval_start` / `data_interval_end`, the
      timetable, `default_args`, an existing accessor — does _not_ cover the need.
      The most common closure here is "that already exists": a Dag-level
      `target_date` was refused because it is the data interval; a `safe_dag()`
      wrapper because it matches nothing the Dag model has (see `adr/0004`).
- [ ] **Don't add authoring surface for a capability the system can't honour.**
      A listing verb for variables was refused because the secrets-backend
      contract is get-by-key — the API would have promised behaviour no backend
      implements.
- [ ] **Search for an in-flight PR on the same issue first, and link it.**
      Duplicate parallel work is a leading closure reason here; several PRs were
      withdrawn once an already-assigned one surfaced. Prefer reviewing or
      building on the existing PR to opening a near-identical second one.
- [ ] **A decoupling change must remove the dependency, not relocate it.**
      Threading a caller-supplied `session` (or any airflow-core object) through
      a signature to delete an import makes the coupling stronger, not weaker —
      and the import hook still goes green. Say what the package needs at run
      time after the change (see `adr/0005`).
- [ ] **A mapped input must yield an expansion count.** Length-changing
      operations on `XComArg` (filter, dedupe, take) are not addable: the
      scheduler materialises one task instance per map index and needs the number
      up front. Changing that contract is AIP-88 work, not an authoring-class
      change (see `adr/0006`).
- [ ] **Split changes that span several entry points.** One PR touching the core
      model, the REST API, the SDK, and the CLI is routinely sent back to be
      separated; scope each PR to one surface.
- [ ] **No blanket mechanical sweeps of this package.** A near-equivalence
      (`type(self)` vs `self.__class__`) that differs in edge cases needs
      per-site justification; a package-wide rewrite is closed as change for its
      own sake.

**Serialization parity & lossless round-trip:**

- [ ] **Thread any new definition field into the serialized-field set.** A new
      `attrs` field on `DAG`, or attribute on `BaseOperator`, must be reflected in
      `DAG._DAG__serialized_fields` / `get_serialized_fields()` (add it, or add it
      to the exclusion list with a reason) — otherwise it silently round-trips to a
      default and never reaches the scheduler/worker.
- [ ] **Keep the SDK definition in parity with its airflow-core serialization
      counterpart** — the `check-...-in-sync` hook compares field-for-field; declare
      fields in the **class body**, don't hide them in `__init__` or a computed list
      (see `serialization/adr/0003`).
- [ ] **Prove the round-trip in a test** — construct, serialize, deserialize, and
      assert the authored value survived; a unit test that only builds the object
      in-process misses the field that never serialized.

**Distribution boundary (architecture invariant, not a preference):**

- [ ] **No `airflow.models` / airflow-core ORM import from this package** — the
      authoring surface stays importable as a standalone distribution; the
      `check_core_imports_in_sdk` hook enforces it. Don't blanket-add ignore markers
      to silence it; move the shared piece into the SDK instead.
- [ ] **`airflow.sdk` is the supported import path** — new public authoring symbols
      are exported from `airflow.sdk` (and its lazy `__getattr__` map), not left
      reachable only via a deep `airflow.sdk.definitions.…` path.

**Mapping, TaskGroup & graph correctness:**

- [ ] **`partial()` / `expand()` changes preserve the mapped-vs-unmapped split** —
      non-mapped kwargs are frozen on the partial; a change must not let a mapped
      argument leak into the un-mapped set or collapse a single-expansion group to a
      bare value.
- [ ] **`XComArg` keeps the dependency edge** — a change to how a task references
      upstream output must still register the graph edge, or the scheduler builds
      the wrong DAG.
- [ ] **`TaskGroup` graph operations stay correct and bounded** — topological sort
      / prefix handling must not regress ordering for reverse-declared Dags or add a
      per-node quadratic sweep.

**Code quality reviewers consistently require:**

- [ ] **Imports at module top**; local imports only for genuine circular-import
      reasons (the `DagContext` / CLI back-references here are the common cases — say
      why). **No heavy work at import time** — the authoring package is imported by
      every Dag file.
- [ ] **No function calls in default arguments** (ruff `B008`) — build mutable/
      computed defaults inside the body, not in the signature.
- [ ] **Action-verb / intent-revealing names**; don't shadow builtins;
      underscore-prefix non-public members; branch on a property/method rather than
      `isinstance` where the model already exposes one.
- [ ] **No duplicated logic** — reuse existing helpers; a third copy of a
      derivation will drift from the serialized-field set.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new path and fails without the change** — for
      authoring changes that means going through real serialize/deserialize (and,
      for mapping, real expansion), not constructing the object in-process and
      asserting on attributes; mocks use `spec`/`autospec`; assert on structured
      `caplog`, not substrings.
- [ ] **Backward compatibility** for the serialized shape — a released serialized
      field can't be ret-conned; version-gate and keep serialization in sync.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (task-sdk ships in `airflow-core`, so its newsfragments live in
      `airflow-core/newsfragments/`); not for internal refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of testing
      — low-effort / mass-AI-generated / near-duplicate parallel PRs get closed.
      Track deferred work in a GitHub issue; take contentious authoring semantics to
      the devlist / an AIP / a second reviewer.

> Mined from PR review history; the sample skews to the Airflow-3 era (the Task
> SDK and the `airflow.sdk` authoring surface are 3.x constructs), so pre-3.0
> authoring conventions are under-represented. Extend as new patterns emerge, and
> add an equivalent `## Review criteria` section to the `AGENTS.md` of every other
> area over time.

## Expectation for large changes

Discuss the approach first — in an issue, on the dev list, or as an AIP — before a
large PR. The public-API compatibility contract and the serialization-parity
invariant are best aligned on _before_ the code, not during review: a signature or
default that ships is one this project then has to support and deprecate on a
release cadence, not something a later PR can quietly take back.
