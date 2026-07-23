---
triage_review_imbalance:
  area: serialization
  criticality: critical          # whole area is backward-compat-critical — every file matters
  review_difficulty: expert
  structural_risk_paths:         # the serialization format itself
    - "serialized_objects.py"
    - "json_schema.py"
    - "schema.json"
    - "encoders.py"
    - "decoders.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "bolkedebruin"]   # internal signal only — never @-mentioned
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# DAG Serialization — Agent Instructions

Turns Dags and their objects into the JSON stored in the metadata DB and reads
them back. This is **backward-compatibility-critical**: a scheduler, API
server, and workers on adjacent versions must all read each other's serialized
data. A change that silently drops or reshapes a field can break running
deployments during an upgrade, with no error at write time.

## Why changes here are expensive to review

- Forward/backward compatibility across independently-deployed components is
  the default requirement, not an edge case.
- A new model field is invisible unless it is added to the serialized field
  set and round-trips through serialize → deserialize.
- The JSON schema is a contract; loosening or reshaping it can accept data an
  older reader can't.

## Knowledge a reviewer (and a substantial contributor) needs

- How `serialized_objects.py` enumerates and (de)serializes fields, and the
  `get_serialized_fields()` contract on operators/models.
- The `schema.json` / `json_schema.py` validation contract.
- Version gating and how old serialized blobs are read by new code (and vice
  versa).

## Before opening a PR here — authoring-agent guard

**This is a backward-compatibility-critical, expensive-to-review area.** If you
are an agent preparing a change here on behalf of a person, first judge whether
the change can be **demonstrated across versions, not just round-trip**: have you
shown that a blob serialized by the _old_ code still deserializes under the new
code, and that a Dag with the field you touched survives the full
serialize/deserialize cycle with nothing silently dropped? A field missing from
`get_serialized_fields()` round-trips perfectly in a unit test and vanishes in
production.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~227 merged and 75 closed-unmerged
serialization PRs. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back
with the specific gaps. Ordered by how often reviewers raise each.

**Is this the right change at all?** _(the largest class of closed PR here)_

- [ ] **A new serialized attribute must not restate an existing concept.** Name the
      alternatives you considered — `data_interval_start` / `data_interval_end`,
      `logical_date`, `run_type`, the timetable, `bundle_version` / `DagVersion`,
      `params` — and why each is insufficient. A "processing date" field was declined
      outright because the data interval already is that concept, produced by the
      timetable, which is the supported extension point (ADR 0005).
- [ ] **No multi-level configuration hierarchy over serialized state.** Precedence
      rules across three places to set which Dag version a run executes against did
      not survive review; one place to set a value.
- [ ] **Fix the generic helper, not the type that revealed the defect.** Two separate
      PRs special-casing one parameter type's unstable repr were closed and the work
      consolidated into `serialize_template_field` — the memory address was never a
      property of that type, it was a property of the fallback path (ADR 0004).
- [ ] **Translate errors once, at the framework boundary.** Per-route validation plus
      a config knob plus a new exception class was rejected in favour of a single
      registered handler for the underlying database error on both the REST and
      execution APIs, which then covers every existing and future write endpoint.
- [ ] **Search for in-flight work before proposing a serialized attribute** — these
      are frequently being designed in parallel as part of a larger effort, and
      duplicates get closed.

**Compatibility & determinism (the core of this area):**

- [ ] **Serialized output is deterministic** — no memory addresses, no unstable
      `str(obj)` fallbacks. Any `<... at 0x...>` in the JSON creates a new
      `DagVersion` every parse cycle. Fix **all** paths that can carry the value
      (task- _and_ DAG-level `default_args`, mapped _and_ non-mapped), not just
      the one in the diff.
- [ ] **New serialized fields are forward/backward compatible** — optional,
      defaulting to `None`, ignorable by an older reader. State the compat
      reasoning in the PR (this is the accepted pattern; e.g.
      `disable_bundle_versioning`).
- [ ] **Core and Task-SDK definitions stay in sync** — the
      `check-...-in-sync` prek hook enforces field parity. Keep the class-body
      annotation rather than editing the sync hook.
- [ ] **`serialize()` emits every field the constructor consumes** — otherwise
      the object silently reverts to defaults after a triggerer/worker restart.
- [ ] **When a run is pinned to an older Dag version, derive ALL metadata**
      (params, timetable, deadlines, allowed run types) from the _resolved_
      version — never the live/latest Dag. Each divergence needs a cross-version
      test.
- [ ] **Compat fallback code is read-only and time-boxed** — use `.get()` not
      `.pop()` (safe on re-invocation), and comment when it can be removed (N+1).

**Tests:**

- [ ] **Round-trip / symmetry test through `DagSerialization`, and it must fail
      without the change.** Reject tests that pass with the bug present (e.g. the
      same lambda reused in-process reuses its address — build two equivalent
      dicts with _different_ callable instances).

**Correctness of the change itself:**

- [ ] **When adding sorting to stabilize output, guard non-orderable/mixed key
      types AND fix the full hashing path** (`SerializedDagModel.hash()`'s
      `sorted()` / `json.dumps(sort_keys=True)`), not just the helper.
- [ ] **Hoist sentinel checks (`ARG_NOT_SET`) above field-name dispatch** so
      they cover every field — a `*_date`-name heuristic routing a sentinel into
      `_deserialize_datetime` is the anti-pattern.
- [ ] **Behaviour-changing validation on already-serialized/deployed Dags needs
      an opt-in / migration path** — it can break every existing user in the
      field on upgrade.
- [ ] **Don't add deserialization "trust/validation" guards where the security
      model already treats the code as trusted** (the triggerer runs user code by
      design — restricting class loading is not a real boundary).
- [ ] **Removing a legacy serialization capability requires proving nothing still
      uses it — and if it goes, it must fail loudly.** An attempt to drop support for
      re-serializing an already-serialized operator stalled because core was still
      relying on it unintentionally (the test suite said so), and reviewers required
      that a removed path _raise_ rather than silently coerce the value to a string:
      a silent degradation here surfaces as a confusing error much later, in a
      different component.
- [ ] Reliance on **private third-party deserialization internals** is documented
      inline with an upstream link.

**Scope & process:**

- [ ] **Keep the fix strictly surgical** — revert unrelated churn (config
      templates, `uv.lock`, extra methods/args); for hash stability, sort only
      for hashing and persist the original unsorted data. Split "don't crash on
      bad deserialization" (backportable) from new features — one PR per concern.
- [ ] **The `Serialization` CI suite must be green** — a serde change that fails
      the parametrized LowestDeps/Postgres/MySQL Serialization groups is
      auto-drafted and won't merge; green-on-your-machine is not enough.
- [ ] Imports at top of file (inline only for a genuine name collision like SDK
      `DAG` vs `airflow.models.dag.DAG` — and then document the reason).

> Mined from PR review history; note the explicit "bump the serialization
> schema version" pattern did not surface recently — compatibility is enforced
> via the optional-field-defaults pattern and the core↔SDK field-sync prek hook.
> Extend as new patterns emerge, and add an equivalent `## Review criteria`
> section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss first — compatibility strategy for a format change should be agreed
before the code.
