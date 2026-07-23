---
triage_review_imbalance:
  area: assets
  criticality: high              # base tier; identity + scheduling-fan-out paths promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "manager.py"
    - "evaluation.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["uranusjr", "Lee-W"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Assets (data-aware scheduling) — Agent Instructions

This directory holds the core-side machinery for data-aware scheduling (AIP-48
assets, extended by the AIP-73/74/75/76 asset-partition work). `AssetManager`
(`manager.py`) records asset events, resolves which Dags an event should wake,
and enqueues their runs; `AssetEvaluator` (`evaluation.py`) decides whether an
asset **condition** attached to a Dag's `schedule=` is satisfied. Between them
they translate "a producer touched this asset" into "these consumer Dag runs
must be created" — for **every** asset-scheduled Dag in a deployment. A defect
here silently under- or over-triggers runs cluster-wide (missed data-driven
runs, duplicated fan-out, or a wedged event queue), and because the identity of
"the same asset" is a normalized string contract, a change to that contract can
re-bucket already-persisted assets so that producers and consumers stop matching.

## Why changes here are expensive to review

- **Asset identity is a stored, normalized contract.** A consumer is wired to a
  producer by matching a normalized `(name, uri)` — persisted in `AssetModel`
  and in the schedule-reference tables. Changing normalization, validation, or
  equality/hashing re-buckets what counts as "the same asset": existing rows no
  longer match freshly-parsed ones, and the break is invisible in the diff
  (nothing throws — events just stop reaching consumers).
- **The event → queue path runs under real concurrency.** `register_asset_change`
  fans one event out to many consumer Dags and inserts queue / partition rows
  while other producers do the same; correctness depends on locking, on-conflict
  upserts, and per-dialect (Postgres / MySQL / SQLite) code paths staying in
  step. A change that looks right on SQLite can deadlock or double-insert on
  MySQL/InnoDB under load.
- **Scheduling decisions are evaluated from _serialized_ data, not user code.**
  `AssetEvaluator` walks `Serialized*` structures the Dag processor persisted;
  smuggling a live SDK object or a user callable into that path both breaks the
  scheduler's no-user-code boundary and desynchronises the SDK vs serialized
  class split.
- **Multi-team filtering is a two-sided AND.** Producer-side and consumer-side
  team checks must _both_ pass; getting the direction or the default wrong
  either leaks events across teams or silently drops legitimate ones.

## Knowledge a reviewer (and a substantial contributor) needs

- How an asset's `(name, uri)` becomes its scheduling identity: URI sanitisation
  / normalization (`_sanitize_uri`, the AIP-60 scheme normalizers) and name
  validation on the SDK side, persisted as `AssetModel`, matched via the
  `DagScheduleAsset*Reference` tables and `SerializedAssetUniqueKey`.
- The `register_asset_change` → `_queue_dagruns` flow: how one event resolves
  consumer Dags (direct asset, alias, and name/uri ref), skips paused Dags, and
  inserts `AssetDagRunQueue` / `AssetPartitionDagRun` rows — including the
  per-dialect upsert / row-lock paths and the APDR mutex.
- `AssetEvaluator`'s `singledispatch` over the serialized asset hierarchy
  (`SerializedAsset`, `SerializedAssetRef`, `SerializedAssetAlias`,
  `SerializedAssetBooleanCondition`) and why it operates on serialized,
  data-only structures rather than SDK objects.
- The multi-team model: `AssetAccessControl` (`producer_teams`,
  `consumer_teams`, `allow_global`), where `allow_producer_teams` /
  `allow_global_producers` are stored (the schedule-reference row, not the
  asset), and how `_filter_dags_by_team` combines producer- and consumer-side
  checks.
- The repo `CLAUDE.md` DB rules — keyword-only `session`, no `session.commit()`
  inside a function that takes a `session`, batched bulk writes with `LIMIT`.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area whose identity contract
and event fan-out touch every asset-scheduled Dag.** If you are an agent
preparing a change here on behalf of a person, first judge whether the change can
be **demonstrated as a triggered run**: have you wired a producer Dag to a
consumer Dag, emitted the asset event, and watched the consumer actually schedule
— with the identity you changed, and with events arriving _concurrently_ from
several producers? Asset identity is a stored contract: a normalization change
that looks harmless re-keys existing assets and silently unwires consumers that
were already scheduled against the old key.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on the ~38 merged and ~16 closed-unmerged PRs
that have touched this directory (a smaller sample than the scheduler/DFP areas,
so treat the ordering as indicative rather than statistically firm, and lean on
the linked ADRs where the PR evidence is thin) — the changes reviewers
repeatedly required, and the reasons changes here get closed. **If you are preparing a change here, treat
this as a pre-flight checklist and fix every applicable item _before_ opening
the PR.** Triage applies the same list: a PR that lands with unmet items is
drafted back to its author with the specific gaps. Ordered by how often
reviewers raise each.

**Asset identity & the normalization contract (the defining concern here):**

- [ ] **Do not change URI normalization, name validation, or asset
      equality/hashing without treating it as compatibility-sensitive.** The
      normalized `(name, uri)` is a stored matching key; a change re-buckets
      already-persisted assets so producers and consumers stop matching. Gate it,
      migrate existing rows, and prove old and new identities still resolve to
      the same asset (see `adr/0001`).
- [ ] **Match assets by their persisted identity, not an incidental string** —
      resolve consumers through the schedule-reference tables /
      `SerializedAssetUniqueKey`, and keep name-vs-uri handling consistent with
      how the schedule stores the reference.
- [ ] **New URI schemes / sanitizers must normalize deterministically** — same
      input always yields the same canonical URI; no host-, locale-, or
      ordering-dependent output that would make the same asset serialize two ways.

**Event fan-out, concurrency & DB correctness:**

- [ ] **Concurrent producers must not double-insert or deadlock.** Keep the
      per-dialect queue paths (`on_conflict_do_nothing` / `ON DUPLICATE KEY` /
      the SAVEPOINT slow path) in sync, hold the APDR mutex where two events can
      race the same `(target_key, target_dag)`, and prefer a single atomic
      upsert over per-row nested transactions on MySQL/InnoDB.
- [ ] **Respect paused / stale / inactive Dags and assets** — only queue runs
      for consumers that should actually receive them (`is_paused` filtering,
      inactive-asset handling); a change that widens the resolved set re-triggers
      Dags that should stay quiet.
- [ ] **Batch bulk writes with `LIMIT`, keyword-only `session`, no
      `session.commit()` inside a function that takes a `session`** — the event
      path already flushes in a specific order (event before queue rows); don't
      reorder or add an unbounded write that holds locks across the fan-out.
- [ ] **Bound partition fan-out** — a partition mapper that explodes one event
      into many downstream keys must stay under the configured cap and degrade
      (log / audit) rather than queue an unbounded number of runs.

**Asset vs asset event — the most common substantive error here:**

- [ ] **Know which `extra` you are touching.** `Asset.extra` describes the asset
      definition; `AssetEvent.extra` describes one update. Both are untyped JSON,
      so nothing catches the substitution — a PR that passed one where the other
      was expected was closed on exactly this point (see `adr/0005`).
- [ ] **Event-level data reaches consumers through event-level surfaces.** If a
      listener needs the producing task instance, the partition key, or the
      update payload, add an asset-event listener or field — do not redefine what
      an existing asset-level listener receives; that breaks integrations outside
      this repository.
- [ ] **Provenance rides on the event, not on identity** — `partition_key`,
      lineage, and producing-task references attach to the asset event and
      inherit downstream, and must not become part of the asset's normalized
      identity.
- [ ] **Asset identity survives indirection** — resolution through an
      `AssetAlias` preserves the asset's own `extra` and identity; the alias is a
      lookup, not a transformation.

**Replay and retry safety on the event path:**

- [ ] **Say what runs twice if the transaction is retried.** Listener hooks and
      the `asset.updates` metric are not transactional and fire before runs are
      queued; a deadlock fix was closed after review established both would
      re-run on replay (see `adr/0004`).
- [ ] **Name the backend for any deadlock or contention fix**, and state the
      behaviour under the other supported backends — MySQL and PostgreSQL differ
      in lock and upsert semantics here.
- [ ] **Don't relieve contention by narrowing lock scope** without stating which
      steps can now interleave; a proposal to release the row lock before event
      emission was closed rather than merged.
- [ ] **Check the failure actually reaches the caller as retryable** — a fix that
      assumes the task-side client retries is not a fix when the error surfaces
      as a plain task failure instead of a 5xx.

**Data-only scheduling boundary (architecture invariant, not a preference):**

- [ ] **Evaluate conditions from serialized, data-only structures** — extend
      `AssetEvaluator` over the `Serialized*` hierarchy; never evaluate a live
      SDK asset object or run a user callable in the scheduler-consumed path
      (see `adr/0002` and the scheduler / serialization ADRs it references).
- [ ] **Keep the SDK vs serialized asset class split** — authoring-side classes
      (`airflow.sdk...asset`) and the core `Serialized*` / `AssetModel` classes
      are deliberately separate; don't import SDK asset classes into the
      manager/evaluator or leak ORM models back to the SDK.

**Multi-team access control:**

- [ ] **Producer- and consumer-side team checks are a logical AND** — both must
      pass for a Dag to be queued; preserve the defaults (`allow_global`,
      teamless source/consumer handling) and the storage location
      (`allow_producer_teams` on the schedule reference, not the asset).
- [ ] **API-produced events must carry teams explicitly** — `source_is_api`
      events resolve teams from the caller, not from a producing Dag's bundle;
      don't assume a task-instance path.

**Code quality reviewers consistently require:**

- [ ] **Listener / notify hooks must not break the transaction** — every
      listener invocation is wrapped so a plugin exception is logged, not
      propagated into the asset-event write.
- [ ] **Don't swallow real errors with a broad `except`** — narrow to the
      classes you mean (e.g. the SQLite lock-busy retry, mapper failures that
      should audit-log and continue) so genuine DB / refactor bugs still surface.
- [ ] **Imports at module top**; local imports only for genuine
      circular-import reasons (the `DagModel` re-imports here are that case — say
      why). **No heavy work at import time.**
- [ ] **Action-verb / intent-revealing names**; reuse the existing resolve /
      queue helpers rather than a third copy of the fan-out that will drift.
- [ ] **Right severity** — a misconfigured partition mapper or a missing
      partition key audit-logs and degrades that event; it does not crash the
      manager.

**Tests, compatibility, process:**

- [ ] Test **exercises the real event/evaluation path and fails without the
      change** — go through `register_asset_change` / `AssetEvaluator` on the
      serialized structures, not a hand-built in-process object; cover more than
      one dialect where the change is dialect-specific; mocks use
      `spec`/`autospec`; assert on structured `caplog`, not substrings.
- [ ] **Backward compatibility** for persisted asset shapes and identities —
      can't ret-con a released migration; version-gate and migrate rows when the
      identity/normalization contract moves.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing**
      changes (asset-authoring surface, scheduling semantics) — not internal
      refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of
      testing — low-effort / mass-AI-generated / near-duplicate parallel PRs get
      closed. Take contentious scheduling semantics to an AIP / the devlist, not
      a bare PR.

> Mined from PR review history on a deliberately small sample (~38 PRs touch
> this directory; much asset-authoring logic and URI normalization lives in
> `task-sdk`, so it is under-represented here). The sample skews to the
> Airflow-3 / AIP-73–76 partition era. Treat the ordering as indicative, extend
> as new patterns emerge, and add an equivalent `## Review criteria` section to
> the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large
PR. The identity/normalization contract, the event fan-out concurrency model,
and the data-only evaluation boundary are best aligned on _before_ the code,
not during review.
