---
triage_review_imbalance:
  area: timetables
  criticality: high              # base tier; the interval/cron engine promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "base.py"
    - "_cron.py"
    - "interval.py"
    - "trigger.py"
    - "simple.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["uranusjr", "Lee-W"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Timetables (scheduling & data intervals) — Agent Instructions

This directory holds the timetables — the objects that decide **when** a Dag
runs and **what data interval** each run covers. A `Timetable`
(`base.py`) answers `next_dagrun_info()` for the scheduler and
`infer_manual_data_interval()` for manual/backfill runs; the concrete engines
live in `_cron.py` / `interval.py` (calendar and delta schedules),
`trigger.py` (cron-trigger and partitioned schedules), `simple.py`
(`@once` / `@continuous` / null / asset-driven), plus `events.py` and
`assets.py`. The scheduler evaluates these from the **serialized** Dag, so a
defect here silently mis-schedules runs — wrong interval, missed run, duplicate
run, or drift across a DST boundary — for **every** deployment using that
schedule, and the symptom usually appears far from the diff.

## Why changes here are expensive to review

- The scheduler drives timetables from _serialized_ data and never re-imports
  the author's module (see `adr/0001`). A change that makes
  `next_dagrun_info()` depend on live author callables, wall-clock time, or any
  process-local state is not locally obvious from the diff but breaks the
  boundary that keeps the scheduler trustworthy and fast.
- The **serialized form is a compatibility contract** (see `adr/0002`): a
  timetable `serialize()`s its own data and is looked up by classpath on
  `deserialize()`. An older scheduler must still read a timetable serialized by
  a newer one, so a shape change that looks harmless can strand runs after an
  upgrade or rolling deploy.
- **Timezone / DST math** is the recurring correctness trap (see `adr/0003`).
  Interval alignment, the cron "fold hour" around a backward DST transition, and
  catchup/manual-interval inference are all easy to get subtly wrong and _hard
  to test_ — the bug only manifests twice a year, in one timezone, on one
  schedule shape.
- Interval boundaries are **off-by-one-prone**: `_align_to_prev` /
  `_align_to_next` / `_get_prev` / `_get_next` interact in ways where a single
  misplaced call shifts every interval by one period or widens a sub-day window
  to a whole day.

## Knowledge a reviewer (and a substantial contributor) needs

- The `Timetable` protocol in `base.py`: `next_dagrun_info()` (and the
  `next_dagrun_info_v2` / `next_run_info_from_dag_model` wrappers),
  `infer_manual_data_interval()`, `serialize()` / `deserialize()`, `summary`,
  `description`, and the `DataInterval` / `DagRunInfo` / `TimeRestriction`
  named tuples that flow through them.
- What `last_automated_data_interval` means and when it is `None` — only on the
  very first schedule of a Dag, before any run exists — and how `restriction`
  (`earliest` / `latest` / `catchup`) bounds the answer.
- The cron engine in `_cron.py`: `croniter` usage, `make_naive` /
  `make_aware` / `convert_to_utc` round-tripping through the timetable's own
  timezone, and the `_covers_every_hour` "fold hour" handling around DST.
- The interval alignment helpers in `interval.py`
  (`_align_to_prev` / `_align_to_next` / `_get_prev` / `_get_next`) and why
  `_get_prev(_align_to_next(...))` is _not_ interchangeable with them.
- How the serialized timetable is registered and reconstructed by classpath,
  and which timetables carry extra serialized state (`EventsTimetable` events,
  `CronTriggerTimetable` / partitioned schedules) that must round-trip.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area whose correctness bugs
surface far from the diff (a wrong interval, a missed run, a DST-only drift).**
If you are an agent preparing a change here on behalf of a person, first judge
whether the change can be **demonstrated as a schedule, not as an argument**: can
you write out the concrete sequence of runs the timetable produces before and
after — including the first run (`last_automated_data_interval is None`), a
catchup window, and a DST transition in a non-UTC timezone — and show it
round-trips through `serialize()` / `deserialize()`? Interval bugs do not fail
loudly; they emit a run at the wrong time months later.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** with a concrete reproduction, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion across the ~44 merged and ~43 closed-unmerged
PRs touching this area — the changes reviewers repeatedly required, and the
reasons changes here get sent back. **If you are preparing a change here, treat this as a pre-flight checklist
and fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Timezone / DST / interval correctness (the defining concern here):**

- [ ] **All datetimes are timezone-aware and computed in the timetable's own
      timezone** — round-trip through `make_naive` / `make_aware` /
      `convert_to_utc` as `_cron.py` does; never do cron/delta math on a naive
      or a blindly-UTC-coerced value. `DataInterval.start` / `end` and
      `DagRunInfo.run_after` **MUST** be aware.
- [ ] **DST transitions are handled, not assumed away** — a backward transition
      folds an hour (the `_covers_every_hour` / fold-hour path); a forward
      transition skips one. Reason about both, and about non-UTC schedules,
      before changing any `_get_next` / `_get_prev` / alignment code.
- [ ] **Interval boundaries stay put** — adding or moving an
      `_align_to_prev` / `_align_to_next` / `_get_prev` / `_get_next` call must
      not shift every interval by one period or widen a sub-day window to a whole
      day. Prove start/end land where intended with an explicit example.
- [ ] **Catchup and manual-interval inference stay consistent** —
      `infer_manual_data_interval()` and `next_dagrun_info()` must agree on what
      interval a given `run_after` belongs to; a manually-triggered run must not
      land in a different interval than the scheduled one would.

**Every consumer of timetable output, not just the scheduler:**

- [ ] **Name the consumers your change affects** — scheduled runs, manual
      triggering, backfill, `airflow dags clear`, the REST API, and the UI all
      read timetable output by different paths. The long tail of fixes in this
      area is "the scheduler path worked, another consumer was missed" (see
      `adr/0004`).
- [ ] **New fields on `DagRunInfo` are backward compatible** — consumers that
      predate the field must keep working; adding one is a compatibility event,
      and it has already required a dedicated backcompat fix once.
- [ ] **Consumers must not re-derive interval arithmetic** — backfill, clear, and
      trigger paths take the interval the timetable computed. Re-deriving day or
      window boundaries is where the wrong-day-cleared and widened-window defects
      came from.
- [ ] **CLI and REST API stay in parity** for a timetable capability, or the gap
      is stated deliberately.
- [ ] **Land the SDK and core sides together** where the timetable class exists
      on both sides of the distribution split; a one-sided change schedules
      correctly and displays or backfills wrongly.
- [ ] **Check for an in-flight PR before opening one.** The most common closure
      reason in this area's closed record is a near-identical parallel PR that
      was further along — the same capability has twice been attempted by two
      contributors at once.

**Changing scheduling semantics — where PRs here actually die:**

- [ ] **Settle the interface before writing the code.** A change to catchup, to
      the first run produced after unpausing, or to how a schedule is expressed
      goes to the dev list or an AIP first, and the PR links that discussion.
      Working patches for exactly these behaviours have been closed — one after a
      year open — not on their code but because no interface had been agreed
      (#38168, #35392). See `adr/0005`.
- [ ] **Do not add a config option that changes when Dag runs fire.** Schedule
      behaviour must travel with the Dag file, so express it as a timetable or as
      a value of a Dag-declared parameter. Supplying the _default_ of a parameter
      the Dag can override (as `scheduler.catchup_by_default` does) is the one
      accepted shape.
- [ ] **Check for an adjacent open PR and resolve the two together.** Two patches
      touching neighbouring parts of the same semantics are one interface
      question; #38168 was held precisely because #35392 was changing the other
      half.
- [ ] **A composition point that accepts a user-supplied object needs its
      registration story up front.** Anything a timetable composes is arbitrary
      code the scheduler invokes — say how `deserialize()` resolves the class and
      what constrains it. The composable-timetables draft (#28757) stalled on
      exactly this question.
- [ ] **Own the proposal to a conclusion.** Accumulated "+1" comments do not
      advance a schedule feature; multiple cron expressions have been attempted
      twice, years apart, and lapsed both times (#24733, #35337).

**Determinism & the scheduler boundary:**

- [ ] **`next_dagrun_info()` is pure and side-effect-free** — its answer depends
      only on `last_automated_data_interval`, `restriction`, and the timetable's
      own serialized fields. No `datetime.now()`, no DB/network/file access, no
      RNG, no branching on live author callables (see `adr/0001`). The scheduler
      evaluates it from serialized data and must never run user code.
- [ ] **Same input → same output** — repeated evaluation with the same arguments
      yields the same `DagRunInfo`; a nondeterministic timetable corrupts
      scheduling and version history.

**Serialization & backward compatibility:**

- [ ] **`serialize()` / `deserialize()` round-trip every field the timetable
      needs** — a new attribute that affects scheduling must be persisted and
      restored, or an upgraded scheduler will mis-schedule a Dag serialized by an
      older one (`EventsTimetable` description / isoformat separator were exactly
      this class of miss). Keep the serialized shape JSON-serializable and stable
      (see `adr/0002`).
- [ ] **An older scheduler must still read a newer serialized timetable** — a
      shape change needs a compatibility path (version-gate / shim / additive
      field), never a silent rename or type change of an existing key. Custom
      timetables are looked up by classpath — do not move or rename a class
      without a compat alias.
- [ ] **Registrable-interface parity** — a new built-in timetable implements the
      full `Timetable` protocol (`summary`, `description`, `serialize` /
      `deserialize`, `next_dagrun_info`, `infer_manual_data_interval`) and, if it
      mirrors an SDK class, keeps the two definitions in sync.

**Code quality reviewers consistently require:**

- [ ] **Reuse the existing engine mixins** — extend `CronMixin` / `DeltaMixin` /
      `_DataIntervalTimetable` rather than adding a third copy of alignment logic
      that will drift.
- [ ] **Imports at module top**; local imports only for genuine circular-import
      reasons (as `compute_rollup_fingerprint` documents). No heavy work at
      import time.
- [ ] **Action-verb / intent-revealing names**; don't raise a bare
      `AirflowException` — use a specific type (`AirflowTimetableInvalid` from
      `validate()`, `ValueError`, or a dedicated class).
- [ ] **Right severity** — an invalid user schedule surfaces through
      `validate()` as `AirflowTimetableInvalid`, it does not crash the scheduler.

**Tests, compatibility, process:**

- [ ] Test **exercises the real path and fails without the change** — drive
      `next_dagrun_info()` / `infer_manual_data_interval()` (and the
      serialize → deserialize round-trip where relevant) directly; use
      `time_machine` for time-dependent cases and parametrize DST / timezone /
      catchup variants rather than testing one happy path. Mocks use
      `spec`/`autospec`; assert on structured `caplog`, not substrings.
- [ ] **Backward compatibility** for serialized timetable shapes — can't ret-con
      a released form; version-gate and keep serialize/deserialize in sync.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (new schedule behaviour), not internal refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of
      testing — low-effort / mass-AI-generated / near-duplicate parallel PRs get
      closed. Take contentious scheduling semantics to the devlist / a second
      reviewer.

> Mined from PR review history. The merged sample skews to the Airflow-3 era
> (this module was reorganised for the SDK/serialization split and AIP-76
> partitions); the closed-unmerged sample reaches back to 2022 and supplies most
> of the scheduling-semantics signal above. Extend as new patterns emerge, and add
> an equivalent `## Review criteria` section to the `AGENTS.md` of every other
> area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The determinism boundary, the serialized-form compatibility contract, and the
timezone/DST invariants are best aligned on _before_ the code, not during review.
