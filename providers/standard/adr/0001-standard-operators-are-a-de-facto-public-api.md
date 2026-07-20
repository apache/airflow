<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 1. The standard operators are a de-facto public API with the widest install base

Date: 2026-07-20

## Status

Accepted

## Context

The `task-sdk` ADRs govern the *base classes* — `BaseOperator`,
`BaseSensorOperator`, `BaseHook` — and the authoring surface users import from
`airflow.sdk`. This ADR governs something different: the **concrete operators
built on those base classes** and shipped from this provider.

`PythonOperator`, `PythonVirtualenvOperator`, `ExternalPythonOperator`,
`BashOperator`, `EmptyOperator`, `TriggerDagRunOperator`, the branch and
short-circuit operators, and the core sensors (`ExternalTaskSensor`,
`DateTimeSensor`, `TimeSensor`, `TimeDeltaSensor`, `FileSensor`,
`DayOfWeekSensor`) are not "a provider's operators" in the sense that
`providers/amazon` has operators. They are what a Dag is *made of*. The `@task`,
`@task.bash`, `@task.virtualenv`, `@task.branch`, `@task.short_circuit` and
`@task.sensor` decorators — the way most modern Dags are actually written —
resolve to classes in this package. Provider-to-provider coupling is modest —
8 distributions import it and 1 declares it as a runtime dependency — but that
understates the reach in the direction that matters: a deployment that installs
no other provider at all still installs this one, and every Dag that uses
`@task` lands here.

That reach is compounded by history. These modules were **airflow-core** until
the 3.x split relocated them here — `EmptyOperator` (#46231), `SmoothOperator`
(#47530), `utils/weekday.py` (#47892), the standard decorators (#48683), during
the wider core source move (#47798). Users did not opt into a new package; the
code they had already written kept working because the released behaviour was
preserved across the move. Their Dags still say `PythonOperator(...)` with the
same kwargs they wrote in Airflow 2.

Two properties make a change here more dangerous than the same change in
another provider:

- **The blast radius is not enumerable.** A reviewer of an `amazon` PR can ask
  "who uses this operator". Here the answer is everyone, and none of them are
  in the PR to object.
- **The release cadence is independent of core**, against a declared floor of
  `apache-airflow>=2.11.0`. A change ships to a user on Airflow 2.11 and a user
  on current main from the same wheel, on a provider wave that has nothing to do
  with their Airflow upgrade. "Works on my main checkout" is not evidence.

The recurring pressure is a specific shape of PR: a contributor notices that a
long-standing behaviour looks wrong — a default that surprises, a return value
handled oddly, a parameter whose semantics are inconsistent — and fixes it
directly. The fix may well be *correct*. But every Dag that depends on the old
behaviour has been quietly depending on it for years, and the "fix" reaches all
of them at once. Correcting a long-standing behaviour in place is itself a
breaking change.

Airflow already has the discipline for this at the base-class level, and it is
applied here too: additive parameters for new capability, and a deprecation
cycle — the old spelling keeps working and warns — for anything retired.

## Decision

Treat every released operator, sensor and decorator in this provider as a public
API whose installed base is the entire Airflow ecosystem. Concretely:

- **New capability is additive.** A new opt-in parameter, defaulting to the
  existing behaviour — never a changed default on an existing one.
- **A released parameter, default, return value, or skip/branch behaviour is not
  changed in place.** It is retired through a deprecation cycle that keeps the
  old spelling constructing and running while it warns.
- **"Fixing" a long-standing behaviour follows the same path as removing it.**
  Being right about the desired semantics does not exempt the change from a
  deprecation path, a flag, or a devlist discussion — it only establishes where
  the path should end.
- **Version-specific behaviour is gated through the provider's own
  `version_compat.py`**, and both sides of the gate are exercised, because the
  same wheel runs on the declared floor and on current main.
- **An underscore prefix is not, by itself, permission to rename.** The line is
  *method on a released class* versus *attribute or local*. A released operator
  or sensor is subclassable, so an underscore-prefixed **method** — the
  `_handle_execution_date_fn` shape — is de-facto public: users override it and
  call it to work around gaps, and changing its name or signature is a breaking
  change treated like a public one. A private **attribute** with no override
  semantics — the `_defer` shape — may be renamed freely. Where this ADR and the
  criteria in `providers/standard/AGENTS.md` once disagreed, the criteria were
  right and this bullet was corrected to match: #52237 tried the free-rename
  reading on `_handle_execution_date_fn`, two reviewers rejected it as breaking,
  and the author closed it.

## Consequences

- Dags keep running across a provider upgrade, which is the property that makes
  the independent provider release cadence tolerable at all.
- The package accumulates opt-in parameters and deprecated aliases rather than
  converging on a clean API. That cost is accepted deliberately: the alternative
  is breaking Dags the project cannot see or migrate.
- Genuinely wrong long-standing behaviour takes longer to correct — it needs a
  deprecation cycle and often a devlist thread — and reviewers are expected to
  hold that line even when the proposed semantics are better.
- Reviewers can reject an in-place behaviour change on blast-radius grounds
  alone, without having to enumerate which Dags break.

A change **violates** this decision when it:

- alters a released default, return value, or skip/branch behaviour of an
  operator, sensor, or decorator in place, instead of adding an opt-in parameter
  or going through a deprecation cycle;
- removes or renames a released public parameter, class, or decorator — or an
  underscore-prefixed **method** on a released operator or sensor, which is
  overridable and therefore reachable — without keeping the old spelling working
  behind a warning;
- justifies an in-place behaviour change as "a fix" for semantics that have been
  released and depended on, without a deprecation path or a devlist discussion;
- introduces behaviour that only works on one supported Airflow version without a
  `version_compat.py` gate covering the other, or gates it without exercising
  both sides;
- narrows what an operator accepts — new validation that rejects an input a
  released version accepted — without a `providers/standard/docs/changelog.rst`
  entry naming the newly-rejected input. (Validation that catches an authoring
  mistake is welcome per this area's ADR 6; what this bullet requires is that
  users are told which Dags will now fail.)

## Evidence

- #50864 ("Merging `TimeSensorAsync` with `TimeSensor`") and #51133 ("merge
  `TimeDeltaSensorAsync` to `TimeDeltaSensor`") — the canonical shape: the
  capability was consolidated onto `deferrable=True`, and the old classes were
  kept constructing and delegating behind an
  `AirflowProviderDeprecationWarning` rather than deleted. #59651 then removed
  only the *documentation* for `TimeDeltaSensorAsync`, leaving the class itself
  in place.
- #50301 — "Adding deprecation notice for `get_current_context` in std
  provider": a symbol that moved to the SDK kept working here behind a warning
  instead of being dropped.
- #48214 ("Add `fail_when_dag_is_paused` param to `TriggerDagRunOperator`"),
  #62259 ("Add `run_after` to `TriggerDagRunOperator`") and #60810 ("Add note
  support to `TriggerDagRunOperator`") — new capability landing as opt-in
  parameters on a heavily used operator, none of them changing existing
  behaviour.
- #56965 ("Throw `NotImplementedError` when `fail_when_dag_is_paused` is used in
  `TriggerDagRunOperator` with Airflow 3.x") and #67726 ("Fix
  `TriggerDagRunOperator` `fail_when_dag_is_paused` on Airflow 3.2+") — the cost
  of a parameter whose behaviour differed by core version: two follow-up PRs to
  make the version gate honest.
- #46306 — "Removing feature: send context in venv operators (using
  `use_airflow_context`)": a removal done deliberately as a release-boundary
  decision, not folded into an unrelated change.
- #58925 — "nit: rename `TriggerDagRunOperator._defer` to `deferrable`":
  **merged**. A private *attribute* with no override semantics is the narrow
  case where an in-place rename remains allowed.
- #52237 (with #52431) — the opposite outcome on an underscore-prefixed
  *method*: renaming `_handle_execution_date_fn` on `ExternalTaskSensor` was
  rejected by two reviewers ("It is breaking for `_handle_execution_date_fn`,
  and it could potentially be used by users") and closed by its author. This
  pair is why the boundary is method-vs-attribute rather than underscore-vs-not.
- #46633 — "`TriggerDagRunOperator` by defaults set logical date as null": a
  genuine default change, made as its own explicit, discussed change at an
  Airflow-3 boundary rather than as a drive-by fix.
- #46231, #47530, #47892, #48683, #47798 — the relocation of these operators,
  sensors, decorators and utilities out of airflow-core into this provider,
  which is why user Dags written against airflow-core imports are the installed
  base this decision protects.
