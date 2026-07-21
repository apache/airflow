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

The `task-sdk` ADRs govern the *base classes* (`BaseOperator`,
`BaseSensorOperator`, `BaseHook`) and the `airflow.sdk` authoring surface. This ADR
governs the **concrete operators built on those base classes** and shipped here.

`PythonOperator`, `PythonVirtualenvOperator`, `BashOperator`, `EmptyOperator`,
`TriggerDagRunOperator`, the branch/short-circuit operators, and the core sensors
(`ExternalTaskSensor`, `DateTimeSensor`, `TimeSensor`, `TimeDeltaSensor`,
`FileSensor`, `DayOfWeekSensor`) are what a Dag is *made of*, not "a provider's
operators" the way `providers/amazon` has them. The `@task`, `@task.bash`,
`@task.virtualenv`, `@task.branch`, `@task.short_circuit` and `@task.sensor`
decorators resolve to classes here. A deployment that installs no other provider
still installs this one, and every `@task` Dag lands here. History compounds the
reach: these modules were **airflow-core** until the 3.x split relocated them
(#46231, #47530, #47892, #48683, during the core source move #47798), and users'
existing Dags kept working because released behaviour was preserved across the move.

Two properties make a change here more dangerous than in another provider: the
blast radius is not enumerable (the answer to "who uses this operator" is everyone,
none in the PR to object), and the release cadence is independent of core against a
floor of `apache-airflow>=2.11.0` — the same wheel ships to a user on 2.11 and one
on current main, so "works on my main checkout" is not evidence. The recurring
pressure is a PR that notices a long-standing behaviour looks wrong and fixes it in
place; the fix may be *correct*, but every Dag has been depending on the old
behaviour for years, so correcting it in place is itself a breaking change. The
discipline, as at the base-class level: additive parameters for new capability, a
deprecation cycle (old spelling keeps working and warns) for anything retired.

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

- Dags keep running across a provider upgrade — the property that makes the
  independent release cadence tolerable at all.
- The package accumulates opt-in parameters and deprecated aliases rather than a
  clean API — deliberately accepted; the alternative is breaking Dags the project
  cannot see or migrate.
- Genuinely wrong long-standing behaviour takes longer to correct (deprecation
  cycle, often a devlist thread), and reviewers hold that line even when the
  proposed semantics are better.
- Reviewers can reject an in-place behaviour change on blast-radius grounds alone,
  without enumerating which Dags break.

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

- #50864, #51133 — the canonical shape: `TimeSensorAsync` / `TimeDeltaSensorAsync`
  consolidated onto `deferrable=True`, old classes kept delegating behind an
  `AirflowProviderDeprecationWarning`. #59651 removed only the *docs*, not the class.
- #50301 — `get_current_context` moved to the SDK but kept working here behind a
  warning.
- #48214, #62259, #60810 — new capability landing as opt-in parameters on
  `TriggerDagRunOperator`, none changing existing behaviour.
- #56965, #67726 — the cost of a parameter whose behaviour differed by core
  version: two follow-ups to make the version gate honest.
- #46306 — a feature removal made deliberately at a release boundary, not folded
  into an unrelated change.
- #58925 — **merged** rename of `TriggerDagRunOperator._defer` to `deferrable`: a
  private *attribute* with no override semantics is the narrow allowed case.
- #52237 (with #52431) — the opposite outcome on a *method*: renaming
  `_handle_execution_date_fn` on `ExternalTaskSensor` was rejected as breaking by
  two reviewers and closed. This pair is why the boundary is method-vs-attribute.
- #46633 — a genuine default change (`TriggerDagRunOperator` logical date null),
  made as its own discussed change at an Airflow-3 boundary, not a drive-by fix.
- #46231, #47530, #47892, #48683, #47798 — the relocation of these operators out of
  airflow-core into this provider, the reason its installed base is what this
  decision protects.
