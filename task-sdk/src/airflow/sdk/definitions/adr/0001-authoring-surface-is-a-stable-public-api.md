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

# 1. The Dag authoring surface is a stable public API — deprecate, don't break

Date: 2026-07-19

## Status

Accepted

## Context

Everything a Dag author imports from `airflow.sdk` — `DAG`, `BaseOperator`,
`TaskGroup`, `Param`, the mapping API (`partial` / `expand`), the `@task` /
`@dag` / `@task_group` decorators — is defined in this package. These are not
internal helpers; they are the API users write their Dags *against*, in files
that live in the user's repository and that this project neither controls nor can
migrate. `DAG` makes that contract explicit: it is an `attrs`-defined class whose
field transformer forces every argument after `dag_id` to be keyword-only, so the
constructor's *parameter names and defaults are the public interface*. Operator
construction is the same: `BaseOperatorMeta._apply_defaults` wraps every
subclass `__init__`, so the accepted kwargs and their defaults are a released
surface too.

Because the author's code already exists, a change here is judged against the
*entire installed base*, not the diff in front of the reviewer. Renaming a
parameter, tightening a default, deleting a class, or making a previously-valid
Dag raise is a breaking change for user code — and the people who wrote that code
are not in the PR to notice. The failure is not a crash in CI; it is thousands of
Dag files that stop importing after an upgrade.

Airflow already has an established discipline for this: user-facing removals go
through a `warnings.warn(..., RemovedInAirflow4Warning, stacklevel=…)` cycle —
the old spelling keeps working while it warns — and *semantic* changes to what a
construct means to author code go through an AIP or a devlist thread, not a bare
PR. The recurring pressure is that a rename or a stricter default looks like a
harmless cleanup locally; it is not, once the name has shipped.

## Decision

Treat the authoring surface as a stable public API. Concretely:

- **Do not break a released signature in place.** A public parameter, default,
  class, or decorator that shipped keeps working; it is retired through a
  deprecation-warning cycle (`RemovedInAirflow4Warning`), often as a
  no-op-with-warning shim, never a hard removal in the same release.
- **A semantic change needs an AIP or a devlist discussion.** New authoring
  surface, a changed default behaviour, or new validation that rejects
  previously-accepted Dags is an architecture decision, not a drive-by diff.
- **Renames are for still-unreleased / experimental surface only**, and are done
  everywhere at once (authoring class, serialization, docs, examples).
- **New validation fails at parse time with a clear author-facing error**, and
  does not reject Dags that were valid before unless that is the deliberate,
  discussed intent.

## Consequences

- Author Dags keep importing and running across an upgrade; a deprecated spelling
  gives users a release to migrate before it is removed.
- Adding authoring surface is more work — it goes through an AIP or at least a
  devlist thread — and that friction is intentional, because the project then has
  to support the surface indefinitely.
- Reviewers can reject a rename-in-place or a stricter default as a compatibility
  break without needing to enumerate who it breaks.

A change **violates** this decision when it:

- renames, removes, or repurposes a *released* public parameter, class, default,
  or decorator in place, instead of keeping it working behind a deprecation
  warning;
- changes the meaning or default behaviour of an authoring construct — or adds
  validation that rejects previously-valid Dags — without an AIP / devlist
  discussion;
- introduces a new public authoring API as a bare PR when it warrants an AIP;
- makes a previously-accepted Dag fail, or fail deep in the scheduler/worker
  rather than at parse time with an author-facing error.

## Evidence

- #48460 — "Make `sla` params no-op with deprecation warning": the canonical
  pattern — a retired authoring parameter is kept accepted and warns, rather than
  removed outright.
- #56127 — "Add back Deprecation warning for `sla_miss_callback`": a deprecation
  warning that had been dropped was *restored*, showing the project treats the
  warning cycle itself as part of the compatibility contract.
- #53496 — "Remove warning for `BaseOperator.executor` because it's false":
  correcting a warning on a still-supported attribute — the deprecation surface is
  maintained deliberately, not casually.
- #65447 (AIP-76 partition authoring API), #65474 (AIP-105 pluggable retry
  policies), and #66160 (AIP-103 accessors) — new authoring surface here landed
  *through an AIP*, not a bare PR, illustrating the bar for semantic additions.
