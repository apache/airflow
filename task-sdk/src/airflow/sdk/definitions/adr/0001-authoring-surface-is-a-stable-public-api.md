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
`TaskGroup`, `Param`, the mapping API (`partial`/`expand`), the `@task`/`@dag`/
`@task_group` decorators — is defined here. These are the API users write Dags
*against*, in files this project neither controls nor can migrate. `DAG` makes it
explicit: an `attrs` class whose field transformer forces every argument after
`dag_id` keyword-only, so the constructor's *parameter names and defaults are the
public interface*. Operator construction is the same via
`BaseOperatorMeta._apply_defaults`.

Because the author's code already exists, a change here is judged against the
*entire installed base*, not the diff in front of the reviewer. Renaming a
parameter, tightening a default, deleting a class, or making a previously-valid
Dag raise breaks user code whose authors are not in the PR — the failure is
thousands of Dag files that stop importing after an upgrade. Airflow's discipline:
user-facing removals go through a `RemovedInAirflow4Warning` cycle (old spelling
keeps working while it warns), and *semantic* changes go through an AIP or devlist
thread, not a bare PR.

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

- Author Dags keep importing across an upgrade; a deprecated spelling gives users
  a release to migrate before removal.
- Adding authoring surface is more work (AIP or devlist), intentionally — the
  project then supports it indefinitely.
- Reviewers can reject a rename-in-place or stricter default as a compatibility
  break without enumerating who it breaks.

A change **violates** this decision when it:

- renames, removes, or repurposes a *released* public parameter, class, default,
  or decorator in place, instead of keeping it working behind a deprecation
  warning;
- changes the meaning or default behaviour of an authoring construct — or adds
  validation that rejects previously-valid Dags — without an AIP / devlist
  discussion. Restoring the documented behaviour of an existing, already-declared
  config key that no code currently reads is a defect fix, not a meaning change —
  it needs a newsfragment, not an AIP;
- introduces a new public authoring API as a bare PR when it warrants an AIP;
- makes a previously-accepted Dag fail, or fail deep in the scheduler/worker
  rather than at parse time with an author-facing error — unless the
  previously-accepted behaviour was a swallowed error or silent no-op that the
  change surfaces.

## Evidence

- #48460 — `sla` params made no-op with deprecation warning; the canonical
  retire-don't-remove pattern.
- #56127 — a dropped `sla_miss_callback` deprecation warning was *restored* — the
  warning cycle is itself part of the compatibility contract.
- #53496 — "Remove warning for `BaseOperator.executor` because it works":
  correcting a warning on a still-supported attribute; maintained deliberately.
- #65447 (AIP-76), #65474 (AIP-105), #66160 (AIP-103) — new authoring surface
  landed *through an AIP*, showing the bar for semantic additions.
