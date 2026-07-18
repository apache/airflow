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

# 1. Cross-version compatibility via optional-field-defaults, not schema-version bumps

Date: 2026-07-18

## Status

Accepted

## Context

Serialized Dags are the interchange format between independently-deployed
Airflow components. During a rolling upgrade the scheduler, the API server, the
Dag processor, and the workers routinely run on *adjacent* Airflow versions at
the same time: a component on version N writes serialized data that a component
on version N-1 must still be able to read, and vice versa. Serialized rows also
persist across an upgrade, so a newer reader must be able to load data that an
older writer produced before the upgrade.

There is a natural temptation, whenever the serialization payload gains a new
field, to reach for a "bump the serialization schema version and branch on it"
mechanism. That is **not** the mechanism Airflow uses for evolving the
serialized payload. A hard version gate makes the two halves of a
mid-upgrade cluster mutually unreadable the moment one side is upgraded, which
is exactly the window the format has to survive. It also forces every reader to
carry version-branching logic that grows without bound as fields are added.

## Decision

Evolve the serialized payload by adding **optional fields that default to
`None`** (or to a behaviour-preserving sentinel), rather than by bumping a
schema version and branching on it.

- A newly serialized field is optional. A newer writer emits it; an older
  reader that does not know the key simply ignores it and behaves as before.
- A newer reader that encounters data written by an older writer — where the
  key is absent — falls back to the field's default and reconstructs the object
  with the pre-existing behaviour.
- The default must be chosen so that "field absent" reproduces the old
  behaviour exactly. `None` is the usual choice; any other default has to be
  behaviour-neutral for old data.
- The PR that adds the field must **state the cross-version compatibility
  reasoning explicitly** — which readers/writers span the change, and why an
  absent value is safe in both directions. Reviewers rely on that statement to
  confirm rolling upgrades are preserved.

## Consequences

- Rolling upgrades keep working: mixed-version clusters read each other's
  serialized data throughout the upgrade window, and persisted rows survive the
  version transition in both directions.
- The reader code stays free of ever-growing version-branch ladders; each new
  field is self-describing through its presence or absence.
- The cost is discipline: every new field must have a behaviour-preserving
  default and an explicit compat rationale in the PR.

A **violating change** looks like any of:

- Adding a **required** field to the serialized payload (no default), so an
  older reader — or the newer reader loading old data — raises `KeyError` /
  fails to reconstruct the object.
- **Reshaping or removing** an existing field (renaming a key, changing its
  type or nesting) with no fallback path that reads the old shape.
- Choosing a default that is *not* behaviour-neutral, so old data silently
  deserializes into a different runtime behaviour than it had before.
- Introducing a "serialization version" gate that makes one side of an
  in-progress rolling upgrade refuse to read the other side's data.

Any of these breaks mixed-version clusters and is rejected on that basis.

## Evidence

- #63884 — adds a `rerun_with_latest_version` config knob whose serialized
  effect is additive and defaults to the prior behaviour, so old readers are
  unaffected.
- #66608 — fetches deadline callback context via the Execution API at runtime
  rather than baking a new required shape into the serialized payload, keeping
  the serialized form readable across adjacent versions.
