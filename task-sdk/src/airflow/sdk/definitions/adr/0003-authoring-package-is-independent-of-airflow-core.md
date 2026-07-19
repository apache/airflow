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

# 3. The authoring package is independent of airflow-core

Date: 2026-07-19

## Status

Accepted

## Context

`task-sdk` is an **independently released distribution**, and this package is its
Dag-authoring surface. `airflow.sdk` is the *supported* import path for authoring
a Dag — the one users are told to write against — and it is also the seam that
underpins the 2.x→3.x compatibility story: a Dag written against `airflow.sdk`
must keep working while airflow-core evolves underneath it. For that to hold, the
authoring package cannot depend on airflow-core internals. In particular it must
not import `airflow.models` or the core ORM: doing so drags SQLAlchemy models and
the server's database layer onto the authoring/worker import path, couples the SDK
to a specific airflow-core version, and defeats the independent-release model.

This independence was *established*, not free. A large body of work deliberately
severed the old coupling — moving shared pieces into the SDK, removing SDK
references from core and core references from the SDK — so that today the
authoring package stands on `airflow.sdk` (plus stdlib / attrs / typing) alone.
The recurring pressure is that the needed symbol *exists* somewhere in
`airflow.models`, so importing it directly looks like the shortest path; it is
also the exact move that re-couples the two distributions. A `check_core_imports_in_sdk`
prek hook guards the boundary, but the hook only catches the import — it is the
decision below that says what to do instead.

## Decision

The authoring package depends only on the SDK's own surface, never on airflow-core
internals. Concretely:

- **No `airflow.models` / airflow-core ORM import from this package.** Code here
  imports from `airflow.sdk` (and stdlib / attrs / typing) only; the
  `check_core_imports_in_sdk` hook enforces it and must not be silenced with
  blanket ignore markers.
- **Shared code moves into the SDK**, rather than being reached back into
  airflow-core — if the authoring surface and core both need something, it lives on
  the SDK side and core depends on the SDK, not the reverse.
- **`airflow.sdk` is the public authoring import path** — new authoring symbols are
  exported from `airflow.sdk` (and its lazy `__getattr__` map), so users never have
  to reach into `airflow.sdk.definitions.…` or into airflow-core to author a Dag.
- **The package stays installable and versioned independently of airflow-core** —
  no dependency that would force the two to release in lockstep.

## Consequences

- A Dag authored against `airflow.sdk` keeps working across airflow-core changes,
  which is what makes the 2.x→3.x migration path viable.
- The authoring/worker import path stays light — no server ORM or database layer is
  pulled in just to define a Dag.
- Sharing code costs more: the shared piece has to be moved into the SDK rather
  than imported from core, and that direction (core depends on SDK) is deliberate.

A change **violates** this decision when it:

- adds an `import` of `airflow.models` or airflow-core ORM (or another airflow-core
  internal) into this package, instead of using the SDK's own surface;
- silences the `check_core_imports_in_sdk` hook with a blanket ignore rather than
  moving the shared code into the SDK;
- makes airflow-core a runtime dependency of the authoring package, or otherwise
  couples the two so they must release together;
- exposes a public authoring symbol only via a deep `airflow.sdk.definitions.…`
  path or via airflow-core, instead of through `airflow.sdk`.

## Evidence

- #54383 — "Remove `airflow.models.DAG`": collapsing the old core `DAG` so the SDK
  `DAG` is the single authoring definition, a keystone of the independence.
- #58223 — "Remove airflow core dependency for typing utilities in task sdk" and
  #55292 — "Remove state dependency from airflow core in sdk": concrete severances
  of SDK→core imports, exactly the direction this decision forbids.
- #58258 — "Remove SDK reference for NOTSET in Airflow Core" and #55538 — "Remove
  SDK dependency from SerializedDAG": the reverse-direction cleanups that keep the
  boundary clean from the core side too.
- #53450 — "Deprecate and move `airflow.utils.task_group` to SDK" and #53629 —
  "Deprecate decorators from Core": the pattern of *moving shared authoring code
  into the SDK* (and deprecating the core spelling) rather than importing back
  into it.
