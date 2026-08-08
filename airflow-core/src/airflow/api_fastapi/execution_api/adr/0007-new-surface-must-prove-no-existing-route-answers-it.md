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

# 7. New surface must prove no existing route already answers it

Date: 2026-07-20

## Status

Accepted

## Context

The other decisions here govern *how* a change to the Execution API is made — versioned
through Cadwyn ([ADR-1](0001-cadwyn-calver-versioning-for-independent-deploys.md)), migrated
even when non-breaking ([ADR-2](0002-migrate-even-non-breaking-field-changes.md)), shaped
only through migration ([ADR-5](0005-private-but-contractual-api-shape-changes-via-migration.md)),
and reached only through the API not the DB
([ADR-6](0006-user-code-reaches-metadata-state-via-the-api-not-the-db.md)). This decision
governs the prior question: whether the surface should exist.

**How this composes with ADR-2 on migration cost.** ADR-2 says an extra migration is trivial;
this ADR treats accumulated migration weight as the scarce resource. Both are correct because
they answer different questions, and the order matters. **This ADR gates whether a piece of
surface comes into existence at all.** Once that gate is passed and the surface exists, ADR-1,
ADR-2 and ADR-5 govern how it changes — and there the answer is always "write the migration,"
with no budget to conserve. Nobody should ever decline to migrate a field change on the grounds
of chain weight; that is what ADR-2 forecloses. The weight argument is admissible only at the
point of asking for *new* surface, never as a reason to skip a migration on surface that already
ships.

The pressure to add is structural: this API is the only channel a worker has to server state,
so any missing capability looks like a missing endpoint or field, and each addition looks small.
The cost is not small and it is permanent — every route and field becomes private-but-contractual
surface consumed by the Python and beta Go SDKs, carried in the Cadwyn chain for as long as any
supported worker might call it, and re-tested per version. An endpoint that duplicates
information an existing response already carries creates two answers to the same question that can
disagree.

Review consistently pushes back, and the recurring shape is that the missing capability is
usually already derivable, semantically wrong, or a gap in a different contract: existence
endpoints an existing status `404` already encodes and that Dag Versioning makes undefined before
a run exists (#67832); a `target_date` field `data_interval_start`/`data_interval_end` already
carry (#67329); a Dag-level endpoint for a paused/not-paused fact that did not need one (#61063);
an asset-event source-run endpoint derivable from the task-instance reference the event holds
(#53357); a `list_variable_keys` capability that was really a gap in the secrets-backend contract
(#61595).

**When the capability is genuinely new, the supported path is an AIP.** Read alongside its
neighbours — the Task SDK's `definitions/adr/0004` refusing new authoring surface, and
`sdk/api/adr/0004` refusing a client method before the spec entry — this decision could look like
a closed loop with no way in. It is not, and the way through is not to erode any of the three: it
is to land the capability as a single end-to-end change under an accepted AIP, which is exactly
how the real ones arrived. AIP-103 added Execution API task/asset-state endpoints and wired the
Task SDK comms and context accessors (#66073, #66160); AIP-76 added `PartitionAtRuntime` (#65447);
AIP-105 added pluggable retry policies (#65474). All merged. The AIP supplies the agreed-need
argument a standalone route PR cannot supply for itself.

## Decision

**A new Execution-API route, or a new field on an existing one, is added only after showing that
no existing route answers the question, and that the question is well-defined under Dag
Versioning.**

- **Derive before adding.** Show which existing responses were examined and why the information
  cannot be obtained from them — including from a status code. Absence is information: a `404`
  from an existing status route is the answer to an existence question, not a reason for an
  existence route.
- **Do not add a field whose meaning an existing field already carries.** Look for the
  established concept first (`data_interval_start` / `data_interval_end` for processing dates, the
  task-instance reference on an asset event for its provenance). A second spelling of the same
  concept is a divergence source, not a convenience.
- **Confirm the question is well-defined.** Dag Versioning means the set of tasks in a Dag is a
  property of a Dag *version*, and a run pins the version. A route whose answer depends on which
  version is meant, before a run exists, is not implementable and does not become so by picking a
  default.
- **Fix the gap at the contract that has it.** When the capability is missing from the
  secrets-backend contract, the serialization contract, or a provider interface, that contract is
  the change — an Execution-API route that satisfies the metadata-DB case only is not a fix.
- **Use the right HTTP semantics for what remains.** An existence check is a `HEAD` or a status
  code, not a `GET` returning a body that restates it.
- **New surface is scoped to what a worker needs now.** Extra shape added for flexibility
  ("returning two lists allows more diverse use cases") is speculative surface carrying full
  migration weight.

## Consequences

Contributors must do exploratory work in the routes and datamodels before proposing an addition,
and some legitimate needs are answered with "derive it from what is there," which is more work at
the call site. Some capability gaps stay open longer because the honest fix is in a harder place —
a backend contract or an AIP — than a new route would have been.

In return, the migration chain stays proportional to the API's real capabilities rather than to
the number of convenience routes ever proposed; the SDKs have one place to look for each fact;
and the API does not acquire routes that answer version-dependent questions with
version-independent answers. This also keeps the per-version test matrix required by ADR-1
tractable.

A change **violates** this decision when it:

- adds a route to `routes/` whose description does not say which existing routes were checked and
  why they are insufficient.
- adds an existence, presence, or "does this exist" route where an existing route's `404` already
  conveys it.
- adds a field carrying a meaning an existing field already has, without explaining why the
  existing one cannot be used — other than as the additive half of a compat migration, where the
  old field is retained and marked deprecated in the same PR. A deliberate overlap window is what
  [ADR-2](0002-migrate-even-non-breaking-field-changes.md) requires, not a violation of this one:
  `TriggerDAGRunPayload` carrying both `logical_date` and `run_after`, and `DagRun` carrying
  `logical_date` alongside `run_after` and the data-interval pair, are that pattern, not
  duplication.
- adds a Dag-level or task-level route whose answer is not well-defined across Dag versions before
  a run exists.
- introduces an endpoint to supply a capability that the underlying backend or provider contract
  does not define, covering only the metadata-DB case.
- returns a response body whose only content is information already carried by the status code, or
  uses `GET` for a pure existence check.
- adds shape beyond the immediate need on the grounds that it permits more use cases later.

## Evidence

- #67832 — Dag tasks/task-groups existence endpoints; rejected — the existing status `404` encodes
  the answer, Dag Versioning makes task existence undefined before a run exists, wrong HTTP verb.
- #67329 — `target_date` field for Dag runs; refused — `data_interval_start`/`data_interval_end`
  already carry that meaning.
- #61063 — configurable bundle version defaults; the added Dag endpoint was unnecessary for the
  paused/not-paused fact it served.
- #53357 — asset-event source-Dag-run endpoint; redirected to deriving it from the task-instance
  reference, noting asset events can exist with no Dag run.
- #61595 — `list_variable_keys`; refused because no secrets backend contract supports listing —
  a contract-level gap.
- #66402, #66410, #66445 — a new task-instance state, its listener hook, and its supervisor/API
  wiring; closed once existing state plus the accepted AIP-103 accessors covered the case.
