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

# 4. Capability gaps are closed at the contract, not in the client

Date: 2026-07-20

## Status

Accepted

## Context

`client.py` is where a missing capability first becomes visible — no method to
list Variable keys, read an asset event's source Dag run, or check whether a Dag
has a task — and the obvious response is to add one. It is the wrong end of the
chain. The client mirrors two contracts it does not own: the Execution-API
OpenAPI spec (from which `datamodels/_generated.py` is regenerated wholesale) and
the provider-side contracts (secrets backends, connections) that determine what
the server can answer. A method with no counterpart in the spec is half-wired by
construction; a method satisfying a request the underlying contract does not
define works for one backend and silently fails or lies for the rest.

The record shows both modes: `list_variable_keys` refused because no secrets
backend contract supports listing, so the real change is to the backend contract
(#61595); an asset event's source Dag run redirected to a *derived* property off
the task-instance reference, with `404` handling for events created without one
(#53357); `run_after` for `TriggerDagRunOperator` needing exception, payload,
routes, datamodels, `task_runner`, and supervisor together, the operator-only
change inert (#61338); `target_date` refused at the semantic level (#67329). The
same asymmetry governs *observable* behaviour: lowering a connection-not-found
log level from `ERROR` to `WARNING` for one hook's noise was withdrawn once the
client's level was recognised as a consumer contract (#56544). And because the
models regenerate as a unit, bumping `datamodel-code-generator` belongs in one
commit with the regenerated models — as in #54027, where the pin change and the
regenerated `_generated.py` for both Task SDK and airflow-ctl landed together.

## Decision

**A capability that the Execution-API spec or an underlying provider contract
does not define is fixed in that contract; the client method is the last step,
never the first.**

- **Server-first ordering.** A new capability starts as a server datamodel and
  route with a Cadwyn `VersionChange`, then reaches this directory by
  regeneration and a client method. A client method for an endpoint the spec
  does not define does not get merged as a first instalment.
- **Derive before requesting.** If the information is obtainable from a response
  the client already receives, expose it as a derived property rather than as a
  new call. Handle the absence case explicitly — a reference that used to be
  guaranteed may now be optional.
- **A backend-contract gap is a backend-contract change.** When the capability
  depends on what secrets backends, connections, or providers are required to
  implement, changing that contract is the work. A client method covering only
  the metadata-DB case is not an increment toward it.
- **Client-observable behaviour is a shared contract.** Log levels, error types,
  and status mappings in `client.py` are consumed by the secrets backend, the
  supervisor, and provider hooks. Do not retune them to suit one caller; fix the
  caller or change the contract deliberately.
- **Generator bumps carry their regeneration.** A change to
  `datamodel-code-generator` or the generation script includes the regenerated
  `datamodels/_generated.py` in the same PR, so the pinned generator and the
  committed models never disagree.

## Consequences

Contributors are sent elsewhere first — server routes, a provider contract, or a
devlist discussion — which is slower than adding the method they came for, and
some real gaps stay open because the honest fix is a cross-cutting contract
change nobody has taken on.

What this buys: the client never claims a capability the deployment cannot
deliver, never develops a second, client-only notion of the API, and stays honest
about version skew — since every method traces to a spec entry with a Cadwyn
version, a worker on an older server fails through version negotiation rather than
a method that was only ever real client-side.

A change **violates** this decision when it:

- adds a method or namespace operation in `client.py` for a route the
  Execution-API spec does not define, or without the paired server-side change.
- adds a client-side capability that the relevant secrets-backend, connection,
  or provider contract does not require implementations to support.
- introduces a new call to fetch data that an already-returned response carries,
  instead of deriving it.
- changes a log level, exception type, or status mapping in the client to
  accommodate one hook, operator, or provider.
- hand-edits `datamodels/_generated.py`, or bumps the generator or generation
  script without committing the regenerated models alongside.
- lands the client half of a stack-spanning feature while the server datamodel,
  route, version file, `comms.py` type, or supervisor handler is deferred to a
  follow-up.

## Evidence

- #61595 — `list_variable_keys`; refused because no secrets backend contract
  supports listing — the gap is in the backend contract.
- #53357 — source Dag run on an asset event; redirected to deriving it from the
  task-instance reference, with `404` handling for watcher-created events.
- #61338 — `run_after` for `TriggerDagRunOperator`; operator-only change inert,
  needing exception, payload, routes, datamodels, `task_runner`, supervisor.
- #67329 — `target_date`; refused at the semantic level — data-interval fields
  carry the concept.
- #56544 — lowering connection-not-found log level; withdrawn once the client's
  level was recognised as a consumer contract.
- #54027 — "Upgrade datamodel-code-generator to 0.32.0"; **merged**, the commit
  carrying the regenerated `_generated.py` for both Task SDK and airflow-ctl
  alongside the pin changes, so generator and models never disagreed.
