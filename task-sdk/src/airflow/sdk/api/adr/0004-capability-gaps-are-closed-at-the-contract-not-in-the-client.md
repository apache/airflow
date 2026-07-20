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

`client.py` is where a missing capability first becomes visible. A Dag author
wants to list Variable keys, read an asset event's source Dag run, or learn
whether a Dag has a given task, and the symptom is always the same: there is no
client method for it. The obvious response is to add one.

It is also the wrong end of the chain. This client is a mirror of two contracts
it does not own — the Execution-API OpenAPI spec, from which
`datamodels/_generated.py` is regenerated wholesale, and the provider-side
contracts (secrets backends, connections) that determine what the server can
actually answer. A method added here that has no counterpart in the spec is
half-wired by construction, and a method that satisfies a request the underlying
contract does not define works for exactly one backend and silently fails or
lies for the rest.

The record shows both failure modes. A `list_variable_keys` capability was
refused because no secrets backend contract supports listing — Airflow can only
enumerate Variables held in the metadata DB, so the client method would have
been correct for the DB backend and quietly incomplete for every remote one; the
real change was to the backend contract, which is a larger discussion (#61595).
A request to expose an asset event's source Dag run was redirected: the event
already references a task instance, so the properties should be derived from
what the API returns, and the client additionally has to handle the `404` case
now that asset events can be created with no Dag run or task instance at all
(#53357). Adding `run_after` to `TriggerDagRunOperator` turned out to require the
exception, the payload, the routes, the datamodels, `task_runner`, and the
supervisor to move together — the operator-side change alone was inert (#61338).
A `target_date` concept was refused at the semantic level, not the plumbing
level (#67329).

The same asymmetry governs changes to the client's *observable* behaviour. A
proposal to lower the log level for a missing connection from `ERROR` to
`WARNING` — motivated by noise from one provider hook — was withdrawn once it
became clear that the client's level is what downstream consumers key on, so a
per-hook annoyance cannot be fixed by moving the shared client's floor (#56544).

Because the generated models are regenerated as a unit, the coupling runs the
other way too: bumping `datamodel-code-generator` changes this directory's
contents, so the bump and the regenerated models belong in one commit — as in
PR #54027, where the pin change and the regenerated `_generated.py` for both
the Task SDK and airflow-ctl landed together.

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

Contributors who arrive at a missing capability through the client are sent
elsewhere first — to the server routes, to a provider contract, or to a devlist
discussion — which is slower and less satisfying than adding the method they
came to add. Some real capability gaps stay open because the honest fix is a
cross-cutting contract change nobody has taken on.

What this buys is that the client never claims a capability the deployment
cannot actually deliver, and never develops a second, client-only notion of what
the API offers. It also keeps the client honest about version skew: since every
method traces to a spec entry with a Cadwyn version, a worker talking to an
older server fails through version negotiation rather than through a method that
was only ever real on the client side.

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

- #61595 — `list_variable_keys` with prefix filter via the Task SDK; refused
  because no secrets backend contract supports listing, so the gap is in the
  backend contract.
- #53357 — source Dag run on an asset event; redirected to deriving it from the
  task-instance reference the response already carries, with `404` handling for
  watcher-created events.
- #61338 — `run_after` for `TriggerDagRunOperator`; the operator-only change was
  inert, requiring exception, payload, routes, datamodels, `task_runner` and
  supervisor together.
- #67329 — `target_date`; refused at the semantic level because the existing
  data-interval fields carry the concept.
- #56544 — lowering the connection-not-found log level from `ERROR` to `WARNING`;
  withdrawn once the shared client's level was recognised as a consumer
  contract.
- #54027 — "Upgrade datamodel-code-generator to 0.32.0"; merged, and the commit
  carries the regenerated `datamodels/_generated.py` for both the Task SDK and
  airflow-ctl alongside the two `pyproject.toml` pin changes, so the pinned
  generator and the committed models never disagreed at any commit.
