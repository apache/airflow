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

# 2. Triggers serialize to (classpath, kwargs) and are reconstructed elsewhere

Date: 2026-07-19

## Status

Accepted

## Context

A trigger has two lives: constructed inside an operator and passed to
`TaskDeferred`, then *actively run* in a triggerer — almost always a **different
process**. `BaseTrigger` bridges them with one narrow contract: `serialize()`
returns a `(classpath, kwargs)` tuple, and the triggerer reconstructs the trigger
by importing `classpath` and calling it with `kwargs`.

That tuple is the *only* thing that crosses the gap. The live Python object — its
open clients, bound methods, enclosing task context — does **not** travel.
Everything the trigger needs must be `kwargs` that are JSON-serializable (or
registered with Airflow serialization) and sufficient to rebuild an equivalent
instance. A session, an open connection, or a closure over operator state cannot
survive the trip and, if smuggled in, either fails to serialize or reconstructs
broken.

Reconstruction happens more than once and in more than one place — two triggerers
at once for HA, moved on redistribution, rebuilt from the persisted row on
restart. So identity must be **deterministic**: the `TriggerEvent` identifying
value and `shared_stream_key()` are compared across independently reconstructed
instances, so deriving them from `time.time()` or `uuid.uuid4()` breaks
deduplication and stream sharing. And because the runner injects state *after*
construction (`_task_instance`, `trigger_id`, templated fields), a subclass that
skips `super().__init__()` reconstructs missing that machinery and crashes the
triggerer. The recurring pressure is convenience: the operator holds a live
handle, so "just pass the client / session into the trigger" looks harmless.

## Decision

A trigger must be fully reconstructable from its `(classpath, kwargs)` alone,
in another process, deterministically. Concretely:

- `serialize()` returns `(classpath, kwargs)` where `kwargs` are
  JSON-serializable (or registered with Airflow serialization) and carry
  **only data** — no live objects, DB sessions, open connections, or closures.
- The trigger's identity — its `TriggerEvent` identifying value and
  `shared_stream_key()` — must be **deterministic**, derived from
  configuration fields, never from per-call values like `time.time()` or
  `uuid.uuid4()`.
- Subclasses must call `super().__init__()` so runner-injected state exists on
  the reconstructed instance.
- Adding or changing a field must keep the **serialize / reconstruct
  round-trip in sync**, including template-field filtering and `trigger_kwargs`
  handling, so the reconstructed instance is equivalent to the original.

## Consequences

- Triggers move freely between processes and are safe to duplicate for HA,
  because the wire format is pure data.
- Deferral is decoupled from operator internals: a trigger depends only on the
  arguments it declared, not live operator state.
- Authors must make every input serializable and deterministic and keep
  constructors round-trip-clean — a real limit on what a trigger can "capture".

A change **violates** this decision when, in `triggers/` code, it:

- returns non-serializable or non-deterministic values from `serialize()`, or
  passes a live object / session / connection / closure through `kwargs`;
- derives a `TriggerEvent` identifying value or `shared_stream_key()` from a
  non-deterministic source (`time.time()`, `uuid.uuid4()`, address identity),
  breaking HA deduplication or stream sharing;
- adds a trigger subclass whose constructor skips `super().__init__()`;
- adds or moves a field without keeping the serialize / reconstruct round-trip
  (and template-field / `trigger_kwargs` handling) in sync.

A reviewer should reject any change that makes a trigger depend on state that
does not survive `(classpath, kwargs)` reconstruction in another process.

## Evidence

- #66002 — keeps `trigger_kwargs` in serialized form until reconstruction,
  reinforcing that kwargs are the wire format, not a live object graph.
- #68636 — a subclass skipping `super().__init__()` reconstructed missing
  runner-injected state and crashed the triggerer: the failure this guards.
- #64715 — filters templated kwargs to fields that exist on the reconstructed
  trigger, since operator `template_fields` and trigger attributes differ.
- #55068 — re-enables `start_from_trigger` by rendering template fields on the
  reconstructed trigger, exercising the same round-trip.
