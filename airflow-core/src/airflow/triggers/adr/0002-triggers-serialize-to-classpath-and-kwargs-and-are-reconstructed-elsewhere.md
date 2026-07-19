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

A trigger has two lives. It is first constructed inside an operator, where it
is passed to `TaskDeferred`; it is then *actively run* in a triggerer, almost
always a **different process** from the one that built it. `BaseTrigger`
bridges those two lives with a single narrow contract: `serialize()` returns a
`(classpath, kwargs)` tuple, and the triggerer reconstructs the trigger by
importing `classpath` and calling it with `kwargs`.

That tuple is the *only* thing that crosses the gap. The live Python object the
operator built — with its open clients, its bound methods, its enclosing task
context — does **not** travel. Whatever the trigger needs in order to run must
be expressible as `kwargs` that are JSON-serializable (or registered with
Airflow's serialization) and sufficient to rebuild an equivalent instance from
scratch. Anything else — a database session, an open connection, a closure over
operator state — cannot survive the trip and, if smuggled in, either fails to
serialize or silently reconstructs into something broken.

The reconstruction also happens more than once, and in more than one place.
For HA, the same trigger can be run in two triggerers at once; on
redistribution it moves between triggerers; on a restart it is rebuilt from the
persisted row. This is why identity must be **deterministic**: the
`TriggerEvent` identifying value and `shared_stream_key()` are compared across
independently reconstructed instances, so deriving them from `time.time()` or
`uuid.uuid4()` breaks deduplication and stream sharing. And because the runner
injects state into the instance *after* constructing it (`_task_instance`,
`trigger_id`, templated fields), a subclass that does not call
`super().__init__()` reconstructs into an object missing that machinery and
crashes the triggerer.

The recurring pressure is convenience: the operator already holds a live handle,
so "just pass the client / the session / this bound value into the trigger"
looks harmless locally. The decision below is what keeps the boundary a pure,
reconstructable data tuple.

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
- Deferral is decoupled from operator internals: the trigger cannot depend on
  live operator state, only on the arguments it declared.
- Trigger authors carry the burden of making every needed input serializable
  and deterministic, and of keeping constructors round-trip-clean — a real
  constraint on how much a trigger can "capture" from where it was built.

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

- #66002 — "Do not deserialize `trigger_kwargs` when loading serialized DAGs":
  keeps trigger kwargs in their serialized form until the trigger is actually
  reconstructed, reinforcing that the kwargs are the wire format rather than a
  live object graph.
- #68636 — "Fix triggerer crash when trigger subclass does not call
  `super().__init__()`": a subclass that skipped the base constructor
  reconstructed into an instance missing runner-injected state and crashed the
  triggerer — the concrete failure this decision guards against.
- #64715 — "Fix trigger template rendering failure when operator
  template_fields differ from trigger attributes": corrects the round-trip so
  templated kwargs are filtered to fields that actually exist on the
  reconstructed trigger, rather than assuming operator and trigger share a
  shape.
- #55068 — "Re-enable `start_from_trigger` feature with rendering of template
  fields": restores start-from-trigger by rendering template fields on the
  reconstructed trigger, exercising the same serialize / reconstruct /
  render round-trip.
