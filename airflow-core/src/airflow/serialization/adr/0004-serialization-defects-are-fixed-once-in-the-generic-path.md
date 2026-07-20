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

# 4. Serialization defects are fixed once, in the generic path

Date: 2026-07-20

## Status

Accepted

## Context

Serialization is a funnel: an open set of Python objects goes in, one JSON
document comes out, and a small number of generic helpers do the narrowing. A
defect in one of those helpers therefore does not present as one bug. It presents
as a stream of apparently unrelated reports — this parameter type serializes
unstably, that templated field crashes on deserialization, this operator's
attribute silently becomes a string — each of which has an obvious local fix.

Local fixes are what get closed here. The recurring case is unstable
representation: an object without a defined `__repr__` serializes as
`<... object at 0x7f...>`, the address changes every parse cycle, the serialized
Dag hashes differently, and a new `DagVersion` is minted for a Dag nobody edited.
Two separate PRs arrived proposing to special-case the one parameter type where
the symptom was observed. Both were closed and the work consolidated into a single
change in the shared template-field serialization helper — because the address was
never a property of that type, it was a property of any object reaching the
fallback path.

The same shape appears at the boundary between serialization and the API. A
payload too large for its column raises a database error on write. The local fix
is per-route validation with a size limit, a configuration knob and a new exception
class — one endpoint's worth of machinery. The accepted fix was one exception
handler for the database's data error, registered once on the public REST API and
once on the execution API, which then covers Dag-run `conf`, connection extras,
variable values, XCom values, task-instance notes, and every write endpoint added
in the future. Less code, more coverage, no new configuration surface.

There is a second cost to the local fix that matters more here than elsewhere.
Serialization output is persisted and read back by components on adjacent
versions, so a per-type special case is not just duplicated logic — it is a
divergence in the *format* that some readers will have and others will not. The
generic path is the only place where a fix is guaranteed to reach everything that
round-trips.

## Decision

**Fix the helper that produced the bad output, not the type that revealed it.**

- **Before writing a per-type branch, establish why the value reached the generic
  fallback at all.** If the answer is "it has no stable representation", the fix
  belongs in the fallback, and it fixes every other type that lands there too.
- **Enumerate the sibling paths and fix them together.** Task-level and Dag-level
  `default_args`, mapped and non-mapped operators, `partial` arguments — a fix in one
  and not the others leaves the same defect reachable and the same version churn.
- **Translate errors at the framework boundary, once.** A failure the database or
  the serializer already reports is turned into the right response by a registered
  handler covering every endpoint — not by validation code copied into each route.
- **A new configuration option is evidence the fix is in the wrong place.** A knob
  that lets a deployment tune around a serialization failure ships the failure and
  the workaround together.
- **Where a fix genuinely cannot be generic, say why in the PR** — the burden is on
  the special case, not on the generic path.

## Consequences

- One fix retires a class of reports rather than one report, and the format stays
  uniform across readers.
- Reviewers can hold a small number of helpers in their head, which is what makes
  review of this area tractable at all.
- Consolidation is slower for the contributor who found the symptom: the fix moves
  to code they did not touch, tests have to cover types they were not chasing, and
  their PR is sometimes closed in favour of someone else's. That churn is the cost of
  not accumulating per-type branches in a format that must stay readable across
  versions.

A change **violates** this decision when it:

- adds a type-specific branch to serialization for a symptom the generic fallback
  causes for any type — registering a deterministic encoding for a value type that
  currently falls through to `repr()` is what `0002` requires and is not a
  violation; this bullet targets branches that special-case a symptom the generic
  path should handle;
- fixes an unstable-representation defect on one code path (task-level
  `default_args`, non-mapped operators, …) while leaving its siblings untouched;
- adds per-route or per-field handling that translates an exception into an HTTP
  status, where a single registered error handler could do it for every endpoint;
- introduces a configuration option whose purpose is to avoid a serialization
  failure;
- adds a new exception class for a condition an existing framework-level error
  already carries.

Relationship to `0002`: the two decisions point the same way and the first bullet
above must not be read against `0002`. `0002` *requires* a value type that would
otherwise reach the payload through `str()`/`repr()` to get a deterministic
encoding; supplying that encoding is compliance, not a type-specific patch. What
this ADR forbids is fixing such a defect at one call site — a single operator
kwarg, one route, one `default_args` path — while the shared helper that every
sibling path goes through keeps the old behaviour.

## Evidence

- #68901 — "Serialize `DagParam` like a normal `Param`", opened to stop a
  `DagParam` serializing with its memory address inside a mapped operator's
  `partial`; converted to an issue rather than merged, once it was clear the fix
  belonged in `serialize_template_field`.
- #65705 — "Fix Dag version inflation caused by unstable `DagParam` repr": the
  second attempt at the same symptom, reworked to fix `serialize_template_field` and
  then closed so the change could land as part of the consolidated work in #63871.
- #66787 — "Validate Dag-run conf payload size at trigger boundary": closed by its
  own author in favour of #66888, which registers one handler for the SQLAlchemy
  data error on both the public REST API and the execution API, and so covers Dag-run
  `conf`, connection extras, variable values, XCom values, task-instance notes and
  every future write endpoint — instead of a config knob, a new exception class and
  per-route validation.
- #60480 — a `TypeError` deserializing templated date fields in mapped operators,
  superseded by the broader fix in #60414.
