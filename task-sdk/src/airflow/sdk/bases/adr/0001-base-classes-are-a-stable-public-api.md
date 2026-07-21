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

# 1. Operator and hook base classes are a stable public API — deprecate, don't break

Date: 2026-07-19

## Status

Accepted

## Context

`BaseOperator`, `BaseSensorOperator`, `BaseHook`, and `BaseNotifier` are the
classes users and *all ~100 provider packages* subclass. The accepted `__init__`
kwargs and their defaults, the `execute(context)` / `poke(context)` lifecycle
contract, and `template_fields` behaviour are therefore a *released public API*,
not internal plumbing — and the code depending on them lives in files this
project neither controls nor can migrate.
`BaseOperatorMeta._apply_defaults` makes the contract explicit: it wraps every
subclass `__init__` so `default_args` precedence, kwarg validation, and
deprecation warnings all live at that seam.

Because that code already exists, a change here is judged against the *entire
installed base of subclasses*, not the diff in front of the reviewer. Renaming a
parameter, tightening a default, deleting a method, or changing what `execute()`
is handed breaks user operators and providers whose authors are not in the PR —
the failure is thousands of operators that stop importing or running after an
upgrade. Airflow's established discipline: user-facing removals go through a
`RemovedInAirflow4Warning` cycle (the old spelling keeps working while it warns),
and *semantic* lifecycle changes go through an AIP or devlist thread, not a bare
PR.

## Decision

Treat the operator and hook base classes as a stable public API. Concretely:

- **Do not break a released base-class signature in place.** A public `__init__`
  parameter, method, default, or attribute that shipped keeps working; it is
  retired through a deprecation-warning cycle (`RemovedInAirflow4Warning`), often
  as a no-op-with-warning shim, never a hard removal in the same release.
- **A semantic change to the lifecycle contract needs an AIP or a devlist
  discussion.** Changing what `execute()`/`poke()` is handed, adding authoring
  surface, or altering what a base construct means to subclasses is an
  architecture decision, not a drive-by diff.
- **New capability is additive.** Prefer a new sibling method or attribute (an
  `aget_hook` / `aget_connection` alongside the sync form) over changing the shape
  of an existing one that subclasses already override or call.
- **Renames are for still-unreleased / experimental surface only**, done
  everywhere at once (base class, serialization, docs, examples).

## Consequences

- User and provider subclasses keep importing and running across an upgrade; a
  deprecated spelling gives a release to migrate before removal.
- Adding base-class surface is more work (AIP or devlist), intentionally — the
  project and every provider then support it indefinitely.
- Reviewers can reject a rename-in-place or stricter default as a compatibility
  break without enumerating which providers it breaks.

A change **violates** this decision when it:

- renames, removes, or repurposes a *released* public `__init__` parameter, method,
  default, or attribute on a base class in place, instead of keeping it working
  behind a deprecation warning;
- changes the meaning of the `execute()`/`poke()` lifecycle contract, or what a
  base construct means to subclasses, without an AIP / devlist discussion;
- introduces new authoring surface on a base class as a bare PR when it warrants an
  AIP;
- reshapes an existing base method that providers override or call, when the
  capability could have been added as a new sibling instead.

## Evidence

- #48460 — `sla` params made no-op with deprecation warning; the canonical
  retire-don't-remove pattern.
- #56127 — a dropped `sla_miss_callback` deprecation warning was *restored* — the
  warning cycle is itself part of the compatibility contract.
- #53496 — "Remove warning for `BaseOperator.executor` because it works":
  correcting a warning on a still-supported attribute; the deprecation surface is
  maintained deliberately.
- #68506 — async `aget_hook` on `BaseHook` added as an *additive* sibling — the
  compatible extension shape.
- #65447 (AIP-76), #65474 (AIP-105), #66160 (AIP-103) — new base-class surface
  landed *through an AIP*, showing the bar for semantic additions.
