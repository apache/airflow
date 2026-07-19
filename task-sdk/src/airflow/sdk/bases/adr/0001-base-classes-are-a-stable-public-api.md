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
classes users and *all ~100 provider packages* subclass. Every operator in a
user's repository and every operator under `providers/` is a `BaseOperator`
subclass; every hook is a `BaseHook` subclass; every sensor is a
`BaseSensorOperator` subclass. The accepted `__init__` kwargs and their defaults,
the `execute(context)` / `poke(context)` lifecycle contract, and the
`template_fields` behaviour are therefore a *released public API*, not internal
plumbing — and the code that depends on them lives in files this project neither
controls nor can migrate.

`BaseOperatorMeta._apply_defaults` makes the contract explicit: it wraps every
subclass `__init__` (`apply_defaults`) so `default_args` precedence, kwarg
validation, and deprecation warnings all live at that seam. The kwargs a
subclass may pass up, and the defaults the base supplies, are a surface every
operator author has already written against.

Because that code already exists, a change here is judged against the *entire
installed base of subclasses*, not the diff in front of the reviewer. Renaming a
parameter, tightening a default, deleting a method, or changing what `execute()`
is handed is a breaking change for user operators and for providers — and the
people who wrote those subclasses are not in the PR to notice. The failure is not
a crash in CI; it is thousands of operators, across user repos and provider
packages, that stop importing or stop running after an upgrade.

Airflow already has an established discipline for this: user-facing removals go
through a `warnings.warn(..., RemovedInAirflow4Warning, stacklevel=…)` cycle —
the old spelling keeps working while it warns — and *semantic* changes to the
lifecycle contract go through an AIP or a devlist thread, not a bare PR. The
recurring pressure is that a rename or a stricter default looks like a harmless
cleanup locally; it is not, once the base class has shipped and providers depend
on it.

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
  deprecated spelling gives maintainers a release to migrate before it is removed.
- Adding base-class surface is more work — it goes through an AIP or at least a
  devlist thread — and that friction is intentional, because the project and every
  provider then have to support the surface indefinitely.
- Reviewers can reject a rename-in-place or a stricter default as a compatibility
  break without needing to enumerate which providers it breaks.

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

- #48460 — "Make `sla` params no-op with deprecation warning": the canonical
  pattern — a retired `BaseOperator` parameter is kept accepted and warns, rather
  than removed outright.
- #56127 — "Add back Deprecation warning for `sla_miss_callback`": a deprecation
  warning that had been dropped was *restored*, showing the project treats the
  warning cycle itself as part of the compatibility contract.
- #53496 — "Remove warning for `BaseOperator.executor` because it's false":
  correcting a warning on a still-supported attribute — the deprecation surface is
  maintained deliberately, not casually.
- #68506 — "Added asynchronous `aget_hook` method to `BaseHook`": new capability
  added as an *additive* sibling to the existing sync method, the shape a
  compatible base-class extension takes.
- #65447 (AIP-76 partition authoring API), #65474 (AIP-105 pluggable retry
  policies), and #66160 (AIP-103 accessors) — new base-class / authoring surface
  landed *through an AIP*, not a bare PR, illustrating the bar for semantic
  additions.
