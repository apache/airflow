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

# 3. Base classes are a provider-facing interface — preserve the subclass contract

Date: 2026-07-19

## Status

Accepted

## Context

`BaseOperator`, `BaseSensorOperator`, `BaseHook`, and the mixins (`SkipMixin`,
`ResumableJobMixin`, `BranchMixIn`) are subclassed by *every* provider package —
roughly a hundred of them, in `providers/`, each released independently and
maintained by different people. A provider hook is a `BaseHook` subclass, a
provider operator is a `BaseOperator` subclass, a provider sensor is a
`BaseSensorOperator` subclass, and provider operators mix in the shared mixins.
None of those subclasses appear in a diff that edits a base class here, yet all of
them depend on its behaviour.

That dependence is not just the public method signatures (covered by ADR 1). It is
the *subclass mechanics*: method resolution order, the `super().__init__()` call
chain that a subclass expects to reach, the kwargs a subclass forwards up through
`BaseOperatorMeta._apply_defaults`, and the cooperative-`super()` protocol the
mixins rely on. A change that keeps every signature identical can still break
providers if it shifts the MRO, stops calling `super().__init__()`, changes what a
mixin's method does to a subclass that already mixes it in, or makes a
previously-concrete method abstract so existing subclasses no longer satisfy it.

Because the subclasses live in separately-released distributions, this contract
cannot be validated by the base-class change's own CI. It is a judgement about the
whole population of provider subclasses — and the failure shows up only when a
provider is built or run against the new base, often after the base change has
already shipped.

## Decision

Treat every base class here as a provider-facing interface, and preserve the
subclass contract — not just the surface signature.

- **Keep method resolution and the `super()` chain intact.** A change must not
  reorder the MRO, drop a `super().__init__()` call subclasses depend on, or alter
  how kwargs flow up through `_apply_defaults`.
- **Keep mixins cooperative.** `SkipMixin` / `ResumableJobMixin` / `BranchMixIn`
  must keep working through `super()` for subclasses that already mix them in;
  making a method abstract, or changing its toggle/return contract, is a
  provider-breaking change and needs the same care as a signature change.
- **Extend additively.** New base capability is a new method or attribute that
  existing subclasses ignore, not a reshaping of one they already override.
- **When a base change is unavoidable, migrate the providers in the same effort**
  or stage it behind a deprecation cycle — do not land a base change that leaves
  provider subclasses broken.

## Consequences

- Provider packages keep building and running against a new base-class release
  without a coordinated emergency fix across a hundred repositories.
- Refactors that *move* shared behaviour into a base class or mixin (rather than
  changing it) are the safe shape, because subclasses keep the same observable
  contract.
- Reviewers weigh a base-class change against the provider population, not only the
  file being edited — a green diff is not sufficient evidence that providers survive.

A change **violates** this decision when it:

- alters method resolution order, drops a `super().__init__()` call, or changes how
  kwargs flow through `_apply_defaults` in a way that breaks existing subclasses;
- changes a mixin's cooperative-`super()` behaviour, makes a previously-concrete
  base/mixin method abstract, or alters its toggle/return contract, so subclasses
  that already mix it in stop working;
- reshapes a base method that providers override or call, when the capability could
  have been added as a new sibling;
- lands a base-class change that leaves provider subclasses broken, without
  migrating them in the same effort or staging it behind a deprecation cycle.

## Evidence

- #68506 — "Added asynchronous `aget_hook` method to `BaseHook`": base-hook
  capability extended *additively* with an async sibling, the safe shape for a
  class every provider hook subclasses.
- #69607 — "Making `ResumableJobMixin` an abstract class and its methods
  abstractmethods": exactly the kind of concrete-to-abstract mixin change that
  reshapes the subclass contract and must be reasoned about against every subclass
  that mixes it in.
- #62749 — "Move `SkipMixin` and `BranchMixIn` to Task SDK": relocating a mixin
  that provider operators mix in — the observable subclass contract must be
  preserved through the move.
- #61132 — "Fix deferrable sensors not respecting `soft_fail` on timeout": a
  `BaseSensorOperator` behaviour every sensor subclass (including provider sensors)
  inherits, corrected without changing the poke/reschedule interface those
  subclasses rely on.
