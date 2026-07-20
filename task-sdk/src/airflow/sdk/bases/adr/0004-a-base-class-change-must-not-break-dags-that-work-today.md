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

# 4. A base-class change must not break Dags that work today

Date: 2026-07-20

## Status

Accepted

## Context

[ADR-1](0001-base-classes-are-a-stable-public-api.md) covers the *declared*
interface: signatures, parameters, and the deprecation cycle that protects them.
This decision covers what a diff here can break without touching a signature at
all — the set of Dags that currently parse and run.

Two kinds of change do this. The first is new validation. Adding a check in
`BaseOperator.__init__`, `BaseOperatorMeta._apply_defaults`, or the `@task`
decorator that rejects an input the base class previously accepted converts
working Dags into parse-time failures across every deployment that upgrades —
including Dags in repositories this project cannot see or migrate. The second is
changed semantics on a path that already has behaviour: how skipping propagates,
how a sensor reschedules, how a mixin cooperates through `super()`. These change
outcomes silently, with no error anywhere.

Both are rejected on the same ground. A change adding an error message when a
task method name collides with a `time` or `builtins` name was closed without
further discussion as a non-starter: "this approach just won't work, and it would
almost certainly break a good portion of Dags that work today" (#55252). Note
what the change was trying to do — produce a *better error* for a genuinely
confusing failure — and that it still failed the test, because the check it
needed would have rejected names that are legal today. Validation of
`executor_config` keys in `BaseOperator` was withdrawn by its author after review
established that executors beyond `KubernetesExecutor` were affected and the
validation could not be written to cover them without rejecting valid configs
(#53459).

The semantic case is enforced through the reproducer. A fix for branch operators
not skipping tasks inside task groups was held before merge because the linked
issue's reproducer used an absolute task id and, when someone ran that exact
repro on a current release, it behaved correctly — the real reported behaviour
traced to a different change in mapped task groups and was tracked separately
(#65751). Without that check the PR would have changed skip semantics for every
branch operator on the strength of a bug it did not fix.

Changes that alter what a base construct *is* — rather than what it does — are
routed away from PRs entirely: a task-group retries proof of concept was told the
change to how task groups work needed a devlist discussion first (#61809), and an
iterable/streaming operator was assessed as "tantamount to a parallel scheduler,
executor and triggerer implementation" and closed in favour of an AIP (#42572).

## Decision

**A change to a base class is evaluated against the installed base of Dags and
provider subclasses it cannot see. Within a release series, a Dag that parses and
runs before the change must parse and run after it.**

The qualifier is load-bearing.
[ADR-1](0001-base-classes-are-a-stable-public-api.md) mandates a
`RemovedInAirflow4Warning` cycle for retiring surface, and that cycle *ends* in a
removal at the next major. An absolute reading of the headline would forbid the
removal step of the project's own deprecation process and make every Airflow-4
removal un-landable. This decision governs the current series; the major boundary
is where ADR-1's cycle is allowed to complete.

- **New validation ships as a warning in the current series.** A check added in
  `__init__`, the metaclass, or the decorator that turns a legal Dag into a parse
  error is not shipped mid-series, even when the input it rejects is a mistake and
  the resulting error message is better. Emit a warning instead. The warning is
  what earns the right to raise: once it has shipped for a series, promoting it to
  an exception at the next major is the ordinary completion of ADR-1's cycle, not
  a violation of this one. New validation that raises *without* having gone
  through that warning period is what this decision refuses.
- **Enumerate who else is affected before validating.** Validation of a field
  consumed by pluggable machinery — executors, providers, backends — has to hold
  for every implementation, not the one that motivated it. If it cannot, the
  check belongs in that implementation.
- **Verify the reproducer before changing semantics.** Run the linked issue's
  reproducer against current `main` and confirm it fails. A semantics change to
  skipping, poking, rescheduling, or mixin cooperation on the strength of a
  repro that does not reproduce changes behaviour for everyone and fixes nobody;
  say plainly which reported behaviour the change does and does not cover.
- **Changing what a construct means goes to an AIP or the devlist.** Reworking
  the semantics of task groups, operator expansion, or the execute lifecycle is
  an architecture decision, not a PR, however small the diff that expresses it.
- **Prefer additive capability.** A new sibling method, mixin, or opt-in
  attribute leaves existing subclasses untouched; a change to an existing
  method's shape or behaviour does not.

## Consequences

Some genuinely bad authoring patterns keep producing confusing failures rather
than clear ones, because the clear failure would require rejecting input that is
currently legal. The base classes accumulate tolerance for input the project
would not choose today. Contributors — often newcomers, since "improve this
error message" is a natural first task — find their change closed for reasons
that are not visible in the file they edited.

The alternative is worse in a way this project has repeatedly judged decisive:
every operator in `providers/` and every operator in every user repository is a
subclass, and a parse-time rejection or a silent semantics shift lands in all of
them on upgrade, with no migration path the project controls. Tolerance here is
the price of the inheritance surface being genuinely public.

A change **violates** this decision when it:

- adds a validation, assertion, or raise in `BaseOperator.__init__`,
  `BaseOperatorMeta`, `BaseSensorOperator`, `BaseHook`, or `decorator.py` that
  rejects input previously accepted, where no deprecation warning for that input
  has shipped in a prior release — or where one has, but the raise is landing
  mid-series rather than at a major boundary.
- validates a field consumed by pluggable machinery against the rules of one
  implementation.
- changes skip, branch, poke, reschedule, or mixin behaviour with no test that
  reproduces the linked issue's reported symptom.
- alters what an authoring construct means to subclasses without an AIP or a
  devlist thread linked in the PR.
- changes an existing method's signature or behaviour where an additive sibling
  would serve.
- bundles unrelated commits into a base-class change, so the blast radius cannot
  be assessed from the diff.

Two further checks are *reviewer prompts*, not violation bullets, because
settling them means checking out the branch and running the reproducer — which no
automated pass over a diff can do. Raise them as questions on the PR:

- does the linked issue's reproducer still fail on current `main`?
- does the diff address the behaviour the issue actually reports, or a
  neighbouring one?

## Evidence

- #55252 — a clearer error when a task method name shadows a `time` or
  `builtins` name; closed as a non-starter because the check would break a good
  portion of Dags that work today.
- #53459 — `executor_config` key validation in `BaseOperator`; withdrawn after
  review showed executors beyond `KubernetesExecutor` were affected.
- #65751 — branch operator not skipping inside task groups; held before merge
  because the linked reproducer passed on a current release and the real
  behaviour traced elsewhere.
- #61809 — task-group retries proof of concept; redirected to the devlist as a
  change to what task groups are.
- #42572 — streaming `IterableOperator` / `DeferredIterable`; closed in favour of
  an AIP after being assessed as a parallel scheduler, executor and triggerer.
- #60363 — a metric re-enablement bundled with unrelated commits; the mixed
  scope was the first thing review asked to split.
