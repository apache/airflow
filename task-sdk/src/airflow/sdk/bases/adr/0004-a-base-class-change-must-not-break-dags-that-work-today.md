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
interface: signatures, parameters, and the deprecation cycle. This decision
covers what a diff can break without touching a signature at all — the set of Dags
that currently parse and run.

Two kinds of change do this. *New validation* — a check in
`BaseOperator.__init__`, `BaseOperatorMeta._apply_defaults`, or the `@task`
decorator that rejects previously-accepted input — converts working Dags into
parse-time failures across every deployment that upgrades, including Dags this
project cannot see. *Changed semantics* on a path that already has behaviour —
skip propagation, sensor reschedule, mixin `super()` cooperation — change outcomes
silently, with no error. An error message for a task method name colliding with a
`time`/`builtins` name was closed as a non-starter that "would almost certainly
break a good portion of Dags that work today" — even though it aimed at a *better
error*, because the check would reject names legal today (#55252); `executor_config`
key validation was withdrawn once review showed executors beyond
`KubernetesExecutor` were affected (#53459).

The semantic case is enforced through the reproducer: a branch-operator skip fix
was held because the linked repro passed on a current release, the real behaviour
tracing to mapped task groups and tracked separately (#65751). Changes to what a
base construct *is* are routed away from PRs entirely — a task-group retries PoC
was sent to the devlist (#61809), and a streaming operator assessed as "tantamount
to a parallel scheduler, executor and triggerer implementation" was closed for an
AIP (#42572).

## Decision

**A change to a base class is evaluated against the installed base of Dags and
provider subclasses it cannot see. Within a release series, a Dag that parses and
runs before the change must parse and run after it.**

The qualifier is load-bearing. [ADR-1](0001-base-classes-are-a-stable-public-api.md)'s
`RemovedInAirflow4Warning` cycle *ends* in a removal at the next major; an
absolute reading of the headline would forbid that removal step and make every
Airflow-4 removal un-landable. This decision governs the current series; the major
boundary is where ADR-1's cycle completes.

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
than clear ones, because the clear failure would reject input that is currently
legal; the base classes accumulate tolerance for input the project would not
choose today. Contributors — often newcomers, since "improve this error message"
is a natural first task — find their change closed for reasons not visible in the
file they edited.

The alternative is worse: every operator in `providers/` and every user repo is a
subclass, so a parse-time rejection or silent semantics shift lands in all of them
on upgrade, with no migration path the project controls. Tolerance here is the
price of the inheritance surface being genuinely public.

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

- #55252 — clearer error for a task method name shadowing `time`/`builtins`;
  closed because the check would break Dags that work today.
- #53459 — `executor_config` key validation; withdrawn after review showed
  executors beyond `KubernetesExecutor` were affected.
- #65751 — branch operator skip fix; held because the linked reproducer passed on
  a current release and the real behaviour traced elsewhere.
- #61809 — task-group retries PoC; redirected to the devlist.
- #42572 — streaming `IterableOperator`/`DeferredIterable`; closed for an AIP as a
  parallel scheduler/executor/triggerer.
- #60363 — metric re-enablement bundled with unrelated commits; review asked to
  split the mixed scope first.
