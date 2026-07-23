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

# 4. How an operator fails is part of its public contract

Date: 2026-07-20

## Status

Accepted

## Context

Dag authors write code against the *failure* behaviour of these operators, not
only their success: an `on_failure_callback` inspecting `context["exception"]`, a
`try`/`except` in a subclass, a retry policy tuned to one error class, a sensor's
soft-fail handling — all depend on which exception type reaches them and whether a
condition raises at all.

This collides with the repo-wide direction to reduce direct `AirflowException`
raises in favour of specific built-ins and dedicated classes. That direction is
enforced *forward-only*, by a prek hook against a generated inventory: new usages
are blocked, existing ones are not rewritten wholesale. A sweep replacing
`AirflowException` with built-ins in one Google service looked like exactly that
cleanup, but review stopped it: for a released provider, every user whose callback
branches on `AirflowException` breaks silently on upgrade with no signature diff to
hint at it — a decision the project makes once on the dev list with a "Breaking
Change" changelog entry, not per-service in review. The mirror case is just as
live: adding a `try`/`except` so a component stops crashing turns a visible failure
into a silent one (a proposed Stackdriver guard applied only to one transport, so
on the default transport it suppressed nothing while looking handled), and removing
a "redundant" guard is only redundant if nothing downstream distinguishes that
path. The rule is not "never change exception behaviour" — it is that doing so is a
user-facing change and is treated as one.

## Decision

The exception type an operator or hook raises, and the conditions under which it
raises at all, are treated as public API for this provider.

- **Do not convert an existing `AirflowException` raise to another type as a
  cleanup.** The project's reduction effort is forward-only *here*: write new
  code with a specific built-in or a dedicated class, and leave released raise
  sites alone unless the change is being made deliberately.

  This is a deliberate, scoped exception to the repo-root `CLAUDE.md` sentence
  "When you touch code that already raises `AirflowException`, prefer narrowing
  it to a more specific exception". That guidance is right for airflow-core,
  where the exception type is an implementation detail behind a version the user
  upgrades knowingly. It is wrong for a **released provider raise site**: the
  provider ships on its own cadence, a user's `except AirflowException:` in a
  callback is silently rewritten by a wheel bump they did not ask for, and there
  is no signature diff to warn them. The narrowing guidance therefore does not
  apply to raise sites already released from this provider; it applies fully to
  new code and to unreleased ones.
- **A deliberate conversion needs a decision and a changelog entry** — dev-list
  agreement where it is broad, and a "Breaking Change" entry in
  `providers/google/docs/changelog.rst` naming the old and new types.
- **New code raises a specific type**: a Python built-in (`ValueError`,
  `TypeError`, `OSError`, …) or a dedicated class. Never a new direct
  `AirflowException` — with the repo-wide carve-out that relocating an
  already-existing raise verbatim during a refactor is not a new usage. The
  `check-no-new-airflow-exceptions` prek hook enforces this mechanically, so a
  human reviewer's attention is better spent on the two bullets below, which no
  hook can check.
- **Do not add a `try`/`except` that turns a failure into a silent success.** A
  guard must be shown to catch the condition that actually occurs, on the code
  path that actually runs; where a component genuinely must not crash, it logs
  and fails visibly rather than returning an empty result.
- **Do not remove an existing guard** without establishing that no caller,
  sensor, or callback distinguishes the path it protected.
- **Keep the sync and deferred paths classified identically.** A condition that
  raises in `execute()` raises the equivalent type from `execute_complete()`,
  and the trigger carries enough detail in its event for that to be possible.
- **Failure-behaviour changes are stated in the PR body**, in the terms a Dag
  author would use: what used to raise, what raises now, and what a user has to
  change.

## Consequences

- Upgrading this provider does not silently change which branch of a user's
  callback runs.
- Exception hygiene improves more slowly than a mechanical sweep would — the
  accepted trade for not breaking released behaviour.
- Reviewers must weigh a class of change that looks purely internal, making some
  small PRs disproportionately expensive to review; this file makes that cost
  predictable.
- The changelog carries entries for changes with no signature diff — exactly the
  case where users have no other warning.

A change **violates** this decision when it:

- rewrites **released** `AirflowException` raise sites in this provider to
  another type without a linked decision and a "Breaking Change" changelog entry.
  This is about *released provider* raise sites specifically; narrowing an
  existing raise site in code you are already changing is what the repo-root
  `CLAUDE.md` asks for, and is not this bullet. #69786 (airbyte,
  `AirflowException` → `RuntimeError`) is the shape the root instruction wants,
  not a violation;
- adds a new direct `raise AirflowException(...)` **in new code** — moving an
  already-existing raise verbatim during a refactor is explicitly not a new
  usage, as the repo-root `CLAUDE.md` says. A PR that extracts a helper so a
  retry decorator can see the raw vendor error, carrying the pre-existing
  friendly-message raise into it, shows up in
  `known_airflow_exceptions.txt` as a count change and is still a relocation
  (#68361);
- introduces a `try`/`except` that lets an operator, sensor, log handler, or
  secret backend return successfully — or return empty — where it previously
  surfaced an error, without evidence that the caught condition is the one that
  occurs;
- removes an existing exception guard without establishing that no caller
  depends on it;
- changes a raised type in the synchronous path only, leaving the deferred path
  classifying the same condition differently;
- alters failure behaviour without describing the change, in user terms, in the
  PR body.

## Evidence

- #67769 — Cloud Run `AirflowException` → built-ins: closed by its author after
  review flagged a breaking change for callback-branching users, needing a dev-list
  discussion and "Breaking Change" entry; request-changes used to prevent merge.
- #61008 — review declined a Bigtable swap of `RuntimeError` for an Airflow
  exception, keeping the existing raise: the direction is new code, not churning old.
- #60727 — Bigtable delete-instance removal of a "redundant" `try`/`except`, closed.
- #68295 — a Stackdriver guard put on hold once it was clear the `try`/`except`
  worked for only one transport, not the default — a fix that only looked like one.
- `check-no-new-airflow-exceptions` and its generated inventory: the mechanism that
  makes the reduction forward-only, so an in-place rewrite is a deliberate act.
