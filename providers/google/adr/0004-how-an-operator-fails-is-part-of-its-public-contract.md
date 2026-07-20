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
only against their success behaviour. An `on_failure_callback` that inspects
`context["exception"]`, a `try`/`except` in a custom operator subclass, a
`retry_exponential_backoff` policy tuned to one class of error, a sensor's
soft-fail handling — all of these depend on which exception type reaches them
and on whether a given condition raises at all.

This collides with a repository-wide direction the project is otherwise
committed to: reducing direct `AirflowException` raises in favour of specific
built-ins and dedicated classes. That direction is enforced forward-only, by a
prek hook against a generated inventory of existing raise sites: new
`AirflowException` usages are blocked, existing ones are not rewritten
wholesale. The forward-only shape is deliberate and is what this ADR records for
this package.

A sweep through one Google service replacing `AirflowException` with Python
built-ins looked like exactly the cleanup the project wants. Review stopped it:
for a released provider, every user whose callback or subclass branches on
`AirflowException` breaks silently on upgrade, with no Dag change to signal it
and nothing in the operator's signature to hint at it. The change was not
rejected as wrong — it was redirected to the dev list and a "Breaking Change"
changelog entry, because that is a decision the project makes once, not
per-service in review.

The mirror-image case is just as live. Adding a `try`/`except` so a component
stops crashing changes a visible failure into a silent one. In the Stackdriver
logging path a proposed guard turned out to apply only to one transport, so on
the default transport it would have suppressed nothing while suggesting the
problem was handled — the author put it on hold pending a real traceback rather
than ship the appearance of a fix. Similarly, removing a "redundant"
`try`/`except` around a Bigtable delete is only redundant if nothing downstream
distinguishes that path.

The rule that follows is not "never change exception behaviour". It is that
changing it is a user-facing change and is treated as one.

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
- Exception hygiene improves more slowly here than a mechanical sweep would
  achieve. That is the accepted trade for not breaking released behaviour.
- Reviewers must weigh a class of change that looks purely internal, which makes
  some small PRs disproportionately expensive to review — this file exists to
  make that cost predictable rather than surprising.
- The changelog carries entries for changes with no signature diff, which is
  exactly the case where users have no other warning.

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

- #67769 — "Google Cloud Run: replace `AirflowException` with Python built-in
  exceptions": closed by its author after review flagged it as a breaking change
  for anyone relying on `AirflowException` in `on_failure_callback` /
  `on_retry_callback`, requiring a dev-list discussion and a "Breaking Change"
  changelog entry. Requesting changes was explicitly used to prevent accidental
  merge.
- #61008 — "Refactor exception handling in BigTable": review declined a swap of
  `RuntimeError` for an Airflow exception, keeping the existing raise as-is and
  noting the project's direction is to avoid Airflow exceptions in new code
  rather than to churn old ones.
- #60727 — a Bigtable delete-instance operator change to remove a "redundant"
  `try`/`except`, closed without merging.
- #68295 — a Stackdriver remote-logging guard put on hold by its author on the
  realisation that the `try`/`except` would only be effective for one transport
  and not for the default one — a guard that would have looked like a fix
  without being one.
- The repository-wide `check-no-new-airflow-exceptions` hook and its generated
  inventory of existing raise sites: the mechanism that makes the reduction
  effort forward-only, which is why an in-place rewrite is a deliberate act
  rather than a cleanup.
