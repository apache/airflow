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

# 5. Dependency and tool versions are bumped by automation, not by hand

Date: 2026-07-20

## Status

Accepted

## Context

Dev tooling's versions are pinned in many places — `dev/breeze` dependencies, CI
environment versions, hook tool versions, base image tags, and the dependency ranges of
the `dev/` plugin templates. Each is a one-line edit that looks like an ideal "good first
PR", and bumping by hand goes wrong in four ways the closed PRs show:

- **The bump is already in flight.** The dependency bots and the periodic "upgrade
  important CI environment" run propose these on a schedule; a hand-written bump lands
  next to the automated one and one is closed as duplicate after both burned a CI cycle.
- **The cap is deliberate.** Some upper bounds exist because a newer release has a known
  defect; raising one "because a new version is out" silently reintroduces the bug, and
  the reason is in a linked upstream issue, not the diff.
- **When automation picks a wrong version, the pin is the symptom.** A daily-edge image
  tag instead of a release tag is a defect in the selection predicate; editing the pin
  leaves the bumper free to repeat the choice next run.
- **A version bump is rarely only a version bump.** Upgrading a formatter, doc generator
  or hook tool changes its output repository-wide; the regenerated output belongs in the
  same PR, and a hand-rolled bump that skips it breaks everyone else's commit.

A related case: the `dev/` template dependency ranges — the react plugin template in
particular — are not free-floating. Generated plugins run against the host Airflow UI's
externalised globals, so a template floor above what the host bundle ships produces
packages that cannot run.

## Decision

- **Version pins under `dev/` are moved by the automated flows** — the grouped
  dependency bots and `upgrade important CI environment` — not by hand-written
  pull requests.
- **A pin with an upper bound is assumed intentional.** Raising one requires
  stating why raising it is now safe — the upstream fix, the release that resolved
  the defect, the re-run that passes. Where the original reason *is* recorded
  (a linked issue, a comment at the pin), name it; where it is not, the burden is
  evidence that the new version works, not archaeology. Caps under `dev/` are
  sparse and added in the middle of incidents — `dev/breeze/pyproject.toml` carries
  exactly one (`flit_core <4`) — so a contributor often cannot cite a reason
  nobody wrote down.
- **A security fix or an active incident overrides the automation-first rule.**
  A CVE in a pinned tool, or a pin that is currently breaking builds, is bumped by
  hand immediately; the pull request states the CVE or the failure it resolves and
  does not wait for the next bot run. Reverting an incident pin once upstream is
  fixed (#62503 → #62602) is the same case.
- **When automation selects a wrong version, the fix goes into the selection
  logic**, with the mis-selected version handled as a regression test, and the
  bad run is redone rather than patched over.
- **A bump that changes generated or formatted output ships that output in the
  same pull request**, produced by running the tool across the whole repository.
- **Template dependency ranges stay within what their host provides**, and are
  moved together with the host's own versions when they must move.

## Consequences

- The pin history stays machine-readable and consistent, avoiding the duplicate-PR churn
  hand bumps create.
- Genuinely urgent upgrades — a security fix, an upstream break — take the hand-bump path
  in the Decision, with the reason stated, rather than waiting on a schedule.
- Contributors seeking a small first contribution have to be redirected, because bumping a
  pin looks ideal and is not.
- The project carries the cost of maintaining the bumper itself, its selection predicates
  and cooldown behaviour.

A change **violates** this decision when it:

- edits a version pin under `dev/` with no accompanying reason beyond "a newer
  version exists";
- raises an existing upper bound without stating why raising it is now safe —
  unless the raise is a security fix or incident response, in which case the pull
  request states the CVE or the failure it resolves and the bullet does not apply;
- pins around a bad automated selection instead of fixing the selection logic;
- upgrades a formatter, generator or hook tool without including the
  repository-wide output that the new version produces;
- moves a template's dependency range above the version its host runtime
  actually provides.

## Evidence

- #52658 — a click upper-bound raise declined: the cap was held because of an upstream
  bug in boolean flags fed from `false` environment variables.
- #62503 → #62602 — "Temporarily pin virtualenv", then "Upgrade Hatch to 1.16.5 and
  revert virtualenv pin": both merged by hand, days apart, because builds were broken;
  neither waited for the bots, and the cap was never written down — why the bullet asks
  whether raising is *now safe* rather than why the cap was added.
- #66580 — an automated CI-environment bump that selected an Alpine daily-edge tag over
  the release tag; closed and redone as #66600 after the selection predicate was fixed.
- #65907 — a hand-written pip bump closed once it was clear the automated run would pick
  it up the following week.
- #58305, #57361 — hand-written and bot-written bumps of the same tool racing each other,
  one of each pair closed as superseded.
- #65221 — a doc-generator upgrade that required running the hook across all files and
  committing the regenerated output in the same change.
- #67741 — a template dependency update closed because it raised the React and Chakra
  floors above the versions the host Airflow UI bundle provides.
- #67649, #67008, #63227, #61604 — grouped dependency PRs routinely closed, recreated or
  superseded by the bots' own subsequent runs.
