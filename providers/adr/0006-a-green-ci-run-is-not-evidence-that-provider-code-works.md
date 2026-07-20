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

# 6. A green CI run is not evidence that provider code works

Date: 2026-07-20

## Status

Accepted

## Context

Provider code is the boundary between Airflow and a third-party service that
neither the reviewer nor CI can reach. Unit tests here mock the vendor client,
so a green CI run proves the code does what its own mocks were told to expect —
not that it works against the service. There is no test in this repository that
can catch "the AWS call actually fails this way", "that Tableau keyword only
exists from client 0.35", or "this Databricks hook cannot be used from a
synchronous operator". Only the author can supply that, and only by running it.

This is not a general "write good tests" position — that lives in the repo-root
`CLAUDE.md` and applies everywhere. It is specific to this directory: the usual
signal a reviewer relies on to gate a merge is, here, structurally incapable of
saying anything about the change. Everywhere else in the repository, green CI
plus a read of the diff is a defensible basis for merging. In `providers/` it is
not, and the gap has to be filled by the author.

That is what makes the **unverified fix** so expensive. A plausible change is
written from reading the traceback rather than reproducing it, CI is green
because the mocks agree, and review has nothing to push back on except a request
for evidence. When that evidence is finally produced, the fix frequently turns
out to address a different failure than the reported one — the real cause was
elsewhere, or the condition the fix guards against never occurs. Several such
PRs were withdrawn by their own authors as soon as they ran the scenario. Worse,
in one case the supporting evidence in the PR body was fabricated outright: a
library version and release date that did not exist, and documentation links
that pointed at a local editor path. The change as written would have broken
every user on the older client, because the new keyword was forwarded
unconditionally.

The volume matters. Low-effort, machine-generated provider PRs arrive faster
than they can be reviewed and have been closed in bulk — not because tooling is
banned, but because a generated diff carries no evidence of a run, which is the
one thing this directory needs and cannot get from CI.

## Decision

A provider change that claims to alter service-facing behaviour is accepted on
evidence of a run against the real service, not on a green CI run.

- **"CI is green" is not evidence for provider code.** For a behaviour fix, the
  PR body carries the reproduction before and the run after, against the actual
  service or a faithful local stand-in for it.
- **Cite the vendor accurately.** Version numbers, release dates and API
  behaviour referenced in a PR must be checkable and correct; a claim about when
  a client-library capability shipped decides whether the change breaks existing
  users.
- **A capability that only exists in a newer vendor client is gated or floored.**
  Forwarding a new keyword unconditionally breaks every user still on the old
  client, and no test in this repository will show it.
- **Where the author cannot run it, say so.** A PR that states plainly that the
  change is unverified is reviewable — a reviewer with access can then take it
  on. A PR that presents an unrun change as verified is not.

## Consequences

- Provider fixes that merge are ones somebody actually ran, which is the only
  practical substitute for integration coverage the project cannot have.
- Contributors without access to the service are limited in what they can fix
  here. That is a real barrier, and it is preferred to shipping guesses into a
  released distribution.
- Some reports stay open longer while a reproduction is found. Review treats
  that as cheaper than merging a change that moves the failure somewhere less
  visible.
- Review time on this directory is spent asking for evidence rather than reading
  code, which is a poor use of it — this file exists so the ask arrives before
  the PR does.

A change **violates** this decision when it:

- claims to fix a service-facing bug with no reproduction, no traceback and no
  evidence of a run against the real service;
- cites a vendor version, release date, or API behaviour that does not check
  out, or links to evidence that cannot be opened;
- adds a keyword, parameter or call unconditionally that only exists in a newer
  vendor client, without raising the floor or gating on the version.

Root-cause discipline — whether the change addresses the failing mechanism or
its symptom, and whether the vendor has already fixed it upstream — is a review
criterion rather than a violation bullet, because it cannot be settled from the
diff. It is in the "Evidence and root cause" checklist in `providers/AGENTS.md`.

## Evidence

- #64305 — a Celery worker health-check fix, closed with: the linked issue
  already explains why this approach is wrong for Airflow, "you are required to
  show evidence the PR works", and there is no sign it was tested against a real
  Celery environment.
- #59150 — a Snowflake connection change, closed with an explicit offer to
  reopen on evidence it was tested against a real Snowflake instance.
- #67429 — a Tableau incremental-refresh parameter whose supporting evidence was
  fabricated (wrong client version, wrong release date, unopenable links), and
  which would have broken every user on the older client because the new keyword
  was forwarded unconditionally. Reopening was made conditional on a floor bump
  and on confirmation the change was tested against a real Tableau Server.
- #69520 — dropping a core model to fix an Elasticsearch log path; withdrawn by
  the author once the model's other consumers surfaced, and reopened as a
  root-cause fix.
- #68720 — a deferrable Beam operator fix withdrawn by its author: it removed
  the immediate error without addressing why the deferral happened too late.
- #65833, #61489, #61903 — Edge-worker and Git-bundle race fixes closed once the
  actual cause was found and fixed elsewhere.
- #67341 — a Databricks dependency bump closed as unnecessary: the upstream
  project's own fix would resolve CI on release.
- #67760, #67759, #67758, #67752 — a batch of low-effort automated PRs closed
  together, including a typo fix applied to the wrong file.
