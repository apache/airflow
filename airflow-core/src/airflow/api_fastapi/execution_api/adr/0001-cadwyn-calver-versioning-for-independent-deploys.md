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

# 1. Cadwyn CalVer versioning because workers and servers deploy independently

Date: 2026-07-18

## Status

Accepted

## Context

The Execution API is the wire contract between the Task SDK on workers and the API
server, and the two halves are upgraded on their own schedules — an operator may roll
the server forward while a fleet of workers runs an older SDK, or pin workers while the
control plane moves. There is no moment where "everyone is on the same version" holds,
so the contract must let an older client talk to a newer server and negotiate what each
side understands.

Airflow uses [Cadwyn](https://github.com/zmievsa/cadwyn) with CalVer
(`versions/vYYYY_MM_DD.py`): each version file holds `VersionChange` classes describing
how the current internal schema is migrated *back* to what an older client expects.
Cadwyn replays these as a linear chain keyed by date, so the ordering and the
released/unreleased boundary are load-bearing, not cosmetic. A file that has shipped in a
release is frozen — clients in the field pin to its date — so editing a released file, or
inserting one that sorts *between* two released files, rewrites history deployed clients
depend on.

## Decision

Every wire-visible change to the Execution API goes through a Cadwyn `VersionChange`
placed in the latest **unreleased** version file (`versions/vYYYY_MM_DD.py`).

- **The unreleased head is identified by an explicit marker, not by its date.** A file's
  `vYYYY_MM_DD` name says when the version was opened, not whether it has shipped:
  `v2026_06_30.py` was the open head well past 2026-06-30, because unreleased changes were
  collapsed onto it rather than opening a new date. Any rule that infers "released" from
  "date is in the past" will wrongly reject a PR that correctly edits the head — and
  wrongly accept one that edits a released file whose date happens to be recent.

  This makes the rule mechanically undecidable as written today, a gap to close in code:
  `versions/__init__.py` should declare the head explicitly — e.g. a `HEAD_VERSION` /
  `RELEASED_VERSIONS` constant next to `bundle` — so reviewers and automated checks read
  the marker instead of parsing dates. Until that constant exists, treat the head as
  undecidable from the diff alone and confirm it against the version-collapse history on
  `main` rather than firing on a date comparison.
- If the head version file is still unreleased, add the `VersionChange` there. Otherwise
  create a new `vYYYY_MM_DD.py` for the next unreleased date and register it in
  `versions/__init__.py`.
- New endpoints declare `endpoint("/path", ["METHOD"]).didnt_exist`; new or renamed fields
  declare `schema(Model).field("name").didnt_exist` — so an older server reports "this did
  not exist yet" and a newer client against an older server fails cleanly through
  negotiation instead of hitting a silent shape mismatch.
- Never edit an already-released version file, and never backdate a new version to a date
  that sorts between two existing releases.

## Consequences

- Independent deploys stay compatible: a worker on last quarter's SDK and a server on this
  quarter's build negotiate a version both understand.
- The unreleased "head" file accumulates pending contract changes for the next release; the
  release process draws the line and freezes it.
- Contributors think in terms of the migration chain, not just the current schema — the
  extra `VersionChange` boilerplate is the cost of safe rolling upgrades.

**A violating change looks like:** adding a field, endpoint, or response shape change to
the datamodels/routes with no accompanying `VersionChange`; putting the `VersionChange`
into an already-released version file; or backdating a new version file so it sorts between
existing releases. Any of these breaks version negotiation — an older client either 500s
against the new server or silently mis-parses a payload, exactly the failure the versioning
scheme exists to prevent.

## Evidence

- #62343 — async connection testing added its endpoint behind a Cadwyn version.
- #66073 — AIP-103 task/asset-state endpoints introduced via `endpoint(...).didnt_exist`.
- #68390 — worker-bound TaskInstance fields versioned rather than changed in place.
