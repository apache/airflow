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

The Execution API is the wire contract between the Task SDK running on workers
and the API server. In real deployments these two halves are upgraded on their
own schedules: an operator may roll the API server forward while a fleet of
workers still runs an older SDK, or pin workers while the control plane moves.
There is no moment where "everyone is on the same version" can be assumed, so
the contract must let an older client talk to a newer server and negotiate what
each side understands.

Airflow uses [Cadwyn](https://github.com/zmievsa/cadwyn) with CalVer
(`versions/vYYYY_MM_DD.py`) to express this. Each version file is a set of
`VersionChange` classes describing how the current internal schema is migrated
*back* to what an older client expects. Cadwyn replays these migrations as a
linear chain keyed by date, so the ordering and the released/unreleased boundary
of those files is load-bearing — not cosmetic. The API server advertises the set
of versions it supports and the client selects one it also understands.

Because the chain is replayed in date order, a version file that has already
shipped in a release is frozen: clients in the field pin to those dates. Editing
a released file, or inserting a new file with a date that sorts *between* two
already-released files, rewrites history the deployed clients already depend on.

## Decision

Every wire-visible change to the Execution API goes through a Cadwyn
`VersionChange` placed in the latest **unreleased** version file
(`versions/vYYYY_MM_DD.py`).

- **The unreleased head is identified by an explicit marker, not by its date.**
  A version file's `vYYYY_MM_DD` name says when the version was opened, not
  whether it has shipped: `v2026_06_30.py` was the open head well past
  2026-06-30, because unreleased changes were collapsed onto it rather than
  opening a new date. Any rule that infers "released" from "date is in the past"
  will wrongly reject a PR that correctly edits the head — and wrongly accept one
  that edits a released file whose date happens to be recent.

  This makes the rule mechanically undecidable as written today, and that is a
  gap to close in code: `versions/__init__.py` should declare the head
  explicitly — e.g. a `HEAD_VERSION` / `RELEASED_VERSIONS` constant next to
  `bundle` — so both reviewers and automated checks read the marker instead of
  parsing dates. Until that constant exists, treat the head as undecidable from
  the diff alone and confirm it against the version-collapse history on `main`
  rather than firing on a date comparison.
- If the head version file is still unreleased, add the `VersionChange` there.
  Otherwise create a new `vYYYY_MM_DD.py` for the next unreleased date and
  register it in `versions/__init__.py`.
- New endpoints declare `endpoint("/path", ["METHOD"]).didnt_exist`; new or
  renamed fields declare `schema(Model).field("name").didnt_exist` — so an
  older server correctly reports "this did not exist yet" and a newer client
  against an older server fails cleanly through negotiation instead of hitting a
  silent shape mismatch.
- Never edit an already-released version file, and never backdate a new version
  to a date that sorts between two existing releases.

## Consequences

- Independent deploys stay compatible: a worker on last quarter's SDK and a
  server on this quarter's build negotiate a version both understand.
- The unreleased "head" file accumulates the pending contract changes for the
  next release; the release process draws the line and the file becomes frozen.
- Contributors must think in terms of the migration chain, not just the current
  schema — the extra `VersionChange` boilerplate is the cost of safe rolling
  upgrades.

**A violating change looks like:** adding a field, endpoint, or response shape
change to the datamodels/routes with no accompanying `VersionChange`; putting
the `VersionChange` into an already-released version file; or backdating a new
version file so it sorts between existing releases. Any of these breaks version
negotiation — an older client either 500s against the new server or silently
mis-parses a payload, exactly the failure the versioning scheme exists to
prevent.

## Evidence

- #62343 — Async connection testing via workers added its endpoint behind a Cadwyn version so older clients negotiate cleanly.
- #66073 — AIP-103 task/asset-state endpoints were introduced via `endpoint(...).didnt_exist` in the unreleased version file.
- #68390 — Worker-bound TaskInstance fields were versioned in the Execution API schema rather than changed in place.
