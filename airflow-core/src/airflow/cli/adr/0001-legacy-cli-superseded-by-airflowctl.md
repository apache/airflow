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

# 1. The local `airflow` CLI is superseded by airflowctl (AIP-94)

Date: 2026-07-18

## Status

Accepted

## Context

AIP-94 migrates CLI functionality away from the local, in-process `airflow`
command toward the `airflowctl` client, which talks to Airflow exclusively
through the public REST API. Under the post-3.0 architecture, only the API
server is allowed to touch the metadata database (see the Architecture
Boundaries in the project instructions); a local CLI that reaches into
`DagModel`, session objects, or manager classes directly violates that
boundary and cannot run against a remote deployment.

The local CLI is therefore legacy and deliberately shrinking. Commands are
being marked as "migrated to airflowctl" one area at a time (for example
`dags pause` / `dags unpause`), and the corresponding implementation moves to
a shared `ctl/commands/…` module that both the client and any remaining
local shim call. Adding new capabilities to the legacy CLI, or re-adding a
capability that a generic `airflowctl` command already covers, works against
this migration and grows the surface that still has to be ported.

## Decision

- Do not add NEW user-facing features to the legacy local `airflow` CLI.
- Route CLI data access through the `airflowctl` client / public REST API —
  never through direct database sessions, `DagModel`/ORM queries, or manager
  classes from a CLI command handler.
- Share logic by calling the common `ctl/commands/…` implementations rather
  than duplicating command bodies in the local CLI.
- Do not re-add a capability that is already covered by a generic
  `airflowctl` command; extend the generic command instead.
- New CLI-adjacent work belongs on the `airflowctl` side of the migration.

## Consequences

The legacy CLI stops accreting new surface, so the AIP-94 migration converges
instead of chasing a moving target. Contributors are steered to add
capabilities once, in the shared `ctl/commands` layer, where both the remote
client and any transitional local path pick them up. This ADR is
transitional and should be revisited once the migration to `airflowctl`
completes.

A VIOLATING change looks like: a PR that adds a brand-new subcommand or flag
to the legacy local CLI; a CLI handler that opens a DB session or queries an
ORM model / manager directly instead of going through the client/API; a
command body copy-pasted into the local CLI instead of calling the shared
`ctl/commands/…` implementation; or re-introducing a command whose function a
generic `airflowctl` command already provides.

## Evidence

- #65519 — new `team_name` support added to legacy CLI pool creation; closed rather than extending the legacy CLI.
- #66223 — legacy-CLI JSON-output cleanup that overlapped work better done in the migrated command path; closed.
- #68650 — `dags pause`/`unpause` marked as migrated to airflowctl, moving the behavior to the shared command layer.
- #58584 — proposed new `airflow db would-migrate` legacy-CLI command; closed rather than growing the legacy surface.
