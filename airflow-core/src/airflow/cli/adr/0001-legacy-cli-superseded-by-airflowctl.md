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

Two boundaries on that statement matter, because without them this ADR reads as
forbidding work the project merges routinely.

**Not everything in the local CLI can move.** airflowctl is a remote REST client;
it can only express what the API server can be asked to do. `airflow scheduler`,
`airflow standalone`, `airflow db` and the local execution paths (`dags test`,
`tasks run`) are local by nature — they start a process on this host or operate on
the metadata database before an API server necessarily exists. These commands are
not waiting to be ported, so improving them is not surface that "still has to be
ported". The 2026 record is unambiguous: #62055 added `--only-idle` to `airflow
scheduler`, #56663 added include/exclude arguments to `db clean`, #62234
re-introduced `--use-migration-files`, #65575 forwarded MySQL SSL parameters to
`db shell`, and #62344 changed sensitive-value handling in the local `connections`
/ `variables` listings. None of them had an airflowctl home to go to.

**Direct database access from a local CLI handler is the existing pattern, not an
aberration.** `provide_session` / `create_session` appear at 44 sites across ten
modules under `cli/commands/`. Those commands *are* the local-administration path
and the boundary they must respect is the one in the project's Architecture
Boundaries — schedulers, workers and Dag processors do not touch the metadata
database; an operator running `airflow db clean` on the database host does. What
this ADR forbids is a *new remote-capable* command reaching into the ORM instead of
going through the client, not the pre-existing local-administration commands.

## Decision

- Do not add a new *remotely-expressible* capability to the legacy local
  `airflow` CLI — anything the API server can be asked to do belongs on
  `airflowctl`.
- **Local-only commands are out of scope for the migration and stay
  maintainable.** `scheduler`, `standalone`, the `db` group, and the local
  execution paths (`dags test`, `tasks run`) start processes or touch the
  metadata database on this host; adding a flag to one of them is ordinary
  maintenance, not new legacy surface. This is the same "prefer a flag on the
  existing command" preference [ADR 3](0003-no-new-command-for-a-capability-that-already-exists.md)
  states — the two agree: ADR 3 decides *whether* the capability is new, this ADR
  decides *where* genuinely new remote capability goes.
- Route **new** remote-capable CLI data access through the `airflowctl` client /
  public REST API rather than through fresh direct database sessions,
  `DagModel`/ORM queries, or manager classes. The existing `provide_session`
  usage in the local-administration commands is the established pattern and is
  not a defect to be reported.
- Share logic by calling the common `ctl/commands/…` implementations rather
  than duplicating command bodies in the local CLI.
- Do not re-add a capability that is already covered by a generic
  `airflowctl` command; extend the generic command instead.
- **Porting a legacy command to `airflowctl` is the work this ADR asks for.** A PR
  that adds the `airflowctl` equivalent of an existing `airflow` command — with or
  without the matching `@deprecated_for_airflowctl(...)` marking in the same diff —
  is executing the migration, not duplicating surface. ADR 3's redundancy test
  ("the behaviour already exists on `main`") does not apply to it: the legacy
  implementation existing is the reason the port is needed.

## Consequences

The legacy CLI stops accumulating new surface, so the AIP-94 migration converges
instead of chasing a moving target. Contributors are steered to add
capabilities once, in the shared `ctl/commands` layer, where both the remote
client and any transitional local path pick them up. This ADR is
transitional and should be revisited once the migration to `airflowctl`
completes.

A change **violates** this decision when it:

- adds a subcommand to the legacy local CLI, or a flag exposing capability that
  the REST API can express, for something that could live on `airflowctl` — a
  flag on a local-only command (`scheduler`, `standalone`, `db`, `dags test`,
  `tasks run`) is not this;
- adds a **new** remote-capable command handler that opens a DB session or queries
  an ORM model / manager directly instead of going through the client/API;
- copy-pastes a command body into the local CLI instead of calling the shared
  `ctl/commands/…` implementation;
- re-introduces a command whose function a generic `airflowctl` command already
  provides.

## Evidence

- #65519 — new `team_name` support added to legacy CLI pool creation; closed rather than extending the legacy CLI.
- #66223 — legacy-CLI JSON-output cleanup that overlapped work better done in the migrated command path; closed.
- #68650 — `dags pause`/`unpause` marked as migrated to airflowctl, moving the behavior to the shared command layer.
- #58584 — proposed new `airflow db would-migrate` legacy-CLI command; closed rather than growing the legacy surface.
- #62055 (`--only-idle` on `airflow scheduler`), #56663 (`db clean` include/exclude
  arguments), #62234 (`--use-migration-files`), #65575 (MySQL SSL parameters for
  `db shell`) — all merged in 2026: the counter-cases that bound this decision.
  Each adds a flag to a local-only command that airflowctl, as a remote REST
  client, cannot host.
