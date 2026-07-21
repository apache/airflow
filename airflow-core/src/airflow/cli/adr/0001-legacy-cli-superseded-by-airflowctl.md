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
through the public REST API. Under the post-3.0 architecture only the API server
touches the metadata database, so a local CLI reaching into `DagModel`, sessions,
or manager classes violates that boundary and cannot run against a remote
deployment. The local CLI is therefore legacy and deliberately shrinking: commands
are marked "migrated to airflowctl" one area at a time (e.g. `dags pause` /
`dags unpause`) with the implementation moving to a shared `ctl/commands/…` module.
Adding new capability to the legacy CLI, or re-adding what a generic `airflowctl`
command already covers, grows the surface still to be ported.

Two boundaries keep this from forbidding routine work. **Not everything can
move:** airflowctl is a remote REST client, so local-by-nature commands —
`scheduler`, `standalone`, `db`, and the local execution paths (`dags test`,
`tasks run`) — have no airflowctl home and are not waiting to be ported. This is
borne out by PRs #62055, #56663, #62234, #65575, and #62344, which all added
flags to such commands and merged in 2026. **Direct DB access from a local
handler is the existing pattern:**
`provide_session` / `create_session` appear at 44 sites under `cli/commands/`;
those *are* the local-administration path. What this ADR forbids is a *new
remote-capable* command reaching into the ORM, not the pre-existing
local-administration commands.

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

The legacy CLI stops accumulating surface, so the AIP-94 migration converges;
contributors add capability once, in the shared `ctl/commands` layer. This ADR is
transitional and should be revisited once the migration completes.

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

- #65519 — new `team_name` on legacy-CLI pool creation; closed rather than extending the legacy CLI.
- #66223 — legacy-CLI JSON-output cleanup better done in the migrated path; closed.
- #68650 — `dags pause`/`unpause` marked migrated to airflowctl, moved to the shared layer.
- #58584 — proposed new `airflow db would-migrate` legacy command; closed.
- #62055, #56663, #62234, #65575 — merged in 2026: flags on local-only commands airflowctl cannot host (the bounding counter-cases).
