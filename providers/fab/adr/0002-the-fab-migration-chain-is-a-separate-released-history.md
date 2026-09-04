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

# 2. The FAB migration chain is a separate, released history

Date: 2026-07-20

## Status

Accepted

## Context

Almost no provider owns database schema; this one does. `FABDBManager`
(`auth_manager/models/db.py`) registers an alembic environment rooted at
`migrations/` with its own `alembic.ini`, `versions/`, and — critically — its own
version table `alembic_version_fab`. It creates and evolves the `ab_user`,
`ab_role`, `ab_permission`, `ab_view_menu` and `ab_permission_view` tables the
authorization implementation reads on every request. Being *separate* from
airflow-core's chain is the point: the two histories advance on unrelated cadences,
and `revision_heads_map` in `db.py` maps provider version to head so
`airflow db migrate` can target this chain independently.

Separateness changes nothing about immutability. The reasoning in the core
migrations ADR applies unchanged: **a revision shipped in a released provider has
already run on real databases and will never run again on them**, so editing its
`upgrade()` only changes future installs and diverges the two populations forever.
Corrections go **forward**, as a new revision on the current head; only unreleased
revisions on `main` are still malleable. Two things make this easier to break than
in core, so it is restated here: the release boundary is harder to see (providers
ship in waves from `main`, so "released?" is a question about provider version
history, not the branch name), and the starting states are unusually varied (the
`ab_*` tables may have been created by FAB itself, by a pre-3.0 airflow-core, or
not at all). That diversity has repeatedly produced backend-specific failures
(MySQL FK names, SQLite batch-alter naming, pre-existing indexes), and the instinct
to fix them by editing the offending revision is exactly the failure this decision
stops — so revisions must be written defensively: idempotent creation, explicit
naming conventions, no assumption of an empty schema. `.github/CODEOWNERS` assigns
the migrations subtree separately.

## Decision

The revisions under `migrations/versions/` form a released, append-only history
that is immutable once shipped, and that stays structurally separate from
airflow-core's chain.

- **Never edit the `upgrade()` or `downgrade()` of a revision that has shipped in
  a released provider version.** Correct forward with a new revision whose
  `down_revision` is the current head.
- **Unreleased revisions may still be edited, retargeted, or dropped** — that is
  the only window in which this history is soft. When in doubt about the release
  boundary, treat the revision as released.
- **Keep the chain linear and the identity intact**: one head, no reuse or
  repurposing of a released revision identifier, and `version_table_name` stays
  `alembic_version_fab`. Never fold a FAB revision into core's chain or vice
  versa.
- **Update `revision_heads_map` in `db.py`** when a new head ships, so the
  provider version to revision mapping stays authoritative.
- **Write revisions to tolerate the states that exist in the wild**: idempotent
  table and index creation, explicit naming conventions for constraints, and no
  assumption that the `ab_*` tables are absent or were created by any particular
  producer.
- **Keep the ORM models and the migration definitions in agreement** — column
  types and sizes, constraint and index names. A divergence is a defect even
  when both halves work in isolation, because `create_db_from_orm` and the
  migration path must converge on the same schema.
- **Exercise a schema change on more than SQLite** when it touches constraints,
  foreign keys, or batch-alter paths; the recurring failures here are
  backend-specific.

## Consequences

- Databases that ran a revision earlier or later reach the same schema state — the
  populations never diverge, exactly as for core.
- The provider releases schema changes on its own cadence without coordinating a
  head with airflow-core, at the cost of a second chain operators must be aware of.
- Corrections cost an extra revision, not a one-line edit — the price of a
  replayable history.
- Reviewers must establish the release boundary themselves and reject in-place
  edits even when the original revision is plainly wrong.

A change **violates** this decision when it:

- edits the `upgrade()` or `downgrade()` of a revision already shipped in a
  released provider version, instead of layering a corrective revision on the
  current head;
- reuses or repurposes a released revision identifier for different DDL, or
  branches the chain so it has more than one head;
- merges this chain into airflow-core's history, or changes
  `version_table_name` away from `alembic_version_fab`;
- adds a revision that assumes an empty database or a single backend, or that
  leaves the ORM definitions and the migration definitions describing different
  schemas.

A reviewer should reject any change that mutates released FAB migration history,
and should ask which backends and which starting states a new revision was
exercised against.

## Evidence

- #54227 — made this chain the authoritative producer of the `ab_*` tables, not a
  side effect of initdb.
- #62308 — auto-discover DB managers from `provider.yaml`, making this chain a
  first-class part of `airflow db migrate`.
- #56100, #56328 — `if_not_exists` on table/index creation: forward corrections for
  databases that already had the objects, not rewrites of shipped revisions.
- #65540 — SQLite batch-alter naming defect in a released revision, fixed by
  controlling naming rather than editing history.
- #65831, #60869 — ORM-versus-migration divergence (MySQL FK name, column sizes),
  corrected by realigning the two definitions.
- #66883 — varied starting-state problem handled in the manager, not by retconning
  revisions.
- #59205 — empty-database starting state made to work through the migration path.
