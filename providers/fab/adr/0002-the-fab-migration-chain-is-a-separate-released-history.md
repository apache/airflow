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

Almost no provider owns database schema. This one does. `FABDBManager`
(`auth_manager/models/db.py`) registers an alembic environment rooted at
`migrations/`, with its own `alembic.ini`, its own `versions/` directory, and —
critically — its own version table, `alembic_version_fab`. It creates and evolves
the `ab_user`, `ab_role`, `ab_permission`, `ab_view_menu` and
`ab_permission_view` tables that the authorization implementation reads on every
request.

Being *separate* from airflow-core's chain is the point. The two histories
advance on unrelated cadences: core's with Airflow releases, this one with
provider releases. A shared version table would make either release train unable
to move without the other. `revision_heads_map` in `db.py` records which provider
version corresponds to which head, so `airflow db migrate` can target this chain
independently.

But separateness changes nothing about immutability. The reasoning in the core
migrations ADR applies here without modification: **a revision that has shipped
in a released provider has already run on real databases and will never run again
on them.** Editing its `upgrade()` afterwards does not retroactively alter the
databases that applied it; it only changes what future installations do, so the
two populations diverge permanently and no schema state is consistent for both.
Corrections go **forward**, as a new revision on top of the current head. Only
revisions that exist on `main` and have not gone out in any provider release are
still malleable.

Two things make the rule easier to break here than in core, which is why it is
restated as this area's decision rather than left as an inherited assumption.

First, **the release boundary is harder to see**. Providers ship in waves from
`main`, so "has this revision been released?" is a question about the provider's
own version history, not about the Airflow version in the branch name. A
contributor reading `migrations/versions/` sees two files and no obvious marker
of which have shipped.

Second, **the starting states are unusually varied**. A deployment can reach this
chain from a database where FAB itself created the `ab_*` tables long ago, from
one where a pre-3.0 airflow-core created them, or from an empty database. That
diversity has repeatedly produced backend-specific failures — foreign-key names
generated differently by MySQL, batch-alter naming conventions on SQLite, indexes
that already exist. The instinct when such a report arrives is to open the
offending revision and correct it. That instinct is the failure this decision
exists to stop, and it also means the migrations must be written defensively
enough that they do not *need* correcting: idempotent creation, explicit naming
conventions, and no assumption of an empty schema.

Ownership reflects the schema-owner status: `.github/CODEOWNERS` assigns the
migrations subtree separately from the rest of the provider.

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

- Databases that already ran a revision and databases that run it later reach the
  same schema state — the populations never diverge, exactly as for core.
- The provider can release schema changes on its own cadence without coordinating
  a head with airflow-core, at the cost of a second chain that operators must be
  aware of when reasoning about `airflow db migrate`.
- Corrections cost an extra revision rather than a one-line edit. That is
  intentional and is the price of a replayable history.
- Reviewers must establish the release boundary themselves — it is not visible in
  the diff — and must reject in-place edits even when the original revision is
  plainly wrong.

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

- #54227 — "Create FAB's user/role tables on migration, not only on initdb":
  established this chain as the authoritative producer of the `ab_*` tables
  rather than a side effect of initialisation.
- #62308 — "Auto-discover DB managers from provider.yaml": made the provider's DB
  manager, and therefore its chain, a first-class part of `airflow db migrate`.
- #56100 — "Add `if_not_exists=True` to FAB migration" and #56328 — "Add
  `if_not_exists` to index creation in migrations": forward corrections for
  databases that already had the objects, rather than rewrites of the shipped
  revisions.
- #65540 — "use `naming_convention` for `02ca36b0235b` batch_alter_table on
  SQLite": a backend-specific defect in a released revision, fixed by controlling
  naming rather than by editing history.
- #65831 — "Fix fab mysql migration error caused by pre defined fk name in orm
  create": the ORM-versus-migration divergence failing on one backend only.
- #60869 — "Align ORM column sizes with migration definitions": the same class of
  divergence, corrected by bringing the two definitions back together.
- #66883 — "Fix provider DB upgrades with existing tables": the varied
  starting-state problem, handled in the manager rather than by retconning
  revisions.
- #59205 — "Permit `airflow db migrate -r` with an empty database": the
  empty-database starting state made to work through the migration path.
