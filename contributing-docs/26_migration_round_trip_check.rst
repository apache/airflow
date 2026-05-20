 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Migration round-trip regression check
=====================================

This page documents the manual prek hook that verifies SQLite
migrations round-trip safely from base to head and back, plus the
``disable_sqlite_fkeys(op)`` placement convention every migration
must follow.

If you are adding or editing a migration, the convention section is
the part that affects you. The rest is operational detail for people
maintaining the check itself.

Files:

- ``scripts/in_container/run_migration_round_trip.py`` — the test driver.
- ``scripts/ci/prek/migration_round_trip.py`` — the prek hook wrapper that
  drives the in-container script via Breeze.

Running the check
-----------------

The hook is registered under ``stages: [manual]`` in
``airflow-core/.pre-commit-config.yaml`` so it does not fire on every
local commit (a full base→head→base walk takes a few minutes). To run
it manually:

.. code-block:: bash

    prek run migration-round-trip --hook-stage manual --all-files

CI runs it automatically on any pull request whose changed files
include ``airflow-core/src/airflow/migrations/`` — the dedicated
``Migration round-trip check`` job in ``.github/workflows/ci-amd-arm.yml``
is gated on the ``has-migrations`` selective-checks output.


Convention: ``disable_sqlite_fkeys`` placement
----------------------------------------------

Migrations that rebuild a parent table via ``op.batch_alter_table(...)``
(any table whose children declare ``ON DELETE CASCADE``) must wrap their
entire body in ``disable_sqlite_fkeys(op)``, **before any DML statement
or any rebuild-mode** ``op.batch_alter_table(...)`` runs.

The exact rule (verified empirically):

    ``PRAGMA foreign_keys = on/off`` only takes effect on a connection
    that is currently in SQLite's autocommit mode. Pure DDL statements
    (``ALTER TABLE``, ``CREATE TABLE``, ``DROP TABLE``) implicitly
    commit and leave the connection in autocommit, so a PRAGMA *after*
    them still works. DML statements (``INSERT``, ``UPDATE``,
    ``DELETE``) leave the connection in an open transaction, and any
    PRAGMA after a DML — including the one inside
    ``disable_sqlite_fkeys`` — is silently a no-op.

The trap: ``op.batch_alter_table`` is sometimes pure DDL (e.g.
``add_column`` only — alembic uses native ``ALTER TABLE`` on SQLite)
and sometimes a rebuild that internally does
``CREATE _alembic_tmp / INSERT INTO ... SELECT FROM / DROP / ALTER
RENAME``. The internal ``INSERT`` makes the connection no longer
autocommit, so a subsequent ``disable_sqlite_fkeys`` block is a no-op.

Because (a) it's hard for migration authors to predict at write time
whether a particular ``batch_alter_table`` will rebuild and (b) the
wrapper costs nothing to put earlier, the practical convention is
strictly stronger than the technical rule:

    Put ``with disable_sqlite_fkeys(op):`` as the **outermost** block
    of any ``upgrade`` / ``downgrade`` that calls
    ``op.batch_alter_table`` on a parent table, before *any* DML or
    DDL. Don't try to optimise it into a smaller scope.

Correct:

.. code-block:: python

    def upgrade():
        from airflow.migrations.utils import disable_sqlite_fkeys

        with disable_sqlite_fkeys(op):
            with op.batch_alter_table("dag", schema=None) as batch_op:
                batch_op.add_column(sa.Column("..."))

            op.execute("UPDATE dag SET ... WHERE ...")

            with op.batch_alter_table("dag", schema=None) as batch_op:
                batch_op.alter_column("...", nullable=False)

Anti-pattern:

.. code-block:: python

    def upgrade():
        # The first batch_alter_table here is pure add_column, so it's
        # native ALTER TABLE (DDL) and on its own would leave the
        # connection in autocommit. But the op.execute("UPDATE ...")
        # below it is DML — it leaves the connection in a non-
        # autocommit state, so the wrapper's PRAGMA is a no-op and the
        # alter_column rebuild runs with foreign_keys still ON.
        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.add_column(sa.Column("..."))

        op.execute("UPDATE dag SET ... WHERE ...")

        with disable_sqlite_fkeys(op):  # ← TOO LATE: PRAGMA is a no-op
            with op.batch_alter_table("dag", schema=None) as batch_op:
                batch_op.alter_column("...", nullable=False)

The same convention applies to ``downgrade()``.

What the check tests
--------------------

The driver:

1. Spins up a fresh SQLite database with ``PRAGMA foreign_keys = ON``.
2. Walks every migration in topological order, base → head, one
   revision at a time. Before each step it idempotently restores the
   seed fixture (``log_template``, ``dag_bundle``, ``dag``,
   ``dag_version``, ``dag_run``, ``task_instance``) to whatever the
   live schema can hold, so any FK chain a rebuild needs to fire
   (``dag → dag_version → task_instance.dag_version_id RESTRICT`` is
   the load-bearing one) is in place.
3. Walks every migration in reverse, head → base, the same way.

Any failure is reported with the failing direction (``up`` /
``down``) and the rev id, plus the underlying exception.

Why per-migration walking
-------------------------

A single-call ``airflow_db.upgradedb(to_revision=tip)`` walks all
migrations on a single shared SQLAlchemy connection. Once any earlier
migration's correctly-placed ``disable_sqlite_fkeys(op)`` flips
``foreign_keys`` to off, that state stays off on the connection — and
later migrations on the same connection silently inherit the protection.
A migration that *omits* ``disable_sqlite_fkeys`` or places it with
the wrong placement (where its own PRAGMA is a no-op) then looks fine
in single-call mode purely because of the inherited state.

Walking each migration through its own
``airflow_db.upgradedb(to_revision=specific_rev)`` call sidesteps that:
each call ``dispose_orm()`` + ``configure_orm()`` builds a fresh engine,
``setup_event_handlers`` fires ``PRAGMA foreign_keys=ON`` on the new
connection, and the migration starts from a clean slate.

Maintaining the seed
--------------------

``SEED_VALUES`` in ``run_migration_round_trip.py`` lists hardcoded
values for every column the seed touches across all revisions in the
chain. If a future migration adds a brand-new ``NOT NULL``-no-default
column to one of the seeded tables (``dag``, ``dag_version``,
``dag_run``, ``task_instance``, ``dag_bundle``, ``log_template``), the
round-trip aborts with a clear ``RuntimeError`` pointing at the table
that needs an addition. Add the new column + a sensible literal value
to ``SEED_VALUES['<table>']`` and re-run.
