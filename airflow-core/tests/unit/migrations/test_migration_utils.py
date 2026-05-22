#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Stairway test for Airflow DB migrations.

Walks every migration step since the 3.0.0 release forward (upgrade) then
backward (downgrade) to verify that all migrations are fully reversible.
This catches problems like:
- a migration that creates a constraint the downgrade forgets to drop
- a downgrade that references a column that was never added by the upgrade

The test runs as a single (non-parametrized) function so that execution order
and DB state are guaranteed without relying on xdist grouping or cross-test
state.  Failures include the specific revision ID so the broken migration is
immediately identifiable in CI logs.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import partial

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import MissingGreenlet, OperationalError

from airflow import settings
from airflow.migrations.utils import (
    disable_sqlite_fkeys,
    ignore_sqlite_value_error,
    mysql_drop_foreignkey_if_exists,
)
from airflow.utils.db import (
    _REVISION_HEADS_MAP,
    _get_alembic_config,
    downgrade,
    upgradedb,
)

# The stairway runs hundreds of upgrade/downgrade cycles. Each cycle goes
# through ``_single_connection_pool`` which calls ``settings.reconfigure_orm()``
# at entry and exit, and ``dispose_orm()`` cannot synchronously close asyncpg
# connections (it raises ``MissingGreenlet`` which is caught and logged).
# Across the full stairway that leaks enough asyncpg connections to exhaust
# Postgres ``max_connections``. On slower backends the total wall time also
# exceeds the default 60s per-test timeout from ``--test-timeout``.
_STAIRWAY_TIMEOUT_SECONDS = 900

pytestmark = pytest.mark.db_test

# Stairway starts from the 3.0.0 head revision.  Starting here (rather than
# the 2.6.2 squashed baseline) avoids triggering FAB-provider downgrade
# handling that airflow.utils.db.downgrade() applies when the target is below
# 2.10.3, and keeps the number of steps manageable on slower backends.
_STAIRWAY_START_REVISION = _REVISION_HEADS_MAP["3.0.0"]

# CI shows two transient failure modes for this test:
#   1. PostgreSQL "too many clients already" when the metadata DB pool is
#      saturated by other parallel tests at the moment a step runs. This
#      surfaces as ``sqlalchemy.exc.OperationalError``.
#   2. SQLAlchemy ``MissingGreenlet`` during async-engine disposal between
#      steps, which can leak into the next migration call.
# Both are environmental; rebuilding the ORM (dispose + reconfigure with
# NullPool) between attempts clears the leaked connections and restores
# ``settings.Session`` so the next ``@provide_session``-wrapped call has a
# working session.
_STEP_RETRIES = 3
_STEP_RETRY_BACKOFF_SECONDS = 2.0
_TRANSIENT_STEP_ERRORS: tuple[type[BaseException], ...] = (OperationalError, MissingGreenlet)


def _run_step_with_retry(step: Callable[[], None], *, revision_id: str, label: str) -> None:
    """
    Run a single migration step, retrying on known transient backend errors.

    Only retries on :class:`sqlalchemy.exc.OperationalError` (e.g. PostgreSQL
    ``too many clients already``) and :class:`sqlalchemy.exc.MissingGreenlet`
    (async-engine disposal leaking into the next call) so genuine migration
    regressions still surface immediately.

    Rebuilds the ORM between attempts: a plain ``dispose_orm`` would leave
    ``settings.Session`` set to ``None``, causing the next
    ``@provide_session``-wrapped call to raise
    ``RuntimeError("Session must be set before!")``. NullPool is reapplied so
    the fixture's connection-leak guard is preserved across retries.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, _STEP_RETRIES + 1):
        try:
            step()
            return
        except _TRANSIENT_STEP_ERRORS as exc:
            last_exc = exc
            if attempt == _STEP_RETRIES:
                break
            # Free pooled connections and rebuild Session/engine before retry.
            try:
                settings.dispose_orm(do_log=False)
                settings.configure_orm(disable_connection_pool=True)
            except Exception:
                pass
            time.sleep(_STEP_RETRY_BACKOFF_SECONDS * attempt)
    raise AssertionError(
        f"Stairway test failed at revision {revision_id!r} during {label} after {_STEP_RETRIES} attempts"
    ) from last_exc


def _get_revisions_in_order() -> list[str]:
    """Return revision IDs in upgrade order, starting after the stairway base."""
    config = _get_alembic_config()
    script = ScriptDirectory.from_config(config)

    # walk_revisions() yields from head → base; reverse for upgrade order.
    all_revisions = list(script.walk_revisions())
    all_revisions.reverse()

    revision_ids = [rev.revision for rev in all_revisions]

    if _STAIRWAY_START_REVISION not in revision_ids:
        raise ValueError(
            f"Stairway base revision {_STAIRWAY_START_REVISION!r} not found in migration scripts. "
            "Update _STAIRWAY_START_REVISION to a valid revision from _REVISION_HEADS_MAP."
        )

    base_index = revision_ids.index(_STAIRWAY_START_REVISION)
    return revision_ids[base_index + 1 :]


@pytest.fixture(scope="module")
def stairway_db():
    """
    Bring the DB to the stairway base revision before the test runs, and
    restore it to the current heads afterwards so other tests are unaffected.

    Uses Airflow's downgrade()/upgradedb() wrappers so that backend-specific
    handling (MySQL metadata-lock, global migration lock, single-connection
    pool) is applied consistently.

    Disables the async ORM session for the duration of the test: each
    upgrade/downgrade cycle reconfigures the ORM, and dispose_orm() cannot
    synchronously close asyncpg connections, so they would otherwise leak
    until Postgres' max_connections is exhausted.
    """
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    # Use NullPool: every connection is closed immediately on return to the
    # pool. Each upgrade/downgrade leaks ~one connection from various places
    # (alembic's env.py, _configured_alembic_environment, external DB manager
    # post-migration hooks); over hundreds of revisions a normal pool fills up
    # and exhausts Postgres' max_connections.
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=_STAIRWAY_START_REVISION)

        yield

        # Restore to latest heads even if the test failed mid-way.
        upgradedb()
    finally:
        settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
        settings.reconfigure_orm()


@pytest.mark.execution_timeout(_STAIRWAY_TIMEOUT_SECONDS)
def test_migration_stairway(stairway_db) -> None:
    """
    Walk every incremental migration step since 3.0.0: upgrade → downgrade → re-upgrade.

    Uses Airflow's upgradedb()/downgrade() wrappers for every step so that
    the global migration lock, MySQL metadata-lock handling, and
    single-connection pool are active — matching real-world upgrade/downgrade
    behaviour and preventing backend-specific hangs.

    The relative ``-1`` specifier is used for downgrade so that merge revisions
    are handled correctly: it always rolls back exactly one step from the
    current head regardless of how many parents a merge revision has.
    """
    revisions = _get_revisions_in_order()

    for revision_id in revisions:
        try:
            _run_step_with_retry(
                partial(upgradedb, to_revision=revision_id),
                revision_id=revision_id,
                label="upgrade",
            )
            _run_step_with_retry(
                partial(downgrade, to_revision="-1"),
                revision_id=revision_id,
                label="downgrade",
            )
            _run_step_with_retry(
                partial(upgradedb, to_revision=revision_id),
                revision_id=revision_id,
                label="re-upgrade",
            )
        finally:
            # upgradedb()/downgrade() each enter ``_single_connection_pool``,
            # which calls ``reconfigure_orm()`` and resets the pool to default.
            # Re-apply NullPool so the iteration cap is preserved.
            settings.dispose_orm(do_log=False)
            settings.configure_orm(disable_connection_pool=True)


class TestDisableSqliteFkeys:
    """Tests for :func:`disable_sqlite_fkeys`."""

    def test_yields_op(self):
        """The context manager yields the *op* object on every backend."""
        with settings.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            op = Operations(ctx)
            with disable_sqlite_fkeys(op) as yielded:
                assert yielded is op

    @pytest.mark.backend("sqlite")
    def test_sqlite_turns_off_fkeys(self):
        """On SQLite, ``PRAGMA foreign_keys`` is OFF inside the context."""

        with settings.engine.connect() as conn:
            # Ensure foreign_keys is ON before entering the context manager.
            conn.execute(text("PRAGMA foreign_keys=ON"))
            conn.commit()

            ctx = MigrationContext.configure(conn)
            op = Operations(ctx)

            with disable_sqlite_fkeys(op):
                result = conn.execute(text("PRAGMA foreign_keys")).scalar()
                assert result == 0, "foreign_keys should be OFF inside disable_sqlite_fkeys"

            result = conn.execute(text("PRAGMA foreign_keys")).scalar()
            assert result == 1, "foreign_keys should be restored to ON after disable_sqlite_fkeys"

    @pytest.mark.backend("postgres", "mysql")
    def test_non_sqlite_is_noop(self):
        """On non-SQLite backends, the context manager simply yields."""

        with settings.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            op = Operations(ctx)
            with disable_sqlite_fkeys(op) as yielded:
                assert yielded is op


class TestMysqlDropForeignkeyIfExists:
    """Tests for :func:`mysql_drop_foreignkey_if_exists`."""

    @pytest.mark.backend("mysql")
    def test_drops_existing_fk(self):
        """On MySQL, an existing foreign key is dropped."""

        with settings.engine.connect() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS _test_mig_parent (id INT PRIMARY KEY)"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS _test_mig_child (
                      id INT PRIMARY KEY,
                      parent_id INT,
                      CONSTRAINT _test_mig_fk FOREIGN KEY (parent_id)
                        REFERENCES _test_mig_parent(id)
                    )
                    """
                )
            )
            conn.commit()

            try:
                ctx = MigrationContext.configure(conn)
                op = Operations(ctx)
                mysql_drop_foreignkey_if_exists("_test_mig_fk", "_test_mig_child", op)
                conn.commit()

                count = conn.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
                        WHERE CONSTRAINT_SCHEMA = DATABASE()
                          AND TABLE_NAME = '_test_mig_child'
                          AND CONSTRAINT_NAME = '_test_mig_fk'
                          AND CONSTRAINT_TYPE = 'FOREIGN KEY'
                        """
                    )
                ).scalar()
                assert count == 0, "Foreign key should have been dropped"
            finally:
                conn.execute(text("DROP TABLE IF EXISTS _test_mig_child"))
                conn.execute(text("DROP TABLE IF EXISTS _test_mig_parent"))
                conn.commit()

    @pytest.mark.backend("mysql")
    def test_noop_for_nonexistent_fk(self):
        """On MySQL, dropping a non-existent FK does not raise."""

        with settings.engine.connect() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS _test_mig_nofk (id INT PRIMARY KEY)"))
            conn.commit()

            try:
                ctx = MigrationContext.configure(conn)
                op = Operations(ctx)
                mysql_drop_foreignkey_if_exists("nonexistent_fk", "_test_mig_nofk", op)
                conn.commit()
            finally:
                conn.execute(text("DROP TABLE IF EXISTS _test_mig_nofk"))
                conn.commit()


class TestIgnoreSqliteValueError:
    """Tests for :func:`ignore_sqlite_value_error`."""

    @pytest.mark.backend("sqlite")
    def test_sqlite_suppresses_value_error(self):
        """On SQLite, ``ValueError`` is suppressed."""

        with settings.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx), ignore_sqlite_value_error():
                raise ValueError("should be suppressed on SQLite")

    @pytest.mark.backend("postgres", "mysql")
    def test_non_sqlite_does_not_suppress(self):
        """On non-SQLite backends, ``ValueError`` propagates."""

        with settings.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx), pytest.raises(ValueError, match="should propagate"):
                with ignore_sqlite_value_error():
                    raise ValueError("should propagate")

    def test_does_not_suppress_other_exceptions(self):
        """Even on SQLite, only ``ValueError`` is suppressed — other exceptions propagate."""
        with settings.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            with Operations.context(ctx), pytest.raises(TypeError):
                with ignore_sqlite_value_error():
                    raise TypeError("not a ValueError")
