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
Remove duplicate ``(state, dag_id)`` indexes on ``dag_run``.

MySQL has no partial indexes, so ``idx_dag_run_queued_dags`` and ``idx_dag_run_running_dags``
are both plain indexes on ``(state, dag_id)`` there. ``idx_dag_run_queued_dags`` is dropped;
``idx_dag_run_running_dags`` is kept because the scheduler names it in a MySQL ``USE INDEX`` hint.

On Postgres the squashed 2.6.2 migration created both without their ``WHERE`` predicate, so
databases built from migration files (``--use-migration-files``) have the same duplicate.
Those are recreated as the partial indexes the ORM declares; databases that already have the
partial indexes are left untouched.

Revision ID: 9f8d3473abf9
Revises: e7c2a91bd540
Create Date: 2026-09-28 12:10:05.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

revision = "9f8d3473abf9"
down_revision = "e7c2a91bd540"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_TABLE = "dag_run"
_INDEX = "idx_dag_run_queued_dags"
_EXISTS_QUERY = (
    "SELECT COUNT(*) FROM information_schema.STATISTICS "
    f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{_TABLE}' AND INDEX_NAME = '{_INDEX}'"
)
_POSTGRES_PARTIAL_INDEXES = {
    "idx_dag_run_queued_dags": "queued",
    "idx_dag_run_running_dags": "running",
}


def _make_postgres_index_partial(conn, index: str, state: str) -> None:
    """
    Recreate ``index`` with its ``WHERE state = <state>`` predicate if it currently has none.

    A ``DO`` block keeps the check server-side, so the same SQL works in offline (``--sql``) mode.
    """
    conn.execute(
        sa.text(f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_index WHERE indexrelid = to_regclass('{index}') AND indpred IS NULL
                ) THEN
                    DROP INDEX {index};
                    CREATE INDEX {index} ON {_TABLE} (state, dag_id) WHERE state = '{state}';
                END IF;
            END $$;
            """)
    )


def _run_if_index_exists(conn, *, exists: bool, statement: str) -> None:
    """
    Run ``statement`` only when the index's presence matches ``exists``.

    MySQL has no ``DROP INDEX IF EXISTS`` / ``CREATE INDEX IF NOT EXISTS``, and a user may
    already have dropped the duplicate by hand. In offline (``--sql``) mode there is no live
    connection to check against, so the guard is emitted as a prepared statement instead.
    """
    if context.is_offline_mode():
        conn.execute(
            sa.text(f"""
                set @var=if(({_EXISTS_QUERY}) {"> 0" if exists else "= 0"},'{statement}','select 1');

                prepare stmt from @var;
                execute stmt;
                deallocate prepare stmt;
                """)
        )
        return
    if bool(conn.execute(sa.text(_EXISTS_QUERY)).scalar()) is exists:
        conn.execute(sa.text(statement))


def upgrade():
    """Remove duplicate ``(state, dag_id)`` indexes on ``dag_run``."""
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        _run_if_index_exists(conn, exists=True, statement=f"DROP INDEX {_INDEX} ON {_TABLE}")
    elif conn.dialect.name == "postgresql":
        for index, state in _POSTGRES_PARTIAL_INDEXES.items():
            _make_postgres_index_partial(conn, index, state)


def downgrade():
    """Recreate ``idx_dag_run_queued_dags`` on MySQL; Postgres keeps the partial indexes the ORM declares."""
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        _run_if_index_exists(
            conn, exists=False, statement=f"CREATE INDEX {_INDEX} ON {_TABLE} (state, dag_id)"
        )
