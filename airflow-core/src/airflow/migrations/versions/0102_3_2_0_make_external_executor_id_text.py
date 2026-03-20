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
Make external_executor_id TEXT to allow for longer external_executor_ids.

Revision ID: a5a3e5eb9b8d
Revises: 55297ae24532
Create Date: 2026-01-28 16:35:00.000000

"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

from airflow.configuration import conf

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

# revision identifiers, used by Alembic.
revision = "a5a3e5eb9b8d"
down_revision = "55297ae24532"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"

_TABLE_KEY_COLUMNS = {
    "task_instance": "id",
    "task_instance_history": "task_instance_id",
}
_COLUMN = "external_executor_id"
_NEW_COL = "external_executor_id_new"
_BATCH_SIZE = conf.getint("database", "migration_batch_size", fallback=1000)


def upgrade():
    """
    Change external_executor_id column from VARCHAR(250) to TEXT.

    On PostgreSQL this is a metadata-only change (no table rewrite), the ACCESS EXCLUSIVE lock is held for only milliseconds.
    On MySQL, TEXT is also a superset of VARCHAR(250), so no rewrite is needed.
    """
    for table in _TABLE_KEY_COLUMNS:
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.alter_column(
                _COLUMN,
                existing_type=sa.VARCHAR(length=250),
                type_=sa.Text(),
                existing_nullable=True,
            )


def downgrade():
    """
    Revert external_executor_id column from TEXT to VARCHAR(250).

    A naive ALTER COLUMN ... TYPE VARCHAR(250) would acquire ACCESS EXCLUSIVE
    for the full duration of a table rewrite — minutes on large tables,
    blocking ALL reads and writes to task_instance / task_instance_history.

    Instead we:
    1. ADD COLUMN (instant, brief ACCESS EXCLUSIVE)
    2. Backfill in batches (ROW EXCLUSIVE only — concurrent reads/writes OK)
    3. DROP old + RENAME new (instant, brief ACCESS EXCLUSIVE)

    This keeps the exclusive lock window to milliseconds at each step.
    """
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "sqlite":
        for table in _TABLE_KEY_COLUMNS:
            _downgrade_sqlite(table)
    else:
        # Generic path works for both PostgreSQL and MySQL 8.0+
        for table in _TABLE_KEY_COLUMNS:
            _downgrade_expand_contract(conn, table)


def _downgrade_expand_contract(conn: Connection, table: str):
    """Expand-contract downgrade for PostgreSQL and MySQL."""
    engine = conn.engine
    key_column = _TABLE_KEY_COLUMNS[table]

    print(f"starting expand-contract downgrade for {table}.{_COLUMN}")

    # Phase 1: ADD the shadow column (instant, ~ms ACCESS EXCLUSIVE)
    with engine.begin() as ddl_conn:
        ddl_conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN {_NEW_COL} VARCHAR(250)"))

    # Phase 2: Backfill in batches using keyset pagination.
    # Keyset (WHERE id > last_seen) is O(batch_size) per batch vs
    # OFFSET which is O(total_rows). UPDATE uses a range on the PK
    # for a single index scan instead of N point lookups.
    total_updated = 0
    last_id = None

    while True:
        with engine.begin() as batch_conn:
            if last_id is None:
                rows = batch_conn.execute(
                    sa.text(
                        f"SELECT {key_column} FROM {table} "
                        f"WHERE {_COLUMN} IS NOT NULL "
                        f"ORDER BY {key_column} LIMIT :batch_size"
                    ),
                    {"batch_size": _BATCH_SIZE},
                ).fetchall()
            else:
                rows = batch_conn.execute(
                    sa.text(
                        f"SELECT {key_column} FROM {table} "
                        f"WHERE {_COLUMN} IS NOT NULL "
                        f"AND {key_column} > :last_id "
                        f"ORDER BY {key_column} "
                        f"LIMIT :batch_size"
                    ),
                    {"last_id": last_id, "batch_size": _BATCH_SIZE},
                ).fetchall()

            if not rows:
                break

            min_id = rows[0][0]
            max_id = rows[-1][0]
            last_id = max_id

            batch_conn.execute(
                sa.text(
                    f"UPDATE {table} SET {_NEW_COL} = "
                    f"CASE WHEN LENGTH({_COLUMN}) > 250 "
                    f"THEN LEFT({_COLUMN}, 250) "
                    f"ELSE {_COLUMN} END "
                    f"WHERE {key_column} >= :min_id AND {key_column} <= :max_id "
                    f"AND {_COLUMN} IS NOT NULL"
                ),
                {"min_id": min_id, "max_id": max_id},
            )

            total_updated += len(rows)
            print(f"backfilled {table}.{_NEW_COL}: {total_updated} rows so far")

    print(f"backfill complete for {table}.{_NEW_COL}: {total_updated} rows total")

    # Phase 3: Atomic swap — DROP old + RENAME new (instant, ~ms ACCESS EXCLUSIVE)
    with engine.begin() as swap_conn:
        swap_conn.execute(sa.text(f"ALTER TABLE {table} DROP COLUMN {_COLUMN}"))
        swap_conn.execute(sa.text(f"ALTER TABLE {table} RENAME COLUMN {_NEW_COL} TO {_COLUMN}"))


def _downgrade_sqlite(table: str):
    """SQLite uses Alembic's copy-and-swap, which is fine for small dev/test DBs."""
    with op.batch_alter_table(table) as batch_op:
        batch_op.alter_column(
            _COLUMN,
            existing_type=sa.Text(),
            type_=sa.VARCHAR(length=250),
            existing_nullable=True,
        )
