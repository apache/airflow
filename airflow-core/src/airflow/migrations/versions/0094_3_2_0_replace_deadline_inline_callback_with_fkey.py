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
Replace deadline's inline callback fields with foreign key to callback table.

Revision ID: e812941398f4
Revises: b87d2135fa50
Create Date: 2025-10-24 00:34:57.111239

"""

from __future__ import annotations

from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op

# revision identifiers, used by Alembic.
revision = "e812941398f4"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"

# Hardcoded constants to avoid importing runtime modules in migrations.
# Prior to 3.2.0, only AsyncCallback (triggerer) with import_path was supported.
_CALLBACK_TYPE_TRIGGERER = "triggerer"
_CALLBACK_FETCH_METHOD_IMPORT_PATH = "import_path"
_CALLBACK_STATE_PENDING = "pending"
_CALLBACK_METRICS_PREFIX = "deadline_alerts"


def upgrade():
    """Replace Deadline table's inline callback fields with callback_id foreign key."""

    def migrate_all_data():
        if context.is_offline_mode():
            print(
                dedent("""
                ------------
                --  WARNING: Unable to migrate the data in the deadline table while in offline mode!
                --  All the rows in the deadline table will be deleted in this mode.
                ------------
                """)
            )
            op.execute("DELETE FROM deadline")
            return

        conn = op.get_bind()
        dialect = conn.dialect.name

        if dialect == "postgresql":
            # PostgreSQL: use gen_random_uuid() and jsonb operations to avoid Python
            # deserialization. The callback JSON is serde-wrapped:
            #   {"__data__": {"path": "...", "kwargs": {...}}, "__classname__": "...", ...}
            # We extract __data__ fields and merge in prefix + dag_id.
            # A writable CTE handles both the INSERT into callback and the UPDATE of
            # deadline in a single statement, so the generated UUID is shared.
            conn.execute(
                sa.text("""
                    WITH new_callbacks AS (
                        SELECT
                            d.id AS deadline_id,
                            gen_random_uuid() AS callback_id,
                            jsonb_build_object(
                                'path', d.callback->'__data__'->>'path',
                                'kwargs', d.callback->'__data__'->'kwargs',
                                'prefix', :prefix,
                                'dag_id', dr.dag_id
                            )::json AS callback_data,
                            CASE
                                WHEN d.callback_state IN ('failed', 'success') THEN d.callback_state
                                ELSE :pending
                            END AS cb_state,
                            CASE
                                WHEN d.callback_state IN ('failed', 'success') THEN true
                                ELSE false
                            END AS is_missed
                        FROM deadline d
                        JOIN dag_run dr ON d.dagrun_id = dr.id
                        WHERE d.callback_id IS NULL
                    ),
                    inserted AS (
                        INSERT INTO callback (id, type, fetch_method, data, state, priority_weight, created_at)
                        SELECT
                            callback_id, :cb_type, :fetch_method, callback_data, cb_state, 1, NOW()
                        FROM new_callbacks
                    )
                    UPDATE deadline
                    SET callback_id = nc.callback_id, missed = nc.is_missed
                    FROM new_callbacks nc
                    WHERE deadline.id = nc.deadline_id
                """),
                {
                    "cb_type": _CALLBACK_TYPE_TRIGGERER,
                    "fetch_method": _CALLBACK_FETCH_METHOD_IMPORT_PATH,
                    "prefix": _CALLBACK_METRICS_PREFIX,
                    "pending": _CALLBACK_STATE_PENDING,
                },
            )
        else:
            # MySQL / SQLite: use batched Python approach with SQL JSON extraction.
            # UUID generation requires Python (no reliable cross-dialect SQL UUID function
            # that matches SQLAlchemy's Uuid column type).
            _migrate_batched(conn, dialect)

    def _migrate_batched(conn, dialect):
        """Batch migration for MySQL/SQLite using Python UUIDs with SQL JSON extraction."""
        import json

        import uuid6

        from airflow.utils.sqlalchemy import UtcDateTime

        deadline_table = sa.table(
            "deadline",
            sa.column("id", sa.Uuid()),
            sa.column("dagrun_id", sa.Integer()),
            sa.column("callback", sa.JSON()),
            sa.column("callback_state", sa.String(20)),
            sa.column("missed", sa.Boolean()),
            sa.column("callback_id", sa.Uuid()),
        )

        dag_run_table = sa.table(
            "dag_run",
            sa.column("id", sa.Integer()),
            sa.column("dag_id", sa.String()),
        )

        callback_table = sa.table(
            "callback",
            sa.column("id", sa.Uuid()),
            sa.column("type", sa.String(20)),
            sa.column("fetch_method", sa.String(20)),
            sa.column("data", sa.JSON()),
            sa.column("state", sa.String(10)),
            sa.column("priority_weight", sa.Integer()),
            sa.column("created_at", UtcDateTime(timezone=True)),
        )

        from datetime import datetime, timezone

        timestamp = datetime.now(timezone.utc)
        batch_size = 1000
        batch_num = 0

        while True:
            batch_num += 1
            batch = conn.execute(
                sa.select(
                    deadline_table.c.id,
                    deadline_table.c.callback,
                    deadline_table.c.callback_state,
                    dag_run_table.c.dag_id,
                )
                .join(dag_run_table, deadline_table.c.dagrun_id == dag_run_table.c.id)
                .where(deadline_table.c.callback_id.is_(None))
                .limit(batch_size)
            ).fetchall()

            if not batch:
                break

            callback_inserts = []
            deadline_updates = []

            for row in batch:
                callback_id = uuid6.uuid7()
                cb = row.callback if isinstance(row.callback, dict) else json.loads(row.callback)
                cb_data = cb.get("__data__", {})
                callback_data = {
                    "path": cb_data.get("path", ""),
                    "kwargs": cb_data.get("kwargs", {}),
                    "prefix": _CALLBACK_METRICS_PREFIX,
                    "dag_id": row.dag_id,
                }

                is_terminal = row.callback_state in ("failed", "success")
                callback_inserts.append(
                    {
                        "id": callback_id,
                        "type": _CALLBACK_TYPE_TRIGGERER,
                        "fetch_method": _CALLBACK_FETCH_METHOD_IMPORT_PATH,
                        "data": callback_data,
                        "state": row.callback_state if is_terminal else _CALLBACK_STATE_PENDING,
                        "priority_weight": 1,
                        "created_at": timestamp,
                    }
                )
                deadline_updates.append(
                    {"deadline_id": row.id, "callback_id": callback_id, "missed": is_terminal}
                )

            conn.execute(callback_table.insert(), callback_inserts)
            conn.execute(
                deadline_table.update()
                .where(deadline_table.c.id == sa.bindparam("deadline_id"))
                .values(callback_id=sa.bindparam("callback_id"), missed=sa.bindparam("missed")),
                deadline_updates,
            )
            print(f"Migrated {len(batch)} deadline records in batch {batch_num}")

    # Add new columns (temporarily nullable until data has been migrated)
    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("missed", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("callback_id", sa.Uuid(), nullable=True))

    migrate_all_data()

    with op.batch_alter_table("deadline") as batch_op:
        # Data for `missed` and `callback_id` has been migrated so make them non-nullable
        batch_op.alter_column("missed", existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column("callback_id", existing_type=sa.Uuid(), nullable=False)

        batch_op.create_index("deadline_missed_deadline_time_idx", ["missed", "deadline_time"], unique=False)
        batch_op.create_index("deadline_callback_id_idx", ["callback_id"], unique=False)
        batch_op.drop_index(batch_op.f("deadline_callback_state_time_idx"))
        batch_op.create_foreign_key(
            batch_op.f("deadline_callback_id_fkey"), "callback", ["callback_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.drop_constraint(batch_op.f("deadline_trigger_id_fkey"), type_="foreignkey")
        batch_op.drop_column("callback")
        batch_op.drop_column("trigger_id")
        batch_op.drop_column("callback_state")


def downgrade():
    """Restore Deadline table's inline callback fields from callback_id foreign key."""

    _CALLBACK_STATE_SUCCESS = "success"
    _CALLBACK_STATE_FAILED = "failed"

    def migrate_batch(conn, deadline_table, callback_table, batch):
        deadline_updates = []
        callback_ids_to_delete = []

        for row in batch:
            try:
                filtered_cb_data = {k: row.callback_data[k] for k in ("path", "kwargs")}

                # Hard-coding the serialization to avoid SDK import.
                # Since only AsyncCallback was supported in the previous versions, this is equivalent to:
                # from airflow.serialization.serde import serialize
                # from airflow.sdk.definitions.deadline import AsyncCallback
                # callback_serialized = serialize(AsyncCallback.deserialize(filtered_data, 0))
                callback_serialized = {
                    "__data__": filtered_cb_data,
                    "__classname__": "airflow.sdk.definitions.deadline.AsyncCallback",
                    "__version__": 0,
                }

                # Mark the deadline as not handled if its callback is not in a terminal state so that the
                # scheduler handles it appropriately
                if row.callback_state in {_CALLBACK_STATE_SUCCESS, _CALLBACK_STATE_FAILED}:
                    callback_state = row.callback_state
                else:
                    callback_state = None

                deadline_updates.append(
                    {
                        "deadline_id": row.deadline_id,
                        "callback": callback_serialized,
                        "callback_state": callback_state,
                        "trigger_id": None,
                        "callback_id": None,
                    }
                )

                callback_ids_to_delete.append(row.callback_id)

            except Exception:
                print(f"Failed to migrate row: {row}")
                raise

        conn.execute(
            deadline_table.update()
            .where(deadline_table.c.id == sa.bindparam("deadline_id"))
            .values(
                callback=sa.bindparam("callback"),
                callback_state=sa.bindparam("callback_state"),
                trigger_id=sa.bindparam("trigger_id"),
                callback_id=sa.bindparam("callback_id"),
            ),
            deadline_updates,
        )
        conn.execute(callback_table.delete().where(callback_table.c.id.in_(callback_ids_to_delete)))

    def migrate_all_data():
        if context.is_offline_mode():
            print(
                dedent("""
                ------------
                --  WARNING: Unable to migrate the data in the
                --  deadline and callback tables while in offline mode!
                --  All the rows in the deadline table and the referenced rows in
                --  the callback table will be deleted in this mode.
                ------------
                """)
            )
            op.execute("DELETE FROM deadline")
            return

        from airflow.utils.sqlalchemy import ExtendedJSON

        deadline_table = sa.table(
            "deadline",
            sa.column("id", sa.Uuid()),
            sa.column("callback_id", sa.Uuid()),
            sa.column("callback", sa.JSON()),
            sa.column("callback_state", sa.String(20)),
            sa.column("trigger_id", sa.Integer()),
        )

        callback_table = sa.table(
            "callback",
            sa.column("id", sa.Uuid()),
            sa.column("data", ExtendedJSON()),
            sa.column("state", sa.String(10)),
        )

        conn = op.get_bind()
        batch_num = 0

        while True:
            batch_num += 1
            batch = conn.execute(
                sa.select(
                    deadline_table.c.id.label("deadline_id"),
                    deadline_table.c.callback_id,
                    callback_table.c.data.label("callback_data"),
                    callback_table.c.state.label("callback_state"),
                )
                .join(callback_table, deadline_table.c.callback_id == callback_table.c.id)
                .where(deadline_table.c.callback.is_(None))  # Only get rows that haven't been downgraded yet
                .limit(1000)
            ).fetchall()

            if not batch:
                break

            migrate_batch(conn, deadline_table, callback_table, batch)
            print(f"Migrated {len(batch)} deadline records in batch {batch_num}")

    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("callback_state", sa.VARCHAR(length=20), nullable=True))
        batch_op.add_column(sa.Column("trigger_id", sa.INTEGER(), autoincrement=False, nullable=True))

        # Temporarily nullable until data has been migrated
        batch_op.add_column(sa.Column("callback", sa.JSON(), nullable=True))

        # Make callback_id nullable so the associated callbacks can be cleared during migration
        batch_op.alter_column("callback_id", existing_type=sa.Uuid(), nullable=True)

        batch_op.drop_constraint(batch_op.f("deadline_callback_id_fkey"), type_="foreignkey")
        # Note: deadline_callback_id_idx is kept here so it can speed up the JOIN in migrate_all_data()

    migrate_all_data()

    with op.batch_alter_table("deadline") as batch_op:
        # Data for `callback` has been migrated so make it non-nullable
        batch_op.alter_column("callback", existing_type=sa.JSON(), nullable=False)
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_index("deadline_missed_deadline_time_idx")
        batch_op.create_index(
            batch_op.f("deadline_callback_state_time_idx"), ["callback_state", "deadline_time"], unique=False
        )
        # Drop the index after data migration so it can speed up the JOIN query in migrate_all_data()
        batch_op.drop_index("deadline_callback_id_idx")
        batch_op.drop_column("callback_id")
        batch_op.drop_column("missed")
