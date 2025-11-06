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

from datetime import datetime, timezone
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import column, select, table
from sqlalchemy_jsonfield import JSONField
from sqlalchemy_utils import UUIDType

from airflow.serialization.serde import deserialize
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime

# revision identifiers, used by Alembic.
revision = "e812941398f4"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"

BATCH_SIZE = 1000


def upgrade():
    """Replace Deadline table's inline callback fields with callback_id foreign key."""
    import uuid6

    from airflow.models.base import StringID
    from airflow.models.callback import CallbackFetchMethod, CallbackState, CallbackType
    from airflow.models.deadline import CALLBACK_METRICS_PREFIX

    timestamp = datetime.now(timezone.utc)

    def migrate_batch(conn, deadline_table, callback_table, batch):
        callback_inserts = []
        deadline_updates = []

        for deadline in batch:
            try:
                callback_id = uuid6.uuid7()

                # Transform serialized callback to the new representation
                callback_data = deserialize(deadline.callback).serialize() | {
                    "prefix": CALLBACK_METRICS_PREFIX,
                    "dag_id": deadline.dag_id,
                }

                if deadline.callback_state and deadline.callback_state in {
                    CallbackState.FAILED,
                    CallbackState.SUCCESS,
                }:
                    deadline_missed = True
                    callback_state = deadline.callback_state
                else:
                    # Mark the deadlines in non-terminal states as not missed so the scheduler handles them
                    deadline_missed = False
                    callback_state = CallbackState.PENDING

                callback_inserts.append(
                    {
                        "id": callback_id,
                        "type": CallbackType.TRIGGERER,  # Past versions only support triggerer callbacks
                        "fetch_method": CallbackFetchMethod.IMPORT_PATH,  # Past versions only support import_path
                        "data": callback_data,
                        "state": callback_state,
                        "priority_weight": 1,  # Default priority weight
                        "created_at": timestamp,
                    }
                )

                deadline_updates.append(
                    {"deadline_id": deadline.id, "callback_id": callback_id, "missed": deadline_missed}
                )
            except Exception:
                print(f"Failed to migrate deadline: {deadline}")
                raise

        conn.execute(callback_table.insert(), callback_inserts)
        conn.execute(
            deadline_table.update()
            .where(deadline_table.c.id == sa.bindparam("deadline_id"))
            .values(callback_id=sa.bindparam("callback_id"), missed=sa.bindparam("missed")),
            deadline_updates,
        )

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

        deadline_table = table(
            "deadline",
            column("id", UUIDType(binary=False)),
            column("dagrun_id", sa.Integer()),
            column("deadline_time", UtcDateTime(timezone=True)),
            column("callback", JSONField()),
            column("callback_state", sa.String(20)),
            column("missed", sa.Boolean()),
            column("callback_id", UUIDType(binary=False)),
        )

        dag_run_table = table(
            "dag_run",
            column("id", sa.Integer()),
            column("dag_id", StringID()),
        )

        callback_table = table(
            "callback",
            column("id", UUIDType(binary=False)),
            column("type", sa.String(20)),
            column("fetch_method", sa.String(20)),
            column("data", ExtendedJSON()),
            column("state", sa.String(10)),
            column("priority_weight", sa.Integer()),
            column("created_at", UtcDateTime(timezone=True)),
        )

        conn = op.get_bind()
        batch_num = 0
        while True:
            batch_num += 1
            batch = conn.execute(
                select(
                    deadline_table.c.id,
                    deadline_table.c.dagrun_id,
                    deadline_table.c.deadline_time,
                    deadline_table.c.callback,
                    deadline_table.c.callback_state,
                    dag_run_table.c.dag_id,
                )
                .join(dag_run_table, deadline_table.c.dagrun_id == dag_run_table.c.id)
                .where(deadline_table.c.callback_id.is_(None))  # Only get rows that haven't been migrated yet
                .limit(BATCH_SIZE)
            ).fetchall()

            if not batch:
                break

            migrate_batch(conn, deadline_table, callback_table, batch)
            print(f"Migrated {len(batch)} deadline records in batch {batch_num}")

    # Add new columns (temporarily nullable until data has been migrated)
    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("missed", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("callback_id", UUIDType(binary=False), nullable=True))

    migrate_all_data()

    with op.batch_alter_table("deadline") as batch_op:
        # Data for `missed` and `callback_id` has been migrated so make them non-nullable
        batch_op.alter_column("missed", existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column("callback_id", existing_type=UUIDType(binary=False), nullable=False)

        batch_op.create_index("deadline_missed_deadline_time_idx", ["missed", "deadline_time"], unique=False)
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
    from airflow.models.callback import CallbackState

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
                if row.callback_state in {CallbackState.SUCCESS, CallbackState.FAILED}:
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

        deadline_table = table(
            "deadline",
            column("id", UUIDType(binary=False)),
            column("callback_id", UUIDType(binary=False)),
            column("callback", JSONField()),
            column("callback_state", sa.String(20)),
            column("trigger_id", sa.Integer()),
        )

        callback_table = table(
            "callback",
            column("id", UUIDType(binary=False)),
            column("data", ExtendedJSON()),
            column("state", sa.String(10)),
        )

        conn = op.get_bind()
        batch_num = 0

        while True:
            batch_num += 1
            batch = conn.execute(
                select(
                    deadline_table.c.id.label("deadline_id"),
                    deadline_table.c.callback_id,
                    callback_table.c.data.label("callback_data"),
                    callback_table.c.state.label("callback_state"),
                )
                .join(callback_table, deadline_table.c.callback_id == callback_table.c.id)
                .where(deadline_table.c.callback.is_(None))  # Only get rows that haven't been downgraded yet
                .limit(BATCH_SIZE)
            ).fetchall()

            if not batch:
                break

            migrate_batch(conn, deadline_table, callback_table, batch)
            print(f"Migrated {len(batch)} deadline records in batch {batch_num}")

    with op.batch_alter_table("deadline") as batch_op:
        batch_op.add_column(sa.Column("callback_state", sa.VARCHAR(length=20), nullable=True))
        batch_op.add_column(sa.Column("trigger_id", sa.INTEGER(), autoincrement=False, nullable=True))

        # Temporarily nullable until data has been migrated
        batch_op.add_column(sa.Column("callback", JSONField(), nullable=True))

        # Make callback_id nullable so the associated callbacks can be cleared during migration
        batch_op.alter_column("callback_id", existing_type=UUIDType(binary=False), nullable=True)

    migrate_all_data()

    with op.batch_alter_table("deadline") as batch_op:
        # Data for `callback` has been migrated so make it non-nullable
        batch_op.alter_column("callback", existing_type=JSONField(), nullable=False)

        batch_op.drop_constraint(batch_op.f("deadline_callback_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_index("deadline_missed_deadline_time_idx")
        batch_op.create_index(
            batch_op.f("deadline_callback_state_time_idx"), ["callback_state", "deadline_time"], unique=False
        )
        batch_op.drop_column("callback_id")
        batch_op.drop_column("missed")
