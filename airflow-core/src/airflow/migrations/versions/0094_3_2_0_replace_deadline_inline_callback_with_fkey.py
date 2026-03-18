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

import json
from datetime import date, datetime, timedelta, timezone
from textwrap import dedent
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import column, select, table

from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime

# revision identifiers, used by Alembic.
revision = "e812941398f4"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"

BATCH_SIZE = 1000
_CALLBACK_TYPE_TRIGGERER = "triggerer"
_CALLBACK_FETCH_METHOD_IMPORT_PATH = "import_path"
_CALLBACK_STATE_FAILED = "failed"
_CALLBACK_STATE_PENDING = "pending"
_CALLBACK_STATE_SUCCESS = "success"
_CALLBACK_TERMINAL_STATES = frozenset({_CALLBACK_STATE_FAILED, _CALLBACK_STATE_SUCCESS})
_CALLBACK_METRICS_PREFIX = "deadline_alerts"
_SERDE_CLASSNAME = "__classname__"
_SERDE_VERSION = "__version__"
_SERDE_DATA = "__data__"
_SERDE_DATETIME_TIMESTAMP = "timestamp"
_SERDE_DATETIME_TIMEZONE = "tz"
_SERDE_TUPLE_TYPES = {
    "builtins.frozenset": frozenset,
    "builtins.set": set,
    "builtins.tuple": tuple,
}
_SERDE_DATETIME_TYPES = {
    "datetime.datetime",
    "pendulum.datetime.DateTime",
}
_SERDE_DATE_TYPES = {
    "datetime.date",
    "pendulum.date.Date",
}
_SERDE_TIMEDELTA_TYPES = {
    "datetime.timedelta",
}
_SERDE_TIMEZONE_TYPES = {
    "pendulum.tz.timezone.FixedTimezone",
    "pendulum.tz.timezone.Timezone",
    "zoneinfo.ZoneInfo",
}


def _deserialize_task_sdk_value(value):
    """Deserialize a minimal subset of Task SDK serde values used in callback kwargs."""
    if value is None or isinstance(value, bool | float | int | str):
        return value

    if isinstance(value, list):
        return [_deserialize_task_sdk_value(item) for item in value]

    if not isinstance(value, dict):
        return value

    if _SERDE_CLASSNAME not in value or _SERDE_VERSION not in value:
        return {key: _deserialize_task_sdk_value(item) for key, item in value.items()}

    classname = value[_SERDE_CLASSNAME]
    data = _deserialize_task_sdk_value(value.get(_SERDE_DATA))

    if classname in _SERDE_TUPLE_TYPES:
        return _SERDE_TUPLE_TYPES[classname](data)

    if classname in _SERDE_TIMEZONE_TYPES:
        return _deserialize_task_sdk_timezone(data)

    if classname in _SERDE_DATETIME_TYPES:
        if not isinstance(data, dict) or _SERDE_DATETIME_TIMESTAMP not in data:
            raise ValueError(f"Unsupported datetime serde payload: {value!r}")
        tz = None
        if _SERDE_DATETIME_TIMEZONE in data:
            tz = _deserialize_task_sdk_timezone(data[_SERDE_DATETIME_TIMEZONE])
        return datetime.fromtimestamp(float(data[_SERDE_DATETIME_TIMESTAMP]), tz=tz)

    if classname in _SERDE_DATE_TYPES:
        if not isinstance(data, str):
            raise ValueError(f"Unsupported date serde payload: {value!r}")
        return date.fromisoformat(data)

    if classname in _SERDE_TIMEDELTA_TYPES:
        return timedelta(seconds=float(data))

    raise ValueError(f"Unsupported Task SDK serde value in callback kwargs: {classname}")


def _deserialize_task_sdk_timezone(value):
    """Deserialize timezone values produced by Task SDK serde."""
    if value in (None, "UTC"):
        return timezone.utc if value == "UTC" else None

    if isinstance(value, int):
        return timezone(timedelta(seconds=value))

    if isinstance(value, str):
        return ZoneInfo(value)

    if isinstance(value, list) and len(value) == 3:
        data, classname, _version = value
        if classname in _SERDE_TIMEZONE_TYPES:
            return _deserialize_task_sdk_timezone(data)

    raise ValueError(f"Unsupported timezone serde payload: {value!r}")


def _extract_callback_data(deadline_callback, dag_id):
    """Convert the stored deadline callback payload into the new callback.data structure."""
    callback = deadline_callback if isinstance(deadline_callback, dict) else json.loads(deadline_callback)
    callback_data = callback.get(_SERDE_DATA)
    if not isinstance(callback_data, dict):
        raise ValueError(f"Deadline callback is missing {_SERDE_DATA}: {callback!r}")

    callback_path = callback_data.get("path")
    if not callback_path:
        raise ValueError(f"Deadline callback is missing path: {callback!r}")

    return {
        "path": callback_path,
        "kwargs": _deserialize_task_sdk_value(callback_data.get("kwargs", {})),
        "prefix": _CALLBACK_METRICS_PREFIX,
        "dag_id": dag_id,
    }


def _get_migrated_callback_state(callback_state):
    """Return callback state and missed flag for the migrated callback row."""
    if callback_state in _CALLBACK_TERMINAL_STATES:
        return callback_state, True
    return _CALLBACK_STATE_PENDING, False


def upgrade():
    """Replace Deadline table's inline callback fields with callback_id foreign key."""
    import uuid6

    timestamp = datetime.now(timezone.utc)

    def migrate_batch(conn, deadline_table, callback_table, batch):
        callback_inserts = []
        deadline_updates = []

        for deadline in batch:
            try:
                callback_id = uuid6.uuid7()

                callback_data = _extract_callback_data(deadline.callback, deadline.dag_id)
                callback_state, deadline_missed = _get_migrated_callback_state(deadline.callback_state)

                callback_inserts.append(
                    {
                        "id": callback_id,
                        "type": _CALLBACK_TYPE_TRIGGERER,
                        "fetch_method": _CALLBACK_FETCH_METHOD_IMPORT_PATH,
                        "data": callback_data,
                        "state": callback_state,
                        "priority_weight": 1,
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
            column("id", sa.Uuid()),
            column("dagrun_id", sa.Integer()),
            column("deadline_time", UtcDateTime(timezone=True)),
            column("callback", sa.JSON()),
            column("callback_state", sa.String(20)),
            column("missed", sa.Boolean()),
            column("callback_id", sa.Uuid()),
        )

        dag_run_table = table(
            "dag_run",
            column("id", sa.Integer()),
            column("dag_id", sa.String()),
        )

        callback_table = table(
            "callback",
            column("id", sa.Uuid()),
            column("type", sa.String(20)),
            column("fetch_method", sa.String(20)),
            column("data", ExtendedJSON()),
            column("state", sa.String(10)),
            column("priority_weight", sa.Integer()),
            column("created_at", UtcDateTime(timezone=True)),
        )

        conn = op.get_bind()
        batch_num = 0
        last_seen_id = None
        while True:
            batch_num += 1
            batch_query = (
                select(
                    deadline_table.c.id,
                    deadline_table.c.callback,
                    deadline_table.c.callback_state,
                    dag_run_table.c.dag_id,
                )
                .join(dag_run_table, deadline_table.c.dagrun_id == dag_run_table.c.id)
                .where(deadline_table.c.callback_id.is_(None))
                .order_by(deadline_table.c.id)
                .limit(BATCH_SIZE)
            )
            if last_seen_id is not None:
                batch_query = batch_query.where(deadline_table.c.id > last_seen_id)

            batch = conn.execute(batch_query).fetchall()

            if not batch:
                break

            migrate_batch(conn, deadline_table, callback_table, batch)
            last_seen_id = batch[-1].id
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
                    _SERDE_DATA: filtered_cb_data,
                    _SERDE_CLASSNAME: "airflow.sdk.definitions.deadline.AsyncCallback",
                    _SERDE_VERSION: 0,
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

        deadline_table = table(
            "deadline",
            column("id", sa.Uuid()),
            column("callback_id", sa.Uuid()),
            column("callback", sa.JSON()),
            column("callback_state", sa.String(20)),
            column("trigger_id", sa.Integer()),
        )

        callback_table = table(
            "callback",
            column("id", sa.Uuid()),
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
        batch_op.add_column(sa.Column("callback", sa.JSON(), nullable=True))

        # Make callback_id nullable so the associated callbacks can be cleared during migration
        batch_op.alter_column("callback_id", existing_type=sa.Uuid(), nullable=True)

    migrate_all_data()

    with op.batch_alter_table("deadline") as batch_op:
        # Data for `callback` has been migrated so make it non-nullable
        batch_op.alter_column("callback", existing_type=sa.JSON(), nullable=False)

        batch_op.drop_constraint(batch_op.f("deadline_callback_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_index("deadline_missed_deadline_time_idx")
        batch_op.create_index(
            batch_op.f("deadline_callback_state_time_idx"), ["callback_state", "deadline_time"], unique=False
        )
        batch_op.drop_column("callback_id")
        batch_op.drop_column("missed")
