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
Add recorded state and owner to trigger.

Revision ID: 5fbfa42a2d01
Revises: 38c8df3fcb9c
Create Date: 2026-10-28 12:00:00.000000

"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import context, op

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.models.base import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "5fbfa42a2d01"
down_revision = "38c8df3fcb9c"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"

_trigger = sa.table(
    "trigger",
    sa.column("id", sa.Integer),
    sa.column("dag_id", sa.String),
    sa.column("owner_ref", sa.JSON),
)
_task_instance = sa.table(
    "task_instance",
    sa.column("id", sa.Uuid),
    sa.column("trigger_id", sa.Integer),
    sa.column("dag_id", sa.String),
)
_callback = sa.table(
    "callback", sa.column("id", sa.Uuid), sa.column("trigger_id", sa.Integer), sa.column("data", sa.Text)
)


def _decode(value):
    """Undo the dict encoding of ``callback.data`` (``{"__type": "dict", "__var": {...}}`` at every level)."""
    if isinstance(value, dict):
        if value.get("__type") == "dict" and isinstance(value.get("__var"), dict):
            value = value["__var"]
        return {key: _decode(item) for key, item in value.items()}
    return value


def _callback_dag_id(raw_data) -> str | None:
    data = _decode(json.loads(raw_data) if isinstance(raw_data, str) else raw_data) or {}
    context = (data.get("kwargs") or {}).get("context") or {}
    return data.get("dag_id") or (context.get("dag_run") or {}).get("dag_id")


def upgrade():
    """Add nullable recorded-state and owner columns to trigger, and record the owner of existing rows."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("trigger", schema=None) as batch_op:
            batch_op.add_column(sa.Column("consecutive_failures", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("first_failed_at", UtcDateTime(), nullable=True))
            batch_op.add_column(sa.Column("last_failed_at", UtcDateTime(), nullable=True))
            batch_op.add_column(sa.Column("last_error", sa.Text(), nullable=True))
            batch_op.add_column(sa.Column("last_event_at", UtcDateTime(), nullable=True))
            batch_op.add_column(sa.Column("dag_id", StringID(), nullable=True))
            batch_op.add_column(sa.Column("owner_ref", sa.JSON(), nullable=True))

    if context.is_offline_mode():
        # Existing rows keep no owner, and a deferred task's or callback's trigger is then not shown by the
        # triggers API until the trigger is re-created.
        return
    # Trigger rows only live while something waits on them, so there are few to fill in.
    conn = op.get_bind()
    owners = [
        {"tid": trigger_id, "b_dag_id": dag_id, "b_owner_ref": {"ti_id": str(ti_id)}}
        for ti_id, trigger_id, dag_id in conn.execute(
            sa.select(_task_instance.c.id, _task_instance.c.trigger_id, _task_instance.c.dag_id).where(
                _task_instance.c.trigger_id.is_not(None)
            )
        )
    ]
    owners += [
        {
            "tid": trigger_id,
            "b_dag_id": _callback_dag_id(raw_data),
            "b_owner_ref": {"callback_id": str(callback_id)},
        }
        for callback_id, trigger_id, raw_data in conn.execute(
            sa.select(_callback.c.id, _callback.c.trigger_id, _callback.c.data).where(
                _callback.c.trigger_id.is_not(None)
            )
        )
    ]
    if owners:
        conn.execute(
            _trigger.update()
            .where(_trigger.c.id == sa.bindparam("tid"))
            .values(dag_id=sa.bindparam("b_dag_id"), owner_ref=sa.bindparam("b_owner_ref", type_=sa.JSON)),
            owners,
        )


def downgrade():
    """Remove the recorded-state and owner columns from trigger."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("trigger", schema=None) as batch_op:
            batch_op.drop_column("owner_ref")
            batch_op.drop_column("dag_id")
            batch_op.drop_column("last_event_at")
            batch_op.drop_column("last_error")
            batch_op.drop_column("last_failed_at")
            batch_op.drop_column("first_failed_at")
            batch_op.drop_column("consecutive_failures")
