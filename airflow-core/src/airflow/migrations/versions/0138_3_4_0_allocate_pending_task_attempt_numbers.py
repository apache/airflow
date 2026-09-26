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
Allocate attempt numbers for pending retries and cleared tasks.

Older releases did not archive retries caused by queued or scheduled failures. Retire
those UUIDs before advancing their try numbers, keeping the attempt accessible through
history. Downgrade retains the archive and replacement UUID, as expected by the old scheduler.

Revision ID: a61f0c9d2b47
Revises: c9f4b3e7a218
Create Date: 2026-09-15 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a61f0c9d2b47"
down_revision = "c9f4b3e7a218"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_COPY_COLUMNS = (
    "task_id",
    "dag_id",
    "run_id",
    "map_index",
    "try_number",
    "start_date",
    "end_date",
    "duration",
    "state",
    "max_tries",
    "hostname",
    "unixname",
    "pool",
    "pool_slots",
    "queue",
    "priority_weight",
    "operator",
    "custom_operator_name",
    "queued_dttm",
    "scheduled_dttm",
    "queued_by_job_id",
    "pid",
    "executor",
    "executor_config",
    "updated_at",
    "rendered_map_index",
    "context_carrier",
    "external_executor_id",
    "trigger_id",
    "trigger_timeout",
    "next_method",
    "next_kwargs",
    "task_display_name",
    "dag_version_id",
    "retry_delay_override",
    "retry_reason",
)

_TASK_INSTANCE = sa.table(
    "task_instance",
    sa.column("id", sa.Uuid()),
    *(sa.column(name) for name in _COPY_COLUMNS),
)
_HISTORY = sa.table(
    "task_instance_history",
    sa.column("task_instance_id", sa.Uuid()),
    *(sa.column(name) for name in _COPY_COLUMNS),
)
_HITL_COLUMNS = (
    "options",
    "subject",
    "body",
    "defaults",
    "multiple",
    "params",
    "assignees",
    "created_at",
    "responded_at",
    "responded_by",
    "chosen_options",
    "params_input",
)


def upgrade():
    """Archive legacy retries before allocating their pending attempt numbers."""
    ti = _TASK_INSTANCE
    history = _HISTORY
    dialect = op.get_bind().dialect.name
    same_try = sa.and_(
        *(
            ti.c[name] == history.c[name]
            for name in ("dag_id", "task_id", "run_id", "map_index", "try_number")
        )
    )
    archive = history.insert().from_select(
        ["task_instance_id", *_COPY_COLUMNS],
        sa.select(
            ti.c.id, *(sa.literal("failed") if name == "state" else ti.c[name] for name in _COPY_COLUMNS)
        )
        .select_from(ti.outerjoin(history, same_try))
        .where(ti.c.state == "up_for_retry", history.c.task_instance_id.is_(None)),
    )

    if dialect == "postgresql":
        # Keep returned UUIDs across statements so dependent rows are handled before UUID rotation.
        retired_table = op.create_table(
            "_airflow_retired_retry_ids",
            sa.Column("id", sa.Uuid(), primary_key=True),
            prefixes=["TEMPORARY"],
            postgresql_on_commit="DROP",
        )
        archived = archive.returning(history.c.task_instance_id).cte("archived")
        op.execute(retired_table.insert().from_select(["id"], sa.select(archived.c.task_instance_id)))
        op.execute("ANALYZE _airflow_retired_retry_ids")
        retired_ids = sa.select(retired_table.c.id)
        # EXISTS retains an indexed plan when the UUID set is too large to hash.
        retired = sa.exists(sa.select(1).where(retired_table.c.id == ti.c.id))
    else:
        op.execute(archive)
        # Only newly archived retries still have their retired UUID in the live table.
        retired = sa.and_(
            ti.c.state == "up_for_retry",
            sa.exists(sa.select(1).where(history.c.task_instance_id == ti.c.id)),
        )
        retired_ids = sa.select(ti.c.id).where(retired)
    hitl = sa.table("hitl_detail", sa.column("ti_id"), *(sa.column(name) for name in _HITL_COLUMNS))
    hitl_history = sa.table(
        "hitl_detail_history", sa.column("ti_history_id"), *(sa.column(name) for name in _HITL_COLUMNS)
    )
    op.execute(
        hitl_history.insert().from_select(
            ["ti_history_id", *_HITL_COLUMNS],
            sa.select(hitl.c.ti_id, *(hitl.c[name] for name in _HITL_COLUMNS)).where(
                hitl.c.ti_id.in_(retired_ids)
            ),
        )
    )
    reschedule = sa.table("task_reschedule", sa.column("ti_id"))
    op.execute(reschedule.delete().where(reschedule.c.ti_id.in_(retired_ids)))

    uuid_sql = {
        "postgresql": (
            "(lpad(to_hex((extract(epoch from clock_timestamp()) * 1000)::bigint), 12, '0') || "
            "'7' || replace(substr(gen_random_uuid()::text, 16), '-', ''))::uuid"
        ),
        "mysql": (
            "lower(concat(lpad(hex(cast(unix_timestamp(current_timestamp(3)) * 1000 as unsigned)), "
            "12, '0'), '7', substr(hex(random_bytes(2)), 2), '8', substr(hex(random_bytes(8)), 2)))"
        ),
        "sqlite": (
            "lower(printf('%012x', cast((julianday('now') - 2440587.5) * 86400000 as integer)) || "
            "'7' || substr(hex(randomblob(2)), 2) || '8' || substr(hex(randomblob(8)), 2))"
        ),
    }[dialect]
    op.execute(
        ti.update()
        .where(sa.or_(ti.c.state == "up_for_retry", sa.and_(ti.c.state.is_(None), ti.c.try_number > 0)))
        .values(
            id=sa.case((retired, sa.literal_column(uuid_sql, type_=sa.Uuid())), else_=ti.c.id),
            try_number=ti.c.try_number + 1,
        )
    )
    if dialect == "postgresql":
        op.drop_table("_airflow_retired_retry_ids")


def downgrade():
    """Leave pending attempt allocation to the old scheduler."""
    op.execute(
        _TASK_INSTANCE.update()
        .where(
            sa.or_(_TASK_INSTANCE.c.state == "up_for_retry", _TASK_INSTANCE.c.state.is_(None)),
            _TASK_INSTANCE.c.try_number > 0,
        )
        .values(try_number=_TASK_INSTANCE.c.try_number - 1)
    )
