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
Unify task attempt ownership without rewriting legacy XCom data.

Revision ID: e7c2a91bd540
Revises: 90e4d18ccadf
Create Date: 2026-09-29 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql, sqlite

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.migrations.utils import sqlite_rebuilds
from airflow.utils.sqlalchemy import ExecutorConfigType, ExtendedJSON, UtcDateTime

revision = "e7c2a91bd540"
down_revision = "90e4d18ccadf"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_COORDINATES = ("dag_id", "task_id", "run_id", "map_index")
_LEGACY = (
    ("xcom", "xcom_v1", "xcom_task_instance_fkey", None),
    ("rendered_task_instance_fields", "rtif_v1", "rtif_ti_fkey", None),
)
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
    "trigger_timeout",
    "next_method",
    "next_kwargs",
    "task_display_name",
    "retry_delay_override",
    "retry_reason",
)
_NOT_EXECUTING_STATES = ("success", "failed", "skipped", "upstream_failed", "removed", "restarting")
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


def _check_source():
    bind = op.get_bind()
    if op.get_context().as_sql:
        return
    inspector = sa.inspect(bind)
    for table, _, name, onupdate in (
        *_LEGACY,
        (
            "task_instance_history",
            None,
            "task_instance_history_ti_fkey",
            "CASCADE",
        ),
    ):
        constraints = inspector.get_foreign_keys(table)
        fk = next((fk for fk in constraints if fk["name"] == name), None)
        if (
            fk is None
            or fk["referred_table"] != "task_instance"
            or fk["constrained_columns"] != list(_COORDINATES)
            or fk["referred_columns"] != list(_COORDINATES)
            or fk["options"].get("ondelete") != "CASCADE"
            or fk["options"].get("onupdate") != onupdate
        ):
            raise RuntimeError(f"Unsupported attempt ownership source schema: {table}.{name}")
        if bind.dialect.name == "postgresql":
            validated = bind.scalar(
                sa.text(
                    "SELECT convalidated FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname=:name"
                ),
                {"table": table, "name": name},
            )
            if not validated:
                raise RuntimeError(f"Attempt ownership requires a validated source FK: {table}.{name}")


def _redirect_legacy(table, constraint, target, *, onupdate=None, not_valid=False):
    if op.get_bind().dialect.name == "mysql":
        quote = op.get_bind().dialect.identifier_preparer.quote
        columns = ", ".join(quote(column) for column in _COORDINATES)
        onupdate_sql = f" ON UPDATE {onupdate}" if onupdate else ""
        op.execute("SET @ti141_foreign_key_checks = @@SESSION.foreign_key_checks")
        op.execute("SET SESSION foreign_key_checks = 0")
        try:
            op.execute(
                f"ALTER TABLE {quote(table)} DROP FOREIGN KEY {quote(constraint)}, "
                "ALGORITHM=INPLACE, LOCK=NONE"
            )
            op.execute(
                f"ALTER TABLE {quote(table)} ADD CONSTRAINT {quote(constraint)} FOREIGN KEY ({columns}) "
                f"REFERENCES {quote(target)} ({columns}) ON DELETE CASCADE{onupdate_sql}, "
                "ALGORITHM=INPLACE, LOCK=NONE"
            )
        finally:
            op.execute("SET SESSION foreign_key_checks = @ti141_foreign_key_checks")
        return
    with op.batch_alter_table(table) as batch:
        batch.drop_constraint(constraint, type_="foreignkey")
        batch.create_foreign_key(
            constraint,
            target,
            list(_COORDINATES),
            list(_COORDINATES),
            ondelete="CASCADE",
            onupdate=onupdate,
            postgresql_not_valid=not_valid,
        )


def upgrade():
    """Retain attempts and give legacy and new task data immutable UUID owners."""
    _check_source()
    with sqlite_rebuilds(op):
        owner = op.create_table(
            "legacy_task_data_owner",
            sa.Column("dag_id", StringID(), nullable=False),
            sa.Column("task_id", StringID(), nullable=False),
            sa.Column("run_id", StringID(), nullable=False),
            sa.Column("map_index", sa.Integer(), nullable=False),
            sa.Column("task_instance_id", sa.Uuid(), nullable=False),
            sa.PrimaryKeyConstraint(*_COORDINATES, name="legacy_task_data_owner_pkey"),
            sa.ForeignKeyConstraint(
                ["task_instance_id"],
                ["task_instance.id"],
                name="legacy_task_data_owner_ti_fkey",
                ondelete="CASCADE",
            ),
        )
        op.create_index("idx_legacy_task_data_owner_ti", "legacy_task_data_owner", ["task_instance_id"])
        op.add_column("log", sa.Column("task_instance_id", sa.Uuid(), nullable=True))
        source = sa.table("task_instance", sa.column("id"), *(sa.column(c) for c in _COORDINATES))
        op.execute(
            owner.insert().from_select(
                [*_COORDINATES, "task_instance_id"],
                sa.select(*(source.c[c] for c in _COORDINATES), source.c.id),
            )
        )
        for old_name, name, constraint, _ in _LEGACY:
            op.rename_table(old_name, name)
            # Validated source FKs plus the complete owner copy prove existing child ownership.
            _redirect_legacy(
                name,
                constraint,
                "legacy_task_data_owner",
                not_valid=op.get_bind().dialect.name == "postgresql",
            )
        live = sa.table(
            "task_instance",
            *(sa.column(c) for c in _COORDINATES),
            sa.column("try_number"),
            sa.column("state"),
        )
        archived = sa.table(
            "task_instance_history", *(sa.column(c) for c in _COORDINATES), sa.column("try_number")
        )
        same_coordinates = [live.c[c] == archived.c[c] for c in _COORDINATES]
        # Clearing archived the try and left the replacement at it; only a row that is still executing that try is the same attempt.
        op.execute(
            live.update()
            .where(
                live.c.state.in_(_NOT_EXECUTING_STATES),
                sa.exists().where(*same_coordinates, archived.c.try_number == live.c.try_number),
            )
            .values(
                try_number=sa.select(sa.func.max(archived.c.try_number))
                .where(*same_coordinates)
                .scalar_subquery()
                + 1
            )
        )
        with op.batch_alter_table("task_instance_history") as batch:
            batch.drop_constraint("task_instance_history_ti_fkey", type_="foreignkey")
        with op.batch_alter_table("task_instance") as batch:
            batch.add_column(sa.Column("working_set", sa.Boolean(), nullable=True, server_default=sa.true()))
            batch.add_column(sa.Column("archived_reason", sa.String(50), nullable=True))
            batch.drop_constraint("task_instance_composite_key", type_="unique")
            batch.create_unique_constraint("task_instance_current_key", [*_COORDINATES, "working_set"])
            batch.create_unique_constraint("task_instance_try_key", [*_COORDINATES, "try_number"])
        ti = sa.table(
            "task_instance",
            sa.column("id"),
            *(sa.column(c) for c in _COPY_COLUMNS),
            sa.column("working_set"),
            sa.column("archived_reason"),
            sa.column("dag_version_id"),
        )
        history = sa.table(
            "task_instance_history",
            sa.column("task_instance_id"),
            *(sa.column(c) for c in _COPY_COLUMNS),
            sa.column("dag_version_id"),
        )
        historical_max_tries = sa.func.coalesce(
            history.c.max_tries,
            sa.case((history.c.try_number > 0, history.c.try_number - 1), else_=0),
        )
        version = sa.table("dag_version", sa.column("id"))
        history_rows = sa.select(
            history.c.task_instance_id,
            *(historical_max_tries if c == "max_tries" else history.c[c] for c in _COPY_COLUMNS),
            sa.null(),
            sa.literal("legacy"),
            version.c.id,
        ).select_from(history.outerjoin(version, history.c.dag_version_id == version.c.id))
        dialect = op.get_bind().dialect.name
        if dialect == "mysql":
            existing = ti.alias("existing")
            history_rows = history_rows.where(
                ~sa.select(1)
                .select_from(existing)
                .where(*(existing.c[c] == history.c[c] for c in (*_COORDINATES, "try_number")))
                .exists()
            )
        columns = [
            "id",
            *_COPY_COLUMNS,
            "working_set",
            "archived_reason",
            "dag_version_id",
        ]
        if dialect == "postgresql":
            insert = (
                postgresql.insert(ti)
                .from_select(columns, history_rows)
                .on_conflict_do_nothing(constraint="task_instance_try_key")
            )
        elif dialect == "sqlite":
            insert = (
                sqlite.insert(ti)
                .from_select(columns, history_rows.where(sa.true()))
                .on_conflict_do_nothing(index_elements=[*_COORDINATES, "try_number"])
            )
        else:
            insert = ti.insert().from_select(columns, history_rows)
        op.execute(insert)
        hitl = sa.table("hitl_detail", sa.column("ti_id"), *(sa.column(c) for c in _HITL_COLUMNS))
        hitl_history = sa.table(
            "hitl_detail_history", sa.column("ti_history_id"), *(sa.column(c) for c in _HITL_COLUMNS)
        )
        op.execute(
            hitl.insert().from_select(
                ["ti_id", *_HITL_COLUMNS],
                sa.select(hitl_history.c.ti_history_id, *(hitl_history.c[c] for c in _HITL_COLUMNS)).join(
                    ti,
                    sa.and_(ti.c.id == hitl_history.c.ti_history_id, ti.c.working_set.is_(None)),
                ),
            )
        )
        op.drop_table("hitl_detail_history")
        op.drop_table("task_instance_history")
        current_attempt = sa.column("working_set", sa.Boolean()).is_(True)
        for name, columns in (
            ("ti_current_state", ["working_set", "state"]),
            ("ti_current_dag_run", ["working_set", "dag_id", "run_id", "state"]),
        ):
            op.create_index(
                name,
                "task_instance",
                columns,
                postgresql_where=current_attempt,
                sqlite_where=current_attempt,
            )
        op.create_table(
            "xcom_v2",
            sa.Column("id", sa.Uuid(), nullable=False),
            sa.Column("task_instance_id", sa.Uuid(), nullable=False),
            sa.Column("key", StringID(length=512), nullable=False),
            sa.Column("value", sa.JSON().with_variant(postgresql.JSONB(), "postgresql")),
            sa.Column("timestamp", TIMESTAMP(), nullable=False),
            sa.Column("dag_result", sa.Boolean(), nullable=True),
            sa.Column("mapped_length", sa.Integer(), nullable=True),
            sa.PrimaryKeyConstraint("id", name="xcom_v2_pkey"),
            sa.UniqueConstraint("task_instance_id", "key", name="xcom_v2_ti_key_uq"),
            sa.ForeignKeyConstraint(
                ["task_instance_id"], ["task_instance.id"], name="xcom_v2_ti_fkey", ondelete="CASCADE"
            ),
            sa.CheckConstraint("mapped_length >= 0", name="xcom_v2_mapped_length_not_negative"),
        )
        op.create_table(
            "rtif_v2",
            sa.Column("id", sa.Uuid(), nullable=False),
            sa.Column("task_instance_id", sa.Uuid(), nullable=False),
            sa.Column("rendered_fields", sa.JSON(), nullable=False),
            sa.Column("k8s_pod_yaml", sa.JSON(), nullable=True),
            sa.PrimaryKeyConstraint("id", name="rtif_v2_pkey"),
            sa.UniqueConstraint("task_instance_id", name="rtif_v2_ti_uq"),
            sa.ForeignKeyConstraint(
                ["task_instance_id"], ["task_instance.id"], name="rtif_v2_ti_fkey", ondelete="CASCADE"
            ),
        )


def downgrade():
    """Refuse downgrade when the predecessor cannot represent retained ownership."""
    if op.get_context().as_sql:
        raise RuntimeError("Offline downgrade cannot verify retained attempt ownership")
    bind = op.get_bind()
    if bind.scalar(sa.text("SELECT 1 FROM task_instance WHERE working_set IS NULL LIMIT 1")):
        raise RuntimeError("Cannot downgrade attempt ownership with historical attempts")
    for name in ("xcom_v2", "rtif_v2"):
        if bind.scalar(sa.text(f"SELECT 1 FROM {name} LIMIT 1")):
            raise RuntimeError(f"Cannot downgrade attempt ownership with data in {name}")
    if bind.scalar(
        sa.text(
            "SELECT 1 FROM legacy_task_data_owner o JOIN task_instance t ON t.id=o.task_instance_id "
            "WHERE o.dag_id<>t.dag_id OR o.task_id<>t.task_id OR o.run_id<>t.run_id OR o.map_index<>t.map_index LIMIT 1"
        )
    ):
        raise RuntimeError("Cannot downgrade attempt ownership after legacy owners changed coordinates")
    with sqlite_rebuilds(op):
        op.drop_table("rtif_v2")
        op.drop_table("xcom_v2")
        op.drop_index("ti_current_state", table_name="task_instance")
        op.drop_index("ti_current_dag_run", table_name="task_instance")
        with op.batch_alter_table("task_instance") as batch:
            batch.drop_constraint("task_instance_current_key", type_="unique")
            batch.drop_constraint("task_instance_try_key", type_="unique")
            batch.create_unique_constraint("task_instance_composite_key", list(_COORDINATES))
            for column in ("working_set", "archived_reason"):
                batch.drop_column(column)
        for old_name, name, constraint, onupdate in _LEGACY:
            _redirect_legacy(name, constraint, "task_instance", onupdate=onupdate)
            op.rename_table(name, old_name)
        op.drop_table("legacy_task_data_owner")
        with op.batch_alter_table("log") as batch:
            batch.drop_column("task_instance_id")
        _restore_history_tables()


def _restore_history_tables():
    op.create_table(
        "task_instance_history",
        sa.Column("task_instance_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("run_id", StringID(), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False, server_default="-1"),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("start_date", UtcDateTime(), nullable=True),
        sa.Column("end_date", UtcDateTime(), nullable=True),
        sa.Column("duration", sa.Float(), nullable=True),
        sa.Column("state", sa.String(20), nullable=True),
        sa.Column("max_tries", sa.Integer(), nullable=True, server_default="-1"),
        sa.Column("hostname", sa.String(1000), nullable=True),
        sa.Column("unixname", sa.String(1000), nullable=True),
        sa.Column("pool", sa.String(256), nullable=False),
        sa.Column("pool_slots", sa.Integer(), nullable=False),
        sa.Column("queue", sa.String(256), nullable=True),
        sa.Column("priority_weight", sa.Integer(), nullable=True),
        sa.Column("operator", sa.String(1000), nullable=True),
        sa.Column("custom_operator_name", sa.String(1000), nullable=True),
        sa.Column("queued_dttm", UtcDateTime(), nullable=True),
        sa.Column("scheduled_dttm", UtcDateTime(), nullable=True),
        sa.Column("queued_by_job_id", sa.Integer(), nullable=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("executor", sa.String(1000), nullable=True),
        sa.Column("executor_config", ExecutorConfigType(), nullable=True),
        sa.Column("updated_at", UtcDateTime(), nullable=True),
        sa.Column("rendered_map_index", sa.String(250), nullable=True),
        sa.Column("context_carrier", ExtendedJSON(), nullable=True),
        sa.Column("external_executor_id", sa.Text(), nullable=True),
        sa.Column("trigger_id", sa.Integer(), nullable=True),
        sa.Column("trigger_timeout", sa.DateTime(), nullable=True),
        sa.Column("next_method", sa.String(1000), nullable=True),
        sa.Column("next_kwargs", ExtendedJSON(), nullable=True),
        sa.Column("task_display_name", sa.String(2000), nullable=True),
        sa.Column("dag_version_id", sa.Uuid(), nullable=True),
        sa.Column("retry_delay_override", sa.Float(), nullable=True),
        sa.Column("retry_reason", sa.String(500), nullable=True),
        sa.PrimaryKeyConstraint("task_instance_id", name="task_instance_history_pkey"),
        sa.UniqueConstraint(*_COORDINATES, "try_number", name="task_instance_history_dtrt_uq"),
        sa.ForeignKeyConstraint(
            list(_COORDINATES),
            [f"task_instance.{c}" for c in _COORDINATES],
            name="task_instance_history_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
    op.create_index("idx_tih_dag_run", "task_instance_history", ["dag_id", "run_id"])
    op.create_table(
        "hitl_detail_history",
        sa.Column("ti_history_id", sa.Uuid(), nullable=False),
        sa.Column("options", sa.JSON(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("defaults", sa.JSON(), nullable=True),
        sa.Column("multiple", sa.Boolean(), nullable=True),
        sa.Column("params", sa.JSON(), nullable=False),
        sa.Column("assignees", sa.JSON(), nullable=True),
        sa.Column("created_at", UtcDateTime(), nullable=False),
        sa.Column("responded_at", UtcDateTime(), nullable=True),
        sa.Column("responded_by", sa.JSON(), nullable=True),
        sa.Column("chosen_options", sa.JSON(), nullable=True),
        sa.Column("params_input", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("ti_history_id", name="hitl_detail_history_pkey"),
        sa.ForeignKeyConstraint(
            ["ti_history_id"],
            ["task_instance_history.task_instance_id"],
            name="hitl_detail_history_tih_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
