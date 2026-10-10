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

from contextlib import contextmanager

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql, sqlite

from airflow.migrations.db_types import TIMESTAMP, StringID
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


# Unlike disable_sqlite_fkeys, a failed SQLite upgrade rolls back atomically and foreign_keys is always restored.
@contextmanager
def _sqlite_rebuilds():
    if op.get_bind().dialect.name != "sqlite":
        yield
        return
    if op.get_context().as_sql:
        raise RuntimeError("SQLite offline SQL cannot render this migration's table rebuilds")
    enabled = op.get_bind().exec_driver_sql("PRAGMA foreign_keys").scalar()
    with op.get_context().autocommit_block():
        op.execute("PRAGMA foreign_keys=OFF")
    try:
        with op.get_bind().begin_nested():
            yield
    finally:
        with op.get_context().autocommit_block():
            op.execute(f"PRAGMA foreign_keys={int(enabled)}")


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
    with _sqlite_rebuilds():
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


def _build_task_instance_table():
    return sa.table(
        "task_instance",
        sa.column("id"),
        *(sa.column(c) for c in _COPY_COLUMNS),
        sa.column("working_set", sa.Boolean()),
        sa.column("dag_version_id"),
    )


def _build_legacy_owner_table():
    return sa.table(
        "legacy_task_data_owner", *(sa.column(c) for c in _COORDINATES), sa.column("task_instance_id")
    )


def _select_latest_attempts(ti):
    newer = ti.alias("newer")
    # An anti-join, because grouping by max(try_number) aggregates the whole table however few rows need copying.
    return (
        sa.select(ti.c.id, *(ti.c[c] for c in _COORDINATES))
        .where(
            ~sa.exists().where(
                *(newer.c[c] == ti.c[c] for c in _COORDINATES), newer.c.try_number > ti.c.try_number
            )
        )
        .subquery()
    )


def _has_duplicate_attempts(bind, ti) -> bool:
    coordinates = [ti.c[c] for c in _COORDINATES]
    for where, group_by in (
        (ti.c.working_set.is_not(None), coordinates),
        (sa.true(), [*coordinates, ti.c.try_number]),
    ):
        duplicated = sa.select(1).select_from(ti).where(where).group_by(*group_by).having(sa.func.count() > 1)
        if bind.scalar(duplicated.limit(1)):
            return True
    return False


def _select_archived_ids(ti):
    return sa.select(ti.c.id).where(ti.c.working_set.is_(None))


def _count_orphaned_archived_attempts(bind, ti) -> int:
    current = ti.alias("current")
    orphaned = (
        sa.select(sa.func.count())
        .select_from(ti)
        .where(
            ti.c.working_set.is_(None),
            ~sa.exists().where(
                *(current.c[c] == ti.c[c] for c in _COORDINATES), current.c.working_set.is_not(None)
            ),
        )
    )
    return bind.scalar(orphaned)


def _has_moved_owners_with_legacy_rows(bind, ti) -> bool:
    owner = _build_legacy_owner_table()
    moved = sa.or_(*(owner.c[c] != ti.c[c] for c in _COORDINATES))
    for name in ("xcom_v1", "rtif_v1"):
        legacy = sa.table(name, *(sa.column(c) for c in _COORDINATES))
        found = (
            sa.select(1)
            .select_from(owner.join(ti, ti.c.id == owner.c.task_instance_id))
            .where(moved, sa.exists().where(*(legacy.c[c] == owner.c[c] for c in _COORDINATES)))
        )
        if bind.scalar(found.limit(1)):
            return True
    return False


def _delete_moved_owners(ti):
    owner = _build_legacy_owner_table()
    op.execute(
        owner.delete().where(
            sa.exists().where(
                ti.c.id == owner.c.task_instance_id,
                sa.or_(*(ti.c[c] != owner.c[c] for c in _COORDINATES)),
            )
        )
    )


def _delete_legacy_rows_of_archived_attempts(ti):
    owner = _build_legacy_owner_table()
    archived_ids = _select_archived_ids(ti)
    # Legacy rows owned by an archived attempt are hidden from its successor, so keeping them would expose them again.
    for name in ("xcom_v1", "rtif_v1"):
        legacy = sa.table(name, *(sa.column(c) for c in _COORDINATES))
        op.execute(
            legacy.delete().where(
                sa.exists().where(
                    *(owner.c[c] == legacy.c[c] for c in _COORDINATES),
                    owner.c.task_instance_id.in_(archived_ids),
                )
            )
        )


def _move_owners_to_current_attempts(ti):
    owner = _build_legacy_owner_table()
    current = ti.alias("current")
    archived_ids = _select_archived_ids(ti)
    xcom_v2 = sa.table("xcom_v2", sa.column("task_instance_id"))
    rtif_v2 = sa.table("rtif_v2", sa.column("task_instance_id"))
    # Deleting archived attempts must not cascade into legacy rows through an owner that points at one.
    op.execute(
        owner.update()
        .where(owner.c.task_instance_id.in_(archived_ids))
        .values(
            task_instance_id=sa.select(current.c.id)
            .where(*(current.c[c] == owner.c[c] for c in _COORDINATES), current.c.working_set.is_not(None))
            .scalar_subquery()
        )
    )
    # Legacy tables reference owners by coordinates, so attempts created after the upgrade need an owner row first.
    op.execute(
        owner.insert().from_select(
            [*_COORDINATES, "task_instance_id"],
            sa.select(*(ti.c[c] for c in _COORDINATES), ti.c.id).where(
                ti.c.working_set.is_not(None),
                ti.c.id.in_(sa.select(xcom_v2.c.task_instance_id))
                | ti.c.id.in_(sa.select(rtif_v2.c.task_instance_id)),
                ~sa.exists().where(*(owner.c[c] == ti.c[c] for c in _COORDINATES)),
            ),
        )
    )


def _delete_replaced_legacy_rows(v1, v2, latest, *matching):
    op.execute(
        v1.delete().where(
            sa.exists().where(
                v2.c.task_instance_id == latest.c.id,
                *(latest.c[c] == v1.c[c] for c in _COORDINATES),
                *matching,
            )
        )
    )


def _copy_xcom_to_legacy(latest):
    columns = ("key", "value", "timestamp", "dag_result", "mapped_length")
    v2 = sa.table("xcom_v2", sa.column("task_instance_id"), *(sa.column(c) for c in columns))
    v1 = sa.table("xcom_v1", *(sa.column(c) for c in (*_COORDINATES, *columns, "dag_run_id")))
    dag_run = sa.table("dag_run", sa.column("id"), sa.column("dag_id"), sa.column("run_id"))
    _delete_replaced_legacy_rows(v1, v2, latest, v2.c.key == v1.c.key)
    source = v2.join(latest, v2.c.task_instance_id == latest.c.id).join(
        dag_run, sa.and_(dag_run.c.dag_id == latest.c.dag_id, dag_run.c.run_id == latest.c.run_id)
    )
    op.execute(
        v1.insert().from_select(
            [*_COORDINATES, *columns, "dag_run_id"],
            sa.select(
                *(latest.c[c] for c in _COORDINATES), *(v2.c[c] for c in columns), dag_run.c.id
            ).select_from(source),
        )
    )


def _copy_rendered_fields_to_legacy(latest):
    columns = ("rendered_fields", "k8s_pod_yaml")
    v2 = sa.table("rtif_v2", sa.column("task_instance_id"), *(sa.column(c) for c in columns))
    v1 = sa.table("rtif_v1", *(sa.column(c) for c in (*_COORDINATES, *columns)))
    _delete_replaced_legacy_rows(v1, v2, latest)
    op.execute(
        v1.insert().from_select(
            [*_COORDINATES, *columns],
            sa.select(*(latest.c[c] for c in _COORDINATES), *(v2.c[c] for c in columns)).select_from(
                v2.join(latest, v2.c.task_instance_id == latest.c.id)
            ),
        )
    )


def _move_archived_attempts_to_history(ti):
    history = sa.table(
        "task_instance_history",
        sa.column("task_instance_id"),
        *(sa.column(c) for c in _COPY_COLUMNS),
        sa.column("dag_version_id"),
    )
    archived = ti.c.working_set.is_(None)
    archived_ids = _select_archived_ids(ti)
    op.execute(
        history.insert().from_select(
            ["task_instance_id", *_COPY_COLUMNS, "dag_version_id"],
            sa.select(ti.c.id, *(ti.c[c] for c in _COPY_COLUMNS), ti.c.dag_version_id).where(archived),
        )
    )
    hitl = sa.table("hitl_detail", sa.column("ti_id"), *(sa.column(c) for c in _HITL_COLUMNS))
    hitl_history = sa.table(
        "hitl_detail_history", sa.column("ti_history_id"), *(sa.column(c) for c in _HITL_COLUMNS)
    )
    op.execute(
        hitl_history.insert().from_select(
            ["ti_history_id", *_HITL_COLUMNS],
            sa.select(hitl.c.ti_id, *(hitl.c[c] for c in _HITL_COLUMNS))
            .join(ti, ti.c.id == hitl.c.ti_id)
            .where(archived),
        )
    )
    # SQLite runs this with foreign keys off, so cascades cannot be relied on.
    for child in ("hitl_detail", "task_instance_note", "task_reschedule"):
        child_table = sa.table(child, sa.column("ti_id"))
        op.execute(child_table.delete().where(child_table.c.ti_id.in_(archived_ids)))
    op.execute(ti.delete().where(archived))


def downgrade():
    """Fold the latest attempt's data back into the legacy tables and restore archived attempts to history."""
    if op.get_context().as_sql:
        raise RuntimeError("Offline downgrade cannot verify retained attempt ownership")
    bind = op.get_bind()
    ti = _build_task_instance_table()
    if _has_duplicate_attempts(bind, ti):
        raise RuntimeError("Cannot downgrade attempt ownership with multiple attempts sharing coordinates")
    if orphans := _count_orphaned_archived_attempts(bind, ti):
        raise RuntimeError(
            f"Cannot downgrade attempt ownership: {orphans} archived attempts (task_instance rows with "
            "working_set IS NULL) have no current attempt at their coordinates; delete them first"
        )
    if _has_moved_owners_with_legacy_rows(bind, ti):
        raise RuntimeError("Cannot downgrade attempt ownership after legacy owners changed coordinates")
    with _sqlite_rebuilds():
        _create_history_tables()
        _delete_moved_owners(ti)
        _delete_legacy_rows_of_archived_attempts(ti)
        _move_owners_to_current_attempts(ti)
        latest = _select_latest_attempts(ti)
        _copy_xcom_to_legacy(latest)
        _copy_rendered_fields_to_legacy(latest)
        _move_archived_attempts_to_history(ti)
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
        with op.batch_alter_table("task_instance_history") as batch:
            batch.create_foreign_key(
                "task_instance_history_ti_fkey",
                "task_instance",
                list(_COORDINATES),
                list(_COORDINATES),
                ondelete="CASCADE",
                onupdate="CASCADE",
            )
        for old_name, name, constraint, onupdate in _LEGACY:
            _redirect_legacy(name, constraint, "task_instance", onupdate=onupdate)
            op.rename_table(name, old_name)
        op.drop_table("legacy_task_data_owner")
        with op.batch_alter_table("log") as batch:
            batch.drop_column("task_instance_id")


def _create_history_tables():
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
