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
Fix migration file ORM inconsistencies.

Revision ID: 888b59e02a5b
Revises: 6222ce48e289
Create Date: 2026-02-20 16:13:02.623981

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = "888b59e02a5b"
down_revision = "6222ce48e289"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"
_TASK_INSTANCE_BATCH_SIZE = 10000
_POOL_FIX_PREFIX = "__airflow_pool_fix_888b59e02a5b_"
_VARIABLE_FIX_PREFIX = "__airflow_var_fix_888b59e02a5b_"
_EXECUTOR_CONFIG_PICKLE_HEX = "80047d942e"


def _build_connection_update_sql():
    return """
    UPDATE connection
    SET
        is_encrypted = COALESCE(is_encrypted, FALSE),
        is_extra_encrypted = COALESCE(is_extra_encrypted, FALSE)
    WHERE is_encrypted IS NULL OR is_extra_encrypted IS NULL
    """


def _build_dag_update_sql():
    return """
    UPDATE dag
    SET
        is_paused = COALESCE(is_paused, FALSE),
        has_import_errors = COALESCE(has_import_errors, FALSE)
    WHERE is_paused IS NULL OR has_import_errors IS NULL
    """


def _build_dag_run_update_sql():
    return """
    UPDATE dag_run
    SET
        state = COALESCE(state, 'queued'),
        log_template_id = COALESCE(log_template_id, (SELECT max(id) FROM log_template)),
        updated_at = COALESCE(updated_at, end_date, start_date, queued_at, logical_date, CURRENT_TIMESTAMP)
    WHERE state IS NULL OR log_template_id IS NULL OR updated_at IS NULL
    """


def _build_log_update_sql():
    return """
    UPDATE log
    SET dttm = COALESCE(dttm, logical_date, CURRENT_TIMESTAMP)
    WHERE dttm IS NULL
    """


def _build_slot_pool_update_sql(dialect_name):
    pool_fallback = (
        f"CONCAT('{_POOL_FIX_PREFIX}', id)" if dialect_name == "mysql" else f"'{_POOL_FIX_PREFIX}' || id"
    )
    return f"""
    UPDATE slot_pool
    SET
        slots = COALESCE(slots, 0),
        pool = COALESCE(pool, {pool_fallback})
    WHERE slots IS NULL OR pool IS NULL
    """


def _build_task_instance_update_sql(dialect_name):
    executor_config_fallback = (
        f"decode('{_EXECUTOR_CONFIG_PICKLE_HEX}', 'hex')"
        if dialect_name == "postgresql"
        else f"x'{_EXECUTOR_CONFIG_PICKLE_HEX}'"
    )
    return f"""
    UPDATE task_instance
    SET
        try_number = COALESCE(try_number, 0),
        max_tries = COALESCE(max_tries, -1),
        hostname = COALESCE(hostname, ''),
        unixname = COALESCE(unixname, ''),
        queue = COALESCE(queue, 'default'),
        priority_weight = COALESCE(priority_weight, 1),
        custom_operator_name = COALESCE(custom_operator_name, COALESCE(operator, '')),
        executor_config = COALESCE(executor_config, {executor_config_fallback})
    WHERE
        try_number IS NULL
        OR max_tries IS NULL
        OR hostname IS NULL
        OR unixname IS NULL
        OR queue IS NULL
        OR priority_weight IS NULL
        OR custom_operator_name IS NULL
        OR executor_config IS NULL
    """


def _build_variable_update_sql(dialect_name):
    key_column = "`key`" if dialect_name == "mysql" else "key"
    key_fallback = (
        f"CONCAT('{_VARIABLE_FIX_PREFIX}', id)"
        if dialect_name == "mysql"
        else f"'{_VARIABLE_FIX_PREFIX}' || id"
    )
    return f"""
    UPDATE variable
    SET
        val = COALESCE(val, ''),
        is_encrypted = COALESCE(is_encrypted, FALSE),
        {key_column} = COALESCE({key_column}, {key_fallback})
    WHERE val IS NULL OR is_encrypted IS NULL OR {key_column} IS NULL
    """


def _task_instance_null_filter(task_instance_table):
    return sa.or_(
        task_instance_table.c.try_number.is_(None),
        task_instance_table.c.max_tries.is_(None),
        task_instance_table.c.hostname.is_(None),
        task_instance_table.c.unixname.is_(None),
        task_instance_table.c.queue.is_(None),
        task_instance_table.c.priority_weight.is_(None),
        task_instance_table.c.custom_operator_name.is_(None),
        task_instance_table.c.executor_config.is_(None),
    )


def _batch_update_task_instance():
    """Backfill task_instance nulls in batches to reduce lock duration on large tables."""
    conn = op.get_bind()
    task_instance_table = sa.table(
        "task_instance",
        sa.column("id", sa.Uuid()),
        sa.column("try_number", sa.Integer()),
        sa.column("max_tries", sa.Integer()),
        sa.column("hostname", sa.String()),
        sa.column("unixname", sa.String()),
        sa.column("queue", sa.String()),
        sa.column("priority_weight", sa.Integer()),
        sa.column("custom_operator_name", sa.String()),
        sa.column("operator", sa.String()),
        sa.column("executor_config", sa.LargeBinary()),
    )

    task_instance_null_filter = _task_instance_null_filter(task_instance_table)
    executor_config_default = bytes.fromhex(_EXECUTOR_CONFIG_PICKLE_HEX)
    last_seen_id = None

    while True:
        batch_query = (
            sa.select(task_instance_table.c.id)
            .where(task_instance_null_filter)
            .order_by(task_instance_table.c.id)
            .limit(_TASK_INSTANCE_BATCH_SIZE)
        )
        if last_seen_id is not None:
            batch_query = batch_query.where(task_instance_table.c.id > last_seen_id)

        batch_ids = conn.execute(batch_query).scalars().all()
        if not batch_ids:
            break

        conn.execute(
            task_instance_table.update()
            .where(task_instance_table.c.id.in_(batch_ids))
            .values(
                try_number=sa.func.coalesce(task_instance_table.c.try_number, 0),
                max_tries=sa.func.coalesce(task_instance_table.c.max_tries, -1),
                hostname=sa.func.coalesce(task_instance_table.c.hostname, ""),
                unixname=sa.func.coalesce(task_instance_table.c.unixname, ""),
                queue=sa.func.coalesce(task_instance_table.c.queue, "default"),
                priority_weight=sa.func.coalesce(task_instance_table.c.priority_weight, 1),
                custom_operator_name=sa.func.coalesce(
                    task_instance_table.c.custom_operator_name,
                    sa.func.coalesce(task_instance_table.c.operator, ""),
                ),
                executor_config=sa.func.coalesce(
                    task_instance_table.c.executor_config, executor_config_default
                ),
            )
        )
        last_seen_id = batch_ids[-1]


def upgrade():
    """Apply Fix migration file inconsistencies with ORM."""
    dialect_name = context.get_context().dialect.name

    # Use raw SQL so this migration remains usable in offline mode (--show-sql-only).
    op.execute(_build_connection_update_sql())
    op.execute(_build_dag_update_sql())

    op.execute(
        """
        INSERT INTO log_template (filename, elasticsearch_id, created_at)
        SELECT
            'dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log',
            '{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}',
            CURRENT_TIMESTAMP
        WHERE NOT EXISTS (SELECT 1 FROM log_template)
        """
    )

    op.execute(_build_dag_run_update_sql())

    op.execute(_build_log_update_sql())

    op.execute(_build_slot_pool_update_sql(dialect_name))

    if context.is_offline_mode():
        op.execute(_build_task_instance_update_sql(dialect_name))
    else:
        _batch_update_task_instance()

    op.execute(_build_variable_update_sql(dialect_name))

    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=False)
        batch_op.alter_column("is_extra_encrypted", existing_type=sa.BOOLEAN(), nullable=False)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column("is_paused", existing_type=sa.BOOLEAN(), nullable=False)
        batch_op.alter_column(
            "has_import_errors",
            existing_type=sa.BOOLEAN(),
            nullable=False,
            existing_server_default=sa.text("(false)"),
        )

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("state", existing_type=StringID(length=50), nullable=False)
        batch_op.alter_column("log_template_id", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("updated_at", existing_type=TIMESTAMP(), nullable=False)

    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.alter_column("dttm", existing_type=TIMESTAMP(), nullable=False)

    with op.batch_alter_table("slot_pool", schema=None) as batch_op:
        batch_op.alter_column("pool", existing_type=StringID(length=256), nullable=False)
        batch_op.alter_column("slots", existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("try_number", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column(
            "max_tries", existing_type=sa.INTEGER(), nullable=False, existing_server_default=sa.text("'-1'")
        )
        batch_op.alter_column("hostname", existing_type=StringID(length=1000), nullable=False)
        batch_op.alter_column("unixname", existing_type=StringID(length=1000), nullable=False)
        batch_op.alter_column("queue", existing_type=StringID(length=256), nullable=False)
        batch_op.alter_column("priority_weight", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("custom_operator_name", existing_type=StringID(length=1000), nullable=False)
        batch_op.alter_column("executor_config", existing_type=sa.BLOB(), nullable=False)

    with op.batch_alter_table("variable", schema=None) as batch_op:
        batch_op.alter_column("key", existing_type=StringID(length=250), nullable=False)
        batch_op.alter_column(
            "val", existing_type=sa.TEXT().with_variant(MEDIUMTEXT, "mysql"), nullable=False
        )
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=False)


def downgrade():
    """
    Unapply Fix migration file inconsistencies with ORM.

    NOTE: The data changes made in upgrade() are intentionally one-way. upgrade() filled NULL
    values with safe defaults (e.g. FALSE for booleans, 0 for integers, '' for strings). This
    downgrade only restores column nullability — it does NOT restore the original NULL values,
    because those cannot be distinguished from legitimately-populated values after the fact.
    """
    with op.batch_alter_table("variable", schema=None) as batch_op:
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=True)
        batch_op.alter_column("val", existing_type=sa.TEXT().with_variant(MEDIUMTEXT, "mysql"), nullable=True)
        batch_op.alter_column("key", existing_type=StringID(length=250), nullable=True)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("executor_config", existing_type=sa.BLOB(), nullable=True)
        batch_op.alter_column("custom_operator_name", existing_type=StringID(length=1000), nullable=True)
        batch_op.alter_column("priority_weight", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("queue", existing_type=StringID(length=256), nullable=True)
        batch_op.alter_column("unixname", existing_type=StringID(length=1000), nullable=True)
        batch_op.alter_column("hostname", existing_type=StringID(length=1000), nullable=True)
        batch_op.alter_column(
            "max_tries", existing_type=sa.INTEGER(), nullable=True, existing_server_default=sa.text("'-1'")
        )
        batch_op.alter_column("try_number", existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table("slot_pool", schema=None) as batch_op:
        batch_op.alter_column("slots", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("pool", existing_type=StringID(length=256), nullable=True)

    with op.batch_alter_table("log", schema=None) as batch_op:
        batch_op.alter_column("dttm", existing_type=TIMESTAMP(), nullable=True)

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("updated_at", existing_type=TIMESTAMP(), nullable=True)
        batch_op.alter_column("log_template_id", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("state", existing_type=StringID(length=50), nullable=True)

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.alter_column(
            "has_import_errors",
            existing_type=sa.BOOLEAN(),
            nullable=True,
            existing_server_default=sa.text("(false)"),
        )
        batch_op.alter_column("is_paused", existing_type=sa.BOOLEAN(), nullable=True)

    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("is_extra_encrypted", existing_type=sa.BOOLEAN(), nullable=True)
        batch_op.alter_column("is_encrypted", existing_type=sa.BOOLEAN(), nullable=True)
