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


def upgrade():
    """Apply Fix migration file inconsistencies with ORM."""
    dialect_name = context.get_context().dialect.name

    # Use raw SQL so this migration remains usable in offline mode (--show-sql-only).
    op.execute("UPDATE connection SET is_encrypted = FALSE WHERE is_encrypted IS NULL")
    op.execute("UPDATE connection SET is_extra_encrypted = FALSE WHERE is_extra_encrypted IS NULL")

    op.execute("UPDATE dag SET is_paused = FALSE WHERE is_paused IS NULL")
    op.execute("UPDATE dag SET has_import_errors = FALSE WHERE has_import_errors IS NULL")

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

    op.execute("UPDATE dag_run SET state = 'queued' WHERE state IS NULL")
    op.execute(
        """
        UPDATE dag_run
        SET log_template_id = (SELECT max(id) FROM log_template)
        WHERE log_template_id IS NULL
        """
    )
    op.execute(
        """
        UPDATE dag_run
        SET updated_at = COALESCE(end_date, start_date, queued_at, logical_date, CURRENT_TIMESTAMP)
        WHERE updated_at IS NULL
        """
    )

    op.execute("UPDATE log SET dttm = COALESCE(logical_date, CURRENT_TIMESTAMP) WHERE dttm IS NULL")

    op.execute("UPDATE slot_pool SET slots = 0 WHERE slots IS NULL")
    if dialect_name == "mysql":
        op.execute(
            "UPDATE slot_pool SET pool = CONCAT('__airflow_pool_fix_888b59e02a5b_', id) WHERE pool IS NULL"
        )
    else:
        op.execute("UPDATE slot_pool SET pool = '__airflow_pool_fix_888b59e02a5b_' || id WHERE pool IS NULL")

    op.execute("UPDATE task_instance SET try_number = 0 WHERE try_number IS NULL")
    op.execute("UPDATE task_instance SET max_tries = -1 WHERE max_tries IS NULL")
    op.execute("UPDATE task_instance SET hostname = '' WHERE hostname IS NULL")
    op.execute("UPDATE task_instance SET unixname = '' WHERE unixname IS NULL")
    op.execute("UPDATE task_instance SET queue = 'default' WHERE queue IS NULL")
    op.execute("UPDATE task_instance SET priority_weight = 1 WHERE priority_weight IS NULL")
    op.execute(
        "UPDATE task_instance SET custom_operator_name = COALESCE(operator, '') WHERE custom_operator_name IS NULL"
    )
    if dialect_name == "postgresql":
        op.execute(
            "UPDATE task_instance SET executor_config = decode('80047d942e', 'hex') WHERE executor_config IS NULL"
        )
    else:
        op.execute("UPDATE task_instance SET executor_config = x'80047d942e' WHERE executor_config IS NULL")

    op.execute("UPDATE variable SET val = '' WHERE val IS NULL")
    op.execute("UPDATE variable SET is_encrypted = FALSE WHERE is_encrypted IS NULL")
    if dialect_name == "mysql":
        op.execute(
            "UPDATE variable SET `key` = CONCAT('__airflow_var_fix_888b59e02a5b_', id) WHERE `key` IS NULL"
        )
    else:
        op.execute("UPDATE variable SET key = '__airflow_var_fix_888b59e02a5b_' || id WHERE key IS NULL")

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
