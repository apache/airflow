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
Add UUID primary key to ``task_instance`` table.

Revision ID: d59cbbef95eb
Revises: 05234396c6fc
Create Date: 2024-10-21 22:39:12.394079
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from airflow.configuration import conf
from airflow.utils.sql_functions import (
    MYSQL_UUID7_FN,
    MYSQL_UUID7_FN_DROP,
    POSTGRES_UUID7_FN,
    POSTGRES_UUID7_FN_DROP,
)

# revision identifiers, used by Alembic.
revision = "d59cbbef95eb"
down_revision = "05234396c6fc"
branch_labels = "None"
depends_on = None
airflow_version = "3.0.0"


ti_table = "task_instance"

# Foreign key columns from task_instance
ti_fk_cols = ["dag_id", "task_id", "run_id", "map_index"]

# Foreign key constraints from other tables to task_instance
ti_fk_constraints = [
    {"table": "rendered_task_instance_fields", "fk": "rtif_ti_fkey"},
    {"table": "task_fail", "fk": "task_fail_ti_fkey"},
    {"table": "task_instance_history", "fk": "task_instance_history_ti_fkey"},
    {"table": "task_instance_note", "fk": "task_instance_note_ti_fkey"},
    {"table": "task_map", "fk": "task_map_task_instance_fkey"},
    {"table": "task_reschedule", "fk": "task_reschedule_ti_fkey"},
    {"table": "xcom", "fk": "xcom_task_instance_fkey"},
]


def _get_type_id_column(dialect_name: str) -> sa.types.TypeEngine:
    # For PostgreSQL, use the UUID type directly as it is more efficient
    if dialect_name == "postgresql":
        return postgresql.UUID(as_uuid=False)
    # For other databases, use String(36) to match UUID format
    return sa.String(36)


def create_foreign_keys():
    for fk in ti_fk_constraints:
        if fk["table"] in ["task_instance_history", "task_map"]:
            continue
        with op.batch_alter_table(fk["table"]) as batch_op:
            batch_op.create_foreign_key(
                constraint_name=fk["fk"],
                referent_table=ti_table,
                local_cols=ti_fk_cols,
                remote_cols=ti_fk_cols,
                ondelete="CASCADE",
            )
    for fk in ti_fk_constraints:
        if fk["table"] not in ["task_instance_history", "task_map"]:
            continue
        with op.batch_alter_table(fk["table"]) as batch_op:
            batch_op.create_foreign_key(
                constraint_name=fk["fk"],
                referent_table=ti_table,
                local_cols=ti_fk_cols,
                remote_cols=ti_fk_cols,
                ondelete="CASCADE",
                onupdate="CASCADE",
            )


def upgrade():
    """Add UUID primary key to task instance table."""
    batch_size = conf.getint("database", "migration_batch_size", fallback=1000)
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    op.add_column("task_instance", sa.Column("id", _get_type_id_column(dialect_name), nullable=True))

    if dialect_name == "postgresql":
        op.execute(POSTGRES_UUID7_FN)

        # Migrate existing rows with UUID v7 using a timestamp-based generation
        while True:
            result = conn.execute(
                text(
                    """
                    WITH cte AS (
                        SELECT ctid
                        FROM task_instance
                        WHERE id IS NULL
                        LIMIT :batch_size
                    )
                    UPDATE task_instance
                    SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, clock_timestamp()))
                    FROM cte
                    WHERE task_instance.ctid = cte.ctid
                    """
                ).bindparams(batch_size=batch_size)
            )
            row_count = result.rowcount
            if row_count == 0:
                break
            print(f"Migrated {row_count} task_instance rows in this batch...")
        op.execute(POSTGRES_UUID7_FN_DROP)

        # Drop existing primary key constraint to task_instance table
        op.execute("ALTER TABLE IF EXISTS task_instance DROP CONSTRAINT task_instance_pkey CASCADE")

    elif dialect_name == "mysql":
        op.execute(MYSQL_UUID7_FN)

        # Migrate existing rows with UUID v7
        op.execute("""
            UPDATE task_instance
            SET id = uuid_generate_v7(coalesce(queued_dttm, start_date, NOW(3)))
            WHERE id IS NULL
        """)

        # Drop this function as it is no longer needed
        op.execute(MYSQL_UUID7_FN_DROP)
        for fk in ti_fk_constraints:
            op.drop_constraint(fk["fk"], fk["table"], type_="foreignkey")
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_pkey", type_="primary")
    elif dialect_name == "sqlite":
        from uuid6 import uuid7

        stmt = text("SELECT COUNT(*) FROM task_instance WHERE id IS NULL")
        conn = op.get_bind()
        task_instances = conn.execute(stmt).scalar()
        uuid_values = [str(uuid7()) for _ in range(task_instances)]

        # Ensure `uuid_values` is a list or iterable with the UUIDs for the update.
        stmt = text("""
            UPDATE task_instance
            SET id = :uuid
            WHERE id IS NULL
        """)

        for uuid_value in uuid_values:
            conn.execute(stmt.bindparams(uuid=uuid_value))

        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_pkey", type_="primary")

    # Add primary key and unique constraint to task_instance table
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column("id", type_=_get_type_id_column(dialect_name), nullable=False)
        batch_op.create_unique_constraint("task_instance_composite_key", ti_fk_cols)
        batch_op.create_primary_key("task_instance_pkey", ["id"])

    # Create foreign key constraints
    create_foreign_keys()


def downgrade():
    """Drop UUID primary key to task instance table."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    if dialect_name == "postgresql":
        op.execute("ALTER TABLE IF EXISTS task_instance DROP CONSTRAINT task_instance_composite_key CASCADE")
        op.execute(POSTGRES_UUID7_FN_DROP)

    elif dialect_name == "mysql":
        for fk in ti_fk_constraints:
            op.drop_constraint(fk["fk"], fk["table"], type_="foreignkey")

        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_composite_key", type_="unique")
        op.execute(MYSQL_UUID7_FN_DROP)

    elif dialect_name == "sqlite":
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_constraint("task_instance_composite_key", type_="unique")

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_pkey", type_="primary")
        batch_op.drop_column("id")
        batch_op.create_primary_key("task_instance_pkey", ti_fk_cols)

    # Re-add foreign key constraints
    create_foreign_keys()
