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
Update TaskInstanceHistory to use uuid.

Revision ID: aa495c49bebf
Revises: 6a9e7a527a88
Create Date: 2025-02-24 08:21:28.349409

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "aa495c49bebf"
down_revision = "6a9e7a527a88"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Use TI.id in TaskInstanceHistory."""
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("new_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"))
        )
    if op.get_bind().dialect.name == "mysql":
        op.execute(
            """
            UPDATE task_instance_history tih
                JOIN task_instance ti
                ON tih.dag_id = ti.dag_id
                AND tih.task_id = ti.task_id
                AND tih.run_id = ti.run_id
                AND tih.map_index = ti.map_index
                SET tih.new_id = ti.id;
        """
        )
    else:
        op.execute(
            """
            UPDATE task_instance_history
            SET new_id = ti.id
            FROM task_instance ti
            WHERE task_instance_history.dag_id = ti.dag_id
                AND task_instance_history.task_id = ti.task_id
                AND task_instance_history.run_id = ti.run_id
                AND task_instance_history.map_index = ti.map_index
        """
        )
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("id")
        batch_op.alter_column(
            "new_id",
            new_column_name="id",
            existing_type=sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
            existing_nullable=False,
        )
        batch_op.create_primary_key("task_instance_history_pkey", ["id"])
        batch_op.drop_constraint("task_instance_history_ti_fkey", type_="foreignkey")
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.create_foreign_key(
            "task_instance_history_ti_fkey",
            "task_instance",
            ["id"],
            ["id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )


def downgrade():
    """Unapply Use TI.id in TaskInstanceHistory."""
    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("new_id", sa.INTEGER, autoincrement=True, nullable=True))
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            """
            ALTER TABLE task_instance_history ADD COLUMN row_num SERIAL;
            UPDATE task_instance_history SET new_id = row_num;
            ALTER TABLE task_instance_history DROP COLUMN row_num;
        """
        )
    elif op.get_bind().dialect.name == "mysql":
        op.execute(
            """
            UPDATE task_instance_history tih
            JOIN (
                SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_num
                FROM task_instance_history
            ) AS temp ON tih.id = temp.id
            SET tih.new_id = temp.row_num;
            """
        )
    else:
        op.execute("""
            UPDATE task_instance_history
            SET new_id = (SELECT COUNT(*) FROM task_instance_history t2 WHERE t2.id <= task_instance_history.id);
        """)

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_constraint("task_instance_history_ti_fkey", type_="foreignkey")
        batch_op.drop_column("id")  # also drops the associated foreignkey
        batch_op.alter_column("new_id", new_column_name="id", nullable=False, existing_type=sa.INTEGER)
        batch_op.create_primary_key("task_instance_history_pkey", ["id"])
        batch_op.create_foreign_key(
            "task_instance_history_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )
