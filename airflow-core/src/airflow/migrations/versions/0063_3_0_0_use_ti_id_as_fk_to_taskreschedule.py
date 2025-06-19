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
Use ti_id as FK to TaskReschedule.

Revision ID: d469d27e2a64
Revises: 16f7f5ee874e
Create Date: 2025-03-06 16:04:49.106274

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "d469d27e2a64"
down_revision = "16f7f5ee874e"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Use ti_id as FK to TaskReschedule."""
    dialect_name = op.get_context().dialect.name
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "ti_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"), nullable=True
            )
        )
    if dialect_name == "postgresql":
        op.execute("""
            UPDATE task_reschedule SET ti_id = task_instance.id
            FROM task_instance
            WHERE task_reschedule.task_id = task_instance.task_id
            AND task_reschedule.dag_id = task_instance.dag_id
            AND task_reschedule.run_id = task_instance.run_id
            AND task_reschedule.map_index = task_instance.map_index
            """)
    elif dialect_name == "mysql":
        op.execute("""
            UPDATE task_reschedule tir
            JOIN task_instance ti ON
                tir.task_id = ti.task_id
                AND tir.dag_id = ti.dag_id
                AND tir.run_id = ti.run_id
                AND tir.map_index = ti.map_index
            SET tir.ti_id = ti.id
            """)
    else:
        op.execute("""
            UPDATE task_reschedule
            SET ti_id = (SELECT id FROM task_instance WHERE task_reschedule.task_id = task_instance.task_id
            AND task_reschedule.dag_id = task_instance.dag_id
            AND task_reschedule.run_id = task_instance.run_id
            AND task_reschedule.map_index = task_instance.map_index)
            """)
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.alter_column(
            "ti_id",
            nullable=False,
            existing_type=sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"),
        )
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.drop_constraint("task_reschedule_dr_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
        )

        batch_op.drop_index("idx_task_reschedule_dag_run")
        batch_op.drop_index("idx_task_reschedule_dag_task_run")
        batch_op.drop_column("map_index")
        batch_op.drop_column("dag_id")
        batch_op.drop_column("task_id")
        batch_op.drop_column("run_id")


def downgrade():
    """Unapply Use ti_id as FK to TaskReschedule."""
    dialect_name = op.get_context().dialect.name
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.add_column(sa.Column("run_id", StringID(), nullable=True))
        batch_op.add_column(sa.Column("task_id", StringID(), nullable=True))
        batch_op.add_column(sa.Column("dag_id", StringID(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "map_index",
                sa.INTEGER(),
                server_default=sa.text("-1"),
                nullable=False,
            )
        )
    # fill the task_id, dag_id, run_id, map_index columns from taskinstance
    if dialect_name == "postgresql":
        op.execute("""
                UPDATE task_reschedule
                SET dag_id = task_instance.dag_id,
                    task_id = task_instance.task_id,
                    run_id = task_instance.run_id,
                    map_index = task_instance.map_index
                FROM task_instance
                WHERE task_reschedule.ti_id = task_instance.id
                """)
    elif dialect_name == "mysql":
        op.execute("""
                UPDATE task_reschedule tir
                JOIN task_instance ti ON
                    tir.ti_id = ti.id
                SET tir.dag_id = ti.dag_id,
                    tir.task_id = ti.task_id,
                    tir.run_id = ti.run_id,
                    tir.map_index = ti.map_index
                """)
    else:
        op.execute("""
                UPDATE task_reschedule
                SET dag_id = (SELECT dag_id FROM task_instance WHERE task_reschedule.ti_id = task_instance.id),
                    task_id = (SELECT task_id FROM task_instance WHERE task_reschedule.ti_id = task_instance.id),
                    run_id = (SELECT run_id FROM task_instance WHERE task_reschedule.ti_id = task_instance.id),
                    map_index = (SELECT map_index FROM task_instance WHERE task_reschedule.ti_id = task_instance.id)
                """)
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.drop_column("ti_id")
        batch_op.alter_column("run_id", nullable=False, existing_type=StringID())
        batch_op.alter_column("task_id", nullable=False, existing_type=StringID())
        batch_op.alter_column("dag_id", nullable=False, existing_type=StringID())
        batch_op.alter_column("map_index", nullable=False, existing_type=sa.INTEGER())

        batch_op.create_foreign_key(
            "task_reschedule_dr_fkey",
            "dag_run",
            ["dag_id", "run_id"],
            ["dag_id", "run_id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )
        batch_op.create_index(
            "idx_task_reschedule_dag_task_run", ["dag_id", "task_id", "run_id", "map_index"], unique=False
        )
        batch_op.create_index("idx_task_reschedule_dag_run", ["dag_id", "run_id"], unique=False)
