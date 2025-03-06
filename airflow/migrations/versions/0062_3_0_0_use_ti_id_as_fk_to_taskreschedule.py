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
Revises: cf87489a35df
Create Date: 2025-03-06 16:04:49.106274

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d469d27e2a64"
down_revision = "cf87489a35df"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Use ti_id as FK to TaskReschedule."""
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "ti_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql"), nullable=False
            )
        )
        batch_op.drop_index("idx_task_reschedule_dag_run")
        batch_op.drop_index("idx_task_reschedule_dag_task_run")
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.drop_constraint("task_reschedule_dr_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey", "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.drop_column("map_index")
        batch_op.drop_column("dag_id")
        batch_op.drop_column("task_id")
        batch_op.drop_column("run_id")


def downgrade():
    """Unapply Use ti_id as FK to TaskReschedule."""
    with op.batch_alter_table("task_reschedule", schema=None) as batch_op:
        batch_op.add_column(sa.Column("run_id", sa.VARCHAR(length=250), autoincrement=False, nullable=False))
        batch_op.add_column(sa.Column("task_id", sa.VARCHAR(length=250), autoincrement=False, nullable=False))
        batch_op.add_column(sa.Column("dag_id", sa.VARCHAR(length=250), autoincrement=False, nullable=False))
        batch_op.add_column(
            sa.Column(
                "map_index",
                sa.INTEGER(),
                server_default=sa.text("'-1'::integer"),
                autoincrement=False,
                nullable=False,
            )
        )
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
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
        batch_op.drop_column("ti_id")
