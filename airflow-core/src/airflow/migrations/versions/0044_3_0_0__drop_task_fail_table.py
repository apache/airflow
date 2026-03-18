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
Drop task_fail table.

Revision ID: 5f57a45b8433
Revises: 486ac7936b78
Create Date: 2024-10-29 17:49:27.740730
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = "5f57a45b8433"
down_revision = "486ac7936b78"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Drop task_fail table."""
    op.drop_table("task_fail")


def downgrade():
    """Re-add task_fail table."""
    op.create_table(
        "task_fail",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("task_id", StringID(length=250), nullable=False),
        sa.Column("dag_id", StringID(length=250), nullable=False),
        sa.Column("run_id", StringID(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default=sa.text("-1"), nullable=False),
        sa.Column("start_date", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("end_date", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_fail_ti_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("task_fail_pkey")),
    )
    with op.batch_alter_table("task_fail", schema=None) as batch_op:
        batch_op.create_index(
            "idx_task_fail_task_instance", ["dag_id", "task_id", "run_id", "map_index"], unique=False
        )
