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
Add task_group_instance table for TaskGroup retry state.

Revision ID: 1e6bb6c1f2d7
Revises: f8c9d7e6b5a4
Create Date: 2026-02-11 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import COLLATION_ARGS, ID_LEN
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "1e6bb6c1f2d7"
down_revision = "f8c9d7e6b5a4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Create task_group_instance table."""
    op.create_table(
        "task_group_instance",
        sa.Column("dag_id", sa.String(length=ID_LEN, **COLLATION_ARGS), nullable=False),
        sa.Column("run_id", sa.String(length=ID_LEN, **COLLATION_ARGS), nullable=False),
        sa.Column("task_group_id", sa.String(length=ID_LEN, **COLLATION_ARGS), nullable=False),
        sa.Column("try_number", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_retry_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("dag_id", "run_id", "task_group_id", name=op.f("task_group_instance_pkey")),
    )
    op.create_index(
        "idx_task_group_instance_dag_run",
        "task_group_instance",
        ["dag_id", "run_id"],
        unique=False,
    )


def downgrade():
    """Drop task_group_instance table."""
    op.drop_index("idx_task_group_instance_dag_run", table_name="task_group_instance")
    op.drop_table("task_group_instance")
