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
Add task_instance_launch table for durable executor launch records.

Revision ID: 3c5f8e9a1d2b
Revises: c7f0a5d2e9b4
Create Date: 2026-05-10 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "3c5f8e9a1d2b"
down_revision = "c7f0a5d2e9b4"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Create task_instance_launch table for durable executor token tracking."""
    op.create_table(
        "task_instance_launch",
        sa.Column("token", sa.String(256), nullable=False),
        sa.Column("task_instance_id", sa.String(250), nullable=False),
        sa.Column("dag_id", sa.String(250), nullable=False),
        sa.Column("task_id", sa.String(250), nullable=False),
        sa.Column("run_id", sa.String(250), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False, server_default="-1"),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("executor", sa.String(256), nullable=False),
        sa.Column("state", sa.String(20), nullable=False, server_default="active"),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("consumed_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("superseded_at", UtcDateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("token", name="pk_task_instance_launch_token"),
        sa.CheckConstraint(
            "state IN ('active', 'consumed', 'superseded')",
            name="state_enum",
        ),
    )
    op.create_index(
        "idx_task_instance_launch_task_instance_id",
        "task_instance_launch",
        ["task_instance_id"],
    )
    op.create_index(
        "idx_task_instance_launch_state_updated",
        "task_instance_launch",
        ["state", "updated_at"],
    )
    op.create_index(
        "idx_task_instance_launch_created_at",
        "task_instance_launch",
        ["created_at"],
    )


def downgrade():
    """Drop task_instance_launch table."""
    op.drop_table("task_instance_launch")
