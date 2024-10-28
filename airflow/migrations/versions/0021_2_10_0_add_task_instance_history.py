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
Add task_instance_history.

Revision ID: d482b7261ff9
Revises: c4602ba06b4b
Create Date: 2024-05-30 14:57:29.765015

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import StringID
from airflow.utils.sqlalchemy import ExecutorConfigType, ExtendedJSON, UtcDateTime

# revision identifiers, used by Alembic.
revision = "d482b7261ff9"
down_revision = "c4602ba06b4b"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Add task_instance_history table."""
    op.create_table(
        "task_instance_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("run_id", StringID(), nullable=False),
        sa.Column(
            "map_index", sa.Integer(), server_default=sa.text("-1"), nullable=False
        ),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("start_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("end_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("duration", sa.Float(), nullable=True),
        sa.Column("state", sa.String(length=20), nullable=True),
        sa.Column("max_tries", sa.Integer(), server_default=sa.text("-1"), nullable=True),
        sa.Column("hostname", sa.String(length=1000), nullable=True),
        sa.Column("unixname", sa.String(length=1000), nullable=True),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("pool", sa.String(length=256), nullable=False),
        sa.Column("pool_slots", sa.Integer(), nullable=False),
        sa.Column("queue", sa.String(length=256), nullable=True),
        sa.Column("priority_weight", sa.Integer(), nullable=True),
        sa.Column("operator", sa.String(length=1000), nullable=True),
        sa.Column("custom_operator_name", sa.String(length=1000), nullable=True),
        sa.Column("queued_dttm", UtcDateTime(timezone=True), nullable=True),
        sa.Column("queued_by_job_id", sa.Integer(), nullable=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("executor", sa.String(length=1000), nullable=True),
        sa.Column("executor_config", ExecutorConfigType(), nullable=True),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("rendered_map_index", sa.String(length=250), nullable=True),
        sa.Column("external_executor_id", sa.String(length=250), nullable=True),
        sa.Column("trigger_id", sa.Integer(), nullable=True),
        sa.Column("trigger_timeout", sa.DateTime(), nullable=True),
        sa.Column("next_method", sa.String(length=1000), nullable=True),
        sa.Column("next_kwargs", ExtendedJSON(), nullable=True),
        sa.Column("task_display_name", sa.String(length=2000), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_instance_history_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        sa.UniqueConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            "try_number",
            name="task_instance_history_dtrt_uq",
        ),
    )


def downgrade():
    """Drop task_instance_history table."""
    op.drop_table("task_instance_history")
