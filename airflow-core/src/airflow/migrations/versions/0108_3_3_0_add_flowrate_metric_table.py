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
Add FlowRateMetric table for task run resource tracking.

Revision ID: 9f3b2c1d4e5f
Revises: 6222ce48e289
Create Date: 2026-03-12 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "9f3b2c1d4e5f"
down_revision = "6222ce48e289"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add flowrate_metric table."""
    op.create_table(
        "flowrate_metric",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("run_id", StringID(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("start_date", UtcDateTime, nullable=True),
        sa.Column("end_date", UtcDateTime, nullable=True),
        sa.Column("cpu_request", sa.Float(), nullable=True),
        sa.Column("memory_request", sa.Float(), nullable=True),
        sa.Column("estimated_cost", sa.Float(), nullable=True),
    )
    op.create_index(
        "ix_flowrate_metric_dag_run_task",
        "flowrate_metric",
        ["dag_id", "run_id", "task_id"],
    )


def downgrade():
    """Drop flowrate_metric table."""
    op.drop_index("ix_flowrate_metric_dag_run_task", table_name="flowrate_metric")
    op.drop_table("flowrate_metric")

