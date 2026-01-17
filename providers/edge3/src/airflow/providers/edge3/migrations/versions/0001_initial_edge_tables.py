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
Initial Edge3 provider tables.

Revision ID: 0001_initial_edge_tables
Revises:
Create Date: 2024-01-01 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "0001_initial_edge_tables"
down_revision = None
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Create Edge3 tables."""
    # Create edge_worker table
    op.create_table(
        "edge_worker",
        sa.Column("worker_name", sa.String(64), nullable=False),
        sa.Column("state", sa.String(20), nullable=True),
        sa.Column("maintenance_comment", sa.String(1024), nullable=True),
        sa.Column("queues", sa.String(256), nullable=True),
        sa.Column("first_online", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_update", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("jobs_active", sa.Integer(), nullable=True, default=0),
        sa.Column("jobs_taken", sa.Integer(), nullable=True, default=0),
        sa.Column("jobs_success", sa.Integer(), nullable=True, default=0),
        sa.Column("jobs_failed", sa.Integer(), nullable=True, default=0),
        sa.Column("sysinfo", sa.String(256), nullable=True),
        sa.PrimaryKeyConstraint("worker_name", name=op.f("edge_worker_pkey")),
    )

    # Create edge_job table
    op.create_table(
        "edge_job",
        sa.Column("dag_id", sa.String(250), nullable=False),
        sa.Column("task_id", sa.String(250), nullable=False),
        sa.Column("run_id", sa.String(250), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False, server_default=sa.text("-1")),
        sa.Column("try_number", sa.Integer(), nullable=False, default=0),
        sa.Column("state", sa.String(20), nullable=True),
        sa.Column("queue", sa.String(256), nullable=True),
        sa.Column("concurrency_slots", sa.Integer(), nullable=True),
        sa.Column("command", sa.String(2048), nullable=True),
        sa.Column("queued_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("edge_worker", sa.String(64), nullable=True),
        sa.Column("last_update", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint(
            "dag_id", "task_id", "run_id", "map_index", "try_number", name=op.f("edge_job_pkey")
        ),
    )
    op.create_index("rj_order", "edge_job", ["state", "queued_dttm", "queue"], unique=False)

    # Create edge_logs table
    op.create_table(
        "edge_logs",
        sa.Column("dag_id", sa.String(250), nullable=False),
        sa.Column("task_id", sa.String(250), nullable=False),
        sa.Column("run_id", sa.String(250), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False, server_default=sa.text("-1")),
        sa.Column("try_number", sa.Integer(), nullable=False, default=0),
        sa.Column("log_chunk_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "log_chunk_data",
            sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            "try_number",
            "log_chunk_time",
            name=op.f("edge_logs_pkey"),
        ),
    )


def downgrade():
    """Drop Edge3 tables."""
    op.drop_table("edge_logs")
    op.drop_index("rj_order", table_name="edge_job")
    op.drop_table("edge_job")
    op.drop_table("edge_worker")
