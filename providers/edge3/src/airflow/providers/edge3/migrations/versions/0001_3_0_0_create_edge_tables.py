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
Create Edge tables if missing.

Revision ID: 9d34dfc2de06
Revises:
Create Date: 2025-06-01 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9d34dfc2de06"
down_revision = None
branch_labels = None
depends_on = None
edge3_version = "3.0.0"


def upgrade() -> None:
    op.create_table(
        "edge_worker",
        sa.Column("worker_name", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("maintenance_comment", sa.String(length=1024), nullable=True),
        sa.Column("queues", sa.String(length=256), nullable=True),
        sa.Column("first_online", sa.DateTime(), nullable=True),
        sa.Column("last_update", sa.DateTime(), nullable=True),
        sa.Column("jobs_active", sa.Integer(), nullable=False),
        sa.Column("jobs_taken", sa.Integer(), nullable=False),
        sa.Column("jobs_success", sa.Integer(), nullable=False),
        sa.Column("jobs_failed", sa.Integer(), nullable=False),
        sa.Column("sysinfo", sa.String(length=256), nullable=True),
        sa.PrimaryKeyConstraint("worker_name"),
        if_not_exists=True,
    )
    op.create_table(
        "edge_job",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default=sa.text("-1"), nullable=False),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("queue", sa.String(length=256), nullable=False),
        sa.Column("concurrency_slots", sa.Integer(), nullable=False),
        sa.Column("command", sa.String(length=2048), nullable=False),
        sa.Column("queued_dttm", sa.DateTime(), nullable=True),
        sa.Column("edge_worker", sa.String(length=64), nullable=True),
        sa.Column("last_update", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("dag_id", "task_id", "run_id", "map_index", "try_number"),
        if_not_exists=True,
    )
    op.create_index("rj_order", "edge_job", ["state", "queued_dttm", "queue"], if_not_exists=True)
    op.create_table(
        "edge_logs",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default=sa.text("-1"), nullable=False),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("log_chunk_time", sa.DateTime(), nullable=False),
        sa.Column("log_chunk_data", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("dag_id", "task_id", "run_id", "map_index", "try_number", "log_chunk_time"),
        if_not_exists=True,
    )


def downgrade() -> None: ...
