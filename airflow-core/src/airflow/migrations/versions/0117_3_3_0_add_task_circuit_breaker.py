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
Add task_circuit_breaker table.

Tracks per-task circuit breaker state: failure counts within a rolling window
and whether the circuit is currently open (blocking scheduling).

Revision ID: a10edcba2695
Revises: acc215baed80
Create Date: 2026-05-27 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a10edcba2695"
down_revision = "acc215baed80"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Create task_circuit_breaker table."""
    op.create_table(
        "task_circuit_breaker",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("is_open", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("opened_at", sa.DateTime(), nullable=True),
        sa.Column("opened_reason", sa.String(length=500), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("window_start", sa.DateTime(), nullable=True),
        sa.Column("reset_after", sa.DateTime(), nullable=True),
        sa.Column("max_failures", sa.Integer(), nullable=False),
        sa.Column("window_seconds", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("dag_id", "task_id", name="task_circuit_breaker_pkey"),
    )
    op.create_index("idx_tcb_open", "task_circuit_breaker", ["is_open"])


def downgrade():
    """Drop task_circuit_breaker table."""
    op.drop_index("idx_tcb_open", table_name="task_circuit_breaker")
    op.drop_table("task_circuit_breaker")
