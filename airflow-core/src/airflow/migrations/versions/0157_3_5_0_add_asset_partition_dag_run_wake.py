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
Add asset_partition_dag_run_wake.

Revision ID: 6b0e4c1f2a37
Revises: d8c6f3a129b0
Create Date: 2026-11-08 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.models.base import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "6b0e4c1f2a37"
down_revision = "d8c6f3a129b0"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the table through which writers ask the scheduler to evaluate pending partition Dag runs."""
    op.create_table(
        "asset_partition_dag_run_wake",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("apdr_id", sa.Integer(), nullable=True),
        sa.Column("target_dag_id", StringID(), nullable=False),
        sa.Column("reason", sa.String(length=20), nullable=False),
        sa.Column("created_at", UtcDateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("asset_partition_dag_run_wake_pkey")),
        sqlite_autoincrement=True,
    )
    op.create_index("idx_apdr_wake_apdr_id", "asset_partition_dag_run_wake", ["apdr_id"])
    op.create_index(
        "idx_apdr_wake_target_dag_id", "asset_partition_dag_run_wake", ["target_dag_id", "apdr_id"]
    )


def downgrade():
    """Drop asset_partition_dag_run_wake."""
    op.drop_table("asset_partition_dag_run_wake")
