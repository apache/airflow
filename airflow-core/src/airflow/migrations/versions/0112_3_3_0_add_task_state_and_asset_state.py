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
Add task_state and asset_state tables.

Revision ID: 8a7b3c2d1e4f
Revises: 9fabad868fdb
Create Date: 2026-04-24 00:00:00.000000

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import Column, ForeignKeyConstraint, Integer, String, Text

from airflow._shared.timezones import timezone
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "8a7b3c2d1e4f"
down_revision = "9fabad868fdb"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add task_state and asset_state tables."""
    op.create_table(
        "task_state",
        Column("dag_run_id", Integer, nullable=False, primary_key=True),
        Column("task_id", String(length=250), nullable=False, primary_key=True),
        Column("map_index", Integer, nullable=False, primary_key=True, server_default="-1"),
        Column("key", String(length=512), nullable=False, primary_key=True),
        Column("dag_id", String(length=250), nullable=False),
        Column("run_id", String(length=250), nullable=False),
        Column("value", Text, nullable=False),
        Column("updated_at", UtcDateTime, nullable=False, default=timezone.utcnow),
        Column("updated_by_try", Integer, nullable=True),
        ForeignKeyConstraint(
            ["dag_run_id"],
            ["dag_run.id"],
            name="task_state_dag_run_fkey",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_task_state_lookup",
        "task_state",
        ["dag_id", "run_id", "task_id", "map_index", "key"],
    )

    op.create_table(
        "asset_state",
        Column("asset_id", Integer, nullable=False, primary_key=True),
        Column("key", String(length=512), nullable=False, primary_key=True),
        Column("value", Text, nullable=False),
        Column("updated_at", UtcDateTime, nullable=False, default=timezone.utcnow),
        ForeignKeyConstraint(
            ["asset_id"],
            ["asset.id"],
            name="asset_state_asset_fkey",
            ondelete="CASCADE",
        ),
    )


def downgrade():
    """Remove task_state and asset_state tables."""
    op.drop_index("idx_task_state_lookup", table_name="task_state")
    op.drop_table("task_state")
    op.drop_table("asset_state")
