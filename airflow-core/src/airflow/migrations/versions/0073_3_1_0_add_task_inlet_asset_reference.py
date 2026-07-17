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
Add task_inlet_asset_reference table.

Revision ID: 583e80dfcef4
Revises: 3ac9e5732b1f
Create Date: 2025-06-04 06:26:36.536172
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, PrimaryKeyConstraint

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "583e80dfcef4"
down_revision = "3ac9e5732b1f"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add task_inlet_asset_reference table."""
    op.create_table(
        "task_inlet_asset_reference",
        Column("asset_id", Integer, primary_key=True, nullable=False),
        Column("dag_id", StringID(), primary_key=True, nullable=False),
        Column("task_id", StringID(), primary_key=True, nullable=False),
        Column("created_at", UtcDateTime, nullable=False),
        Column("updated_at", UtcDateTime, nullable=False),
        ForeignKeyConstraint(
            ["asset_id"],
            ["asset.id"],
            name="tiar_asset_fkey",
            ondelete="CASCADE",
        ),
        PrimaryKeyConstraint("asset_id", "dag_id", "task_id", name="tiar_pkey"),
        ForeignKeyConstraint(
            columns=["dag_id"],
            refcolumns=["dag.dag_id"],
            name="tiar_dag_id_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_task_inlet_asset_reference_dag_id", "dag_id"),
    )


def downgrade():
    """Remove task_inlet_asset_reference table."""
    op.drop_table("task_inlet_asset_reference")
