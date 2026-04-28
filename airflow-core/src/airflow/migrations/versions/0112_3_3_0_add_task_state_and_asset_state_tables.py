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

Revision ID: fde9ed84d07b
Revises: 9fabad868fdb
Create Date: 2026-04-27 09:29:20.165461

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "fde9ed84d07b"
down_revision = "9fabad868fdb"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Apply add task_state and asset_state tables."""
    op.create_table(
        "asset_state",
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("key", sa.String(length=512), nullable=False),
        sa.Column("value", sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"), nullable=False),
        sa.Column("updated_at", UtcDateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["asset_id"], ["asset.id"], name="asset_state_asset_fkey", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("asset_id", "key", name="asset_state_pkey"),
    )
    op.create_table(
        "task_state",
        sa.Column("dag_run_id", sa.Integer(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("key", sa.String(length=512), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("run_id", StringID(), nullable=False),
        sa.Column("value", sa.Text().with_variant(mysql.MEDIUMTEXT(), "mysql"), nullable=False),
        sa.Column("updated_at", UtcDateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dag_run_id"], ["dag_run.id"], name="task_state_dag_run_fkey", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("dag_run_id", "task_id", "map_index", "key", name="task_state_pkey"),
    )
    with op.batch_alter_table("task_state", schema=None) as batch_op:
        batch_op.create_index(
            "idx_task_state_lookup", ["dag_id", "run_id", "task_id", "map_index"], unique=False
        )


def downgrade():
    """Unapply add task_state and asset_state tables."""
    with op.batch_alter_table("task_state", schema=None) as batch_op:
        batch_op.drop_index("idx_task_state_lookup")

    op.drop_table("task_state")
    op.drop_table("asset_state")
