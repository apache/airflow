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
Add dag_cancel_condition table.

Revision ID: 1d92505f2d67
Revises: 7cd968ff1b8d
Create Date: 2026-10-23 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "1d92505f2d67"
down_revision = "7cd968ff1b8d"
branch_labels = None
depends_on = None
airflow_version = "3.6.0"


def upgrade():
    """Create dag_cancel_condition table."""
    op.create_table(
        "dag_cancel_condition",
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("webhook", StringID(length=64), nullable=False),
        sa.Column("condition_digest", sa.String(64), nullable=False),
        sa.Column("events", sa.JSON(), nullable=True),
        sa.Column("match", sa.JSON(), nullable=False),
        sa.Column("correlation_key_conf", sa.String(250), nullable=True),
        sa.Column("cancel_state", sa.String(20), nullable=True),
        sa.PrimaryKeyConstraint(
            "dag_id", "task_id", "webhook", "condition_digest", name=op.f("dag_cancel_condition_pkey")
        ),
        sa.ForeignKeyConstraint(
            ["dag_id"],
            ["dag.dag_id"],
            name=op.f("dag_cancel_condition_dag_id_fkey"),
            ondelete="CASCADE",
        ),
    )
    op.create_index(op.f("idx_dag_cancel_condition_webhook"), "dag_cancel_condition", ["webhook"])


def downgrade():
    """Drop dag_cancel_condition table."""
    # Dropping the table drops its index, and its primary key leads with dag_id, so MySQL needs no
    # separate index for the foreign key.
    op.drop_table("dag_cancel_condition")
