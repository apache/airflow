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
Add dag_schedule_dataset_alias_reference table.

Revision ID: 22ed7efa9da2
Revises: 8684e37832e6
Create Date: 2024-08-05 08:41:47.696495

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

import airflow
from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "22ed7efa9da2"
down_revision = "8684e37832e6"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Add dag_schedule_dataset_alias_reference table."""
    op.create_table(
        "dag_schedule_dataset_alias_reference",
        sa.Column("alias_id", sa.Integer(), nullable=False),
        sa.Column("dag_id", StringID(), primary_key=True, nullable=False),
        sa.Column("created_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ("alias_id",),
            ["dataset_alias.id"],
            name="dsdar_dataset_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            columns=("dag_id",),
            refcolumns=["dag.dag_id"],
            name="dsdar_dag_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("alias_id", "dag_id", name="dsdar_pkey"),
    )
    op.create_index(
        "idx_dag_schedule_dataset_alias_reference_dag_id",
        "dag_schedule_dataset_alias_reference",
        ["dag_id"],
        unique=False,
    )


def downgrade():
    """Drop dag_schedule_dataset_alias_reference table."""
    op.drop_table("dag_schedule_dataset_alias_reference")
