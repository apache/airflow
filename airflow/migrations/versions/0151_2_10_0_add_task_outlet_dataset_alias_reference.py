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
Add task_outlet_dataset_alias_reference table.

Revision ID: 5e90e837334a
Revises: 8684e37832e6
Create Date: 2024-07-29 13:58:59.871047

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

import airflow

# revision identifiers, used by Alembic.
revision = "5e90e837334a"
down_revision = "8684e37832e6"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Add task_outlet_dataset_alias_reference table."""
    op.create_table(
        "task_outlet_dataset_alias_reference",
        sa.Column("dataset_alias_id", sa.Integer(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("created_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dag_id"], ["dag.dag_id"], name="todar_dag_id_fkey", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["dataset_alias_id"], ["dataset_alias.id"], name="todar_dataset_alias_fkey", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("dataset_alias_id", "dag_id", "task_id", name="todar_pkey"),
    )
    op.create_index(
        "idx_task_outlet_dataset_alias_reference_dag_id",
        "task_outlet_dataset_alias_reference",
        ["dag_id"],
        unique=False,
    )


def downgrade():
    """Remove task_outlet_dataset_alias_reference table."""
    op.drop_table("task_outlet_dataset_alias_reference")
