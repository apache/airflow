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
Update ORM for asset partitioning.

Revision ID: 665854ef0536
Revises: e812941398f4
Create Date: 2025-10-14 10:27:04.345130

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

revision = "665854ef0536"
down_revision = "e812941398f4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Update ORM for asset partitioning."""
    op.create_table(
        "partitioned_asset_key_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("asset_event_id", sa.Integer(), nullable=False),
        sa.Column("asset_partition_dag_run_id", sa.Integer(), nullable=False),
        sa.Column("source_partition_key", StringID(), nullable=False),
        sa.Column("target_dag_id", StringID(), nullable=False),
        sa.Column("target_partition_key", StringID(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("partitioned_asset_key_log_pkey")),
    )
    op.create_table(
        "asset_partition_dag_run",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("target_dag_id", StringID(), nullable=False),
        sa.Column("created_dag_run_id", sa.Integer(), nullable=True),
        sa.Column("partition_key", StringID(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("asset_partition_dag_run_pkey")),
        sa.ForeignKeyConstraint(
            columns=("created_dag_run_id",),
            refcolumns=["dag_run.id"],
            name="apdr_created_dag_run_id_fkey",
            ondelete="CASCADE",
        ),
    )

    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        batch_op.add_column(sa.Column("partition_key", StringID(), nullable=True))

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("partition_key", StringID(), nullable=True))


def downgrade():
    """Unapply Update ORM for asset partitioning."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("partition_key")

    with op.batch_alter_table("asset_event", schema=None) as batch_op:
        batch_op.drop_column("partition_key")

    op.drop_table("partitioned_asset_key_log")
    op.drop_table("asset_partition_dag_run")
