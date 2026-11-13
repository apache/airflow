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
Add asset deadline support to deadline and deadline_alert tables.

Revision ID: a4f19e82c301
Revises: d8c6f3a129b0
Create Date: 2026-11-13 10:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "a4f19e82c301"
down_revision = "d8c6f3a129b0"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add asset_id, partition_key, cron, and is_producer columns for Asset Deadlines."""
    with disable_sqlite_fkeys():
        with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
            batch_op.alter_column("serialized_dag_id", existing_type=sa.Uuid(), nullable=True)
            batch_op.alter_column("reference", existing_type=sa.JSON(), nullable=True)
            batch_op.alter_column("interval", existing_type=sa.JSON(), nullable=True)
            batch_op.add_column(sa.Column("asset_id", sa.Integer(), nullable=True))
            batch_op.add_column(
                sa.Column("is_producer", sa.Boolean(), nullable=False, server_default=sa.false())
            )
            batch_op.add_column(sa.Column("cron", sa.String(length=250), nullable=True))
            batch_op.create_foreign_key(
                batch_op.f("deadline_alert_asset_id_fkey"),
                "asset",
                ["asset_id"],
                ["id"],
                ondelete="CASCADE",
            )

        with op.batch_alter_table("deadline", schema=None) as batch_op:
            batch_op.add_column(sa.Column("asset_id", sa.Integer(), nullable=True))
            batch_op.add_column(sa.Column("partition_key", sa.String(length=250), nullable=True))
            batch_op.create_foreign_key(
                batch_op.f("deadline_asset_id_fkey"),
                "asset",
                ["asset_id"],
                ["id"],
                ondelete="CASCADE",
            )
            batch_op.create_index(
                "deadline_asset_id_partition_idx", ["asset_id", "partition_key"], unique=False
            )


def downgrade():
    """Remove asset deadline columns from deadline and deadline_alert tables."""
    with disable_sqlite_fkeys():
        with op.batch_alter_table("deadline", schema=None) as batch_op:
            batch_op.drop_index("deadline_asset_id_partition_idx")
            batch_op.drop_constraint(batch_op.f("deadline_asset_id_fkey"), type_="foreignkey")
            batch_op.drop_column("partition_key")
            batch_op.drop_column("asset_id")

        with op.batch_alter_table("deadline_alert", schema=None) as batch_op:
            batch_op.drop_constraint(batch_op.f("deadline_alert_asset_id_fkey"), type_="foreignkey")
            batch_op.drop_column("cron")
            batch_op.drop_column("is_producer")
            batch_op.drop_column("asset_id")
            batch_op.alter_column("interval", existing_type=sa.JSON(), nullable=False)
            batch_op.alter_column("reference", existing_type=sa.JSON(), nullable=False)
            batch_op.alter_column("serialized_dag_id", existing_type=sa.Uuid(), nullable=False)
