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
Add backfill priority propagation.

Revision ID: 49f235e4e7cd
Revises: feb8b1e719ee
Create Date: 2026-11-26 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import sqlite_rebuilds

revision = "49f235e4e7cd"
down_revision = "feb8b1e719ee"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Add the priority propagation columns to backfill, dag_run and asset_event."""
    op.add_column(
        "backfill", sa.Column("propagate_priority", sa.Boolean(), nullable=False, server_default=sa.false())
    )
    op.add_column(
        "dag_run", sa.Column("backfill_derived", sa.Boolean(), nullable=False, server_default=sa.false())
    )
    op.add_column(
        "asset_event",
        sa.Column("backfill_derived", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("asset_event", sa.Column("origin_backfill_id", sa.Integer(), nullable=True))


def downgrade():
    """Remove the priority propagation columns from backfill, dag_run and asset_event."""
    with sqlite_rebuilds(op):
        with op.batch_alter_table("asset_event") as batch_op:
            batch_op.drop_column("origin_backfill_id")
            batch_op.drop_column("backfill_derived")
        with op.batch_alter_table("dag_run") as batch_op:
            batch_op.drop_column("backfill_derived")
        with op.batch_alter_table("backfill") as batch_op:
            batch_op.drop_column("propagate_priority")
