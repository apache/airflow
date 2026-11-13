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
Preserve exact task execution attribution in asset state.

Revision ID: a5d7b9c13e40
Revises: c3e7a9182f64
Create Date: 2026-09-28 04:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import CompactUUID

revision = "a5d7b9c13e40"
down_revision = "c3e7a9182f64"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Keep writer identity after its task execution is archived."""
    op.add_column(
        "asset_state_store", sa.Column("last_updated_by_task_instance_id", sa.Uuid(), nullable=True)
    )
    op.add_column("asset_state_store", sa.Column("last_updated_by_region_id", CompactUUID(), nullable=True))
    op.add_column("asset_state_store", sa.Column("last_updated_by_region_index", sa.Integer(), nullable=True))
    op.add_column("asset_state_store", sa.Column("last_updated_by_try_number", sa.Integer(), nullable=True))


def downgrade():
    """Remove exact attribution without changing asset state values."""
    op.drop_column("asset_state_store", "last_updated_by_try_number")
    op.drop_column("asset_state_store", "last_updated_by_region_index")
    op.drop_column("asset_state_store", "last_updated_by_region_id")
    op.drop_column("asset_state_store", "last_updated_by_task_instance_id")
