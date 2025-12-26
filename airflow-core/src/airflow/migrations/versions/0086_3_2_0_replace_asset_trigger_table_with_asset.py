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
replace asset_trigger table with asset_watcher.

Revision ID: 15d84ca19038
Revises: cc92b33c6709
Create Date: 2025-09-14 01:34:40.423767

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "15d84ca19038"
down_revision = "cc92b33c6709"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"

_STRING_COLUMN_TYPE = sa.String(length=1500).with_variant(
    sa.String(length=1500, collation="latin1_general_cs"),
    "mysql",
)


def upgrade():
    """Apply replace asset_trigger table with asset_watcher."""
    # Drop the old asset_trigger table if it exists - it's no longer used by the new code
    # The asset_watcher table will be populated automatically by the DAG parsing process
    # Note: asset_trigger was introduced in Airflow 3.0.0, so it might not exist for older upgrades
    op.drop_table("asset_trigger", if_exists=True)

    # Create the new asset_watcher table
    op.create_table(
        "asset_watcher",
        sa.Column(
            "name",
            _STRING_COLUMN_TYPE,
            nullable=False,
        ),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("trigger_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            columns=("asset_id",),
            refcolumns=["asset.id"],
            name="awm_asset_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            columns=("trigger_id",),
            refcolumns=["trigger.id"],
            name="awm_trigger_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("asset_id", "trigger_id", name="asset_watcher_pkey"),
    )

    op.create_index("idx_awm_trigger_id", "asset_watcher", ["trigger_id"])


def downgrade():
    """Unapply replace asset_trigger table with asset_watcher."""
    op.drop_table("asset_watcher")

    # Recreate the old asset_trigger table for downgrade
    op.create_table(
        "asset_trigger",
        sa.Column("trigger_id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("asset_id", sa.Integer(), primary_key=True, nullable=False),
        sa.ForeignKeyConstraint(
            columns=("trigger_id",),
            refcolumns=["trigger.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("asset_id",),
            ["asset.id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index("idx_asset_trigger_asset_id", "asset_trigger", ["asset_id"])
    op.create_index("idx_asset_trigger_trigger_id", "asset_trigger", ["trigger_id"])
