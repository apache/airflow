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
Add references between assets and triggers.

Revision ID: 9fc3fc5de720
Revises: 2b47dc6bc8df
Create Date: 2024-11-04 16:40:59.927266

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Integer

# revision identifiers, used by Alembic.
revision = "9fc3fc5de720"
down_revision = "2b47dc6bc8df"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    op.create_table(
        "asset_trigger",
        sa.Column("trigger_id", Integer, primary_key=True, nullable=False),
        sa.Column("asset_id", Integer, primary_key=True, nullable=False),
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


def downgrade():
    op.drop_table("asset_trigger")
