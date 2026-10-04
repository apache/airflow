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
Attribute log and asset events to exact task tries.

Revision ID: c3e7a9182f64
Revises: 7f8c9a2d410e
Create Date: 2026-09-28 01:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c3e7a9182f64"
down_revision = "7f8c9a2d410e"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Index log attribution and retain asset event attribution once the emitting try is archived."""
    op.create_index("idx_log_task_instance_id", "log", ["task_instance_id"])
    op.add_column("asset_event", sa.Column("source_task_instance_id", sa.Uuid(), nullable=True))
    op.create_index("idx_asset_event_source_ti", "asset_event", ["source_task_instance_id"])


def downgrade():
    """Remove exact attribution while retaining the original event coordinates."""
    op.drop_index("idx_asset_event_source_ti", table_name="asset_event")
    op.drop_column("asset_event", "source_task_instance_id")
    op.drop_index("idx_log_task_instance_id", table_name="log")
