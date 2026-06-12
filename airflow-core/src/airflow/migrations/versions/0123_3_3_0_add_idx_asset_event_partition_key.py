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
Add index on asset_event (asset_id, partition_key).

Revision ID: 7a98f1b7dbd3
Revises: 9ff64e1c35d3
Create Date: 2026-04-01 00:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "7a98f1b7dbd3"
down_revision = "9ff64e1c35d3"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Apply Add index on asset_event (asset_id, partition_key)."""
    op.create_index(
        "idx_asset_event_asset_id_partition_key",
        "asset_event",
        ["asset_id", "partition_key"],
    )


def downgrade():
    """Unapply Add index on asset_event (asset_id, partition_key)."""
    op.drop_index("idx_asset_event_asset_id_partition_key", table_name="asset_event")
