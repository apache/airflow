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
Add GIN index on asset_event.extra for PostgreSQL.

Revision ID: 5a5d3253e946
Revises: d2f4e1b3c5a7
Create Date: 2026-04-01 23:00:00.000000

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "5a5d3253e946"
down_revision = "d2f4e1b3c5a7"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add GIN index on asset_event.extra for PostgreSQL only."""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        op.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_asset_event_extra_gin "
                "ON asset_event USING GIN ((extra::jsonb) jsonb_ops)"
            )
        )


def downgrade():
    """Remove GIN index on asset_event.extra."""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        op.execute(text("DROP INDEX IF EXISTS idx_asset_event_extra_gin"))
