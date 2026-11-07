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
Add origin and idempotency fields to asset events.

Revision ID: 6b008b64b02c
Revises: d8c6f3a129b0
Create Date: 2026-11-07 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

revision = "6b008b64b02c"
down_revision = "d8c6f3a129b0"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Record where each event came from, and the key that makes an emission repeatable."""
    with op.batch_alter_table("asset_event") as batch_op:
        batch_op.add_column(sa.Column("origin", sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column("idempotency_key", StringID(), nullable=True))
        batch_op.add_column(sa.Column("scope_hash", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("payload_hash", sa.String(length=64), nullable=True))
        batch_op.create_index("idx_asset_event_scope_hash", ["scope_hash"], unique=True)


def downgrade():
    """Remove the origin and idempotency fields from asset events."""
    with op.batch_alter_table("asset_event") as batch_op:
        batch_op.drop_index("idx_asset_event_scope_hash")
        batch_op.drop_column("payload_hash")
        batch_op.drop_column("scope_hash")
        batch_op.drop_column("idempotency_key")
        batch_op.drop_column("origin")
