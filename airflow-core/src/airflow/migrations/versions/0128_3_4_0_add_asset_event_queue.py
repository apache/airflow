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
Add asset_event_queue table.

Revision ID: a63ae7baae97
Revises: 7a98f1b7dbd3
Create Date: 2026-07-15 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "a63ae7baae97"
down_revision = "7a98f1b7dbd3"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply Add asset_event_queue table."""
    op.create_table(
        "asset_event_queue",
        sa.Column("ti_id", sa.Uuid(), nullable=False),
        # Emitted task outlets and outlet events are stored together in a single JSON payload. The
        # queue is a durable buffer only ever read back in full by the scheduler drain, so separate
        # columns buy nothing and a single write keeps the enqueue on the task-success path cheap.
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("ti_id", name="asset_event_queue_pkey"),
        sa.ForeignKeyConstraint(
            ["ti_id"],
            ["task_instance.id"],
            name="aeq_ti_fkey",
            ondelete="CASCADE",
            # Cascade a TI id change (clear/retry reassigns a new uuid7) so pending asset events
            # follow the task instance instead of being orphaned and dropped by the scheduler drain.
            onupdate="CASCADE",
        ),
    )
    with op.batch_alter_table("asset_event_queue", schema=None) as batch_op:
        batch_op.create_index("idx_asset_event_queue_created_at", ["created_at"], unique=False)


def downgrade():
    """Unapply Add asset_event_queue table."""
    with op.batch_alter_table("asset_event_queue", schema=None) as batch_op:
        batch_op.drop_index("idx_asset_event_queue_created_at")
    op.drop_table("asset_event_queue")
