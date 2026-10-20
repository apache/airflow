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
Add inbound_wait table.

Revision ID: 7cd968ff1b8d
Revises: 303f314bf654
Create Date: 2026-10-20 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "7cd968ff1b8d"
down_revision = "303f314bf654"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Create inbound_wait table and index inbound_delivery for early-delivery lookups."""
    op.create_table(
        "inbound_wait",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("ti_id", sa.Uuid(), nullable=False),
        sa.Column("webhook", StringID(length=64), nullable=False),
        sa.Column("events", sa.JSON(), nullable=True),
        sa.Column("match", sa.JSON(), nullable=False),
        sa.Column("key_digest", sa.String(64), nullable=False),
        sa.Column("match_digest", sa.String(64), nullable=False),
        sa.Column("open", sa.Boolean(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("delivery_id", sa.Uuid(), nullable=True),
        sa.Column("deadline", UtcDateTime(timezone=True), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("ended_at", UtcDateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("inbound_wait_pkey")),
        sa.ForeignKeyConstraint(
            ["ti_id"],
            ["task_instance.id"],
            name="inbound_wait_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        sa.UniqueConstraint(
            "webhook",
            "match_digest",
            "open",
            name=op.f("inbound_wait_webhook_match_digest_open_uq"),
        ),
    )
    op.create_index(op.f("idx_inbound_wait_ti_id"), "inbound_wait", ["ti_id"])
    op.create_index(op.f("idx_inbound_wait_status_ti_id"), "inbound_wait", ["status", "ti_id"])
    op.create_index(op.f("idx_inbound_wait_deadline"), "inbound_wait", ["deadline"])
    op.create_index(
        op.f("idx_inbound_wait_webhook_open_key_digest"), "inbound_wait", ["webhook", "open", "key_digest"]
    )
    op.create_index(
        op.f("idx_inbound_delivery_webhook_outcome_received_at"),
        "inbound_delivery",
        ["webhook", "outcome", "received_at"],
    )


def downgrade():
    """Drop inbound_wait table and the early-delivery index on inbound_delivery."""
    op.drop_index(op.f("idx_inbound_delivery_webhook_outcome_received_at"), table_name="inbound_delivery")
    # Dropping the table drops its indexes. MySQL refuses to drop idx_inbound_wait_ti_id on its own,
    # because the foreign key needs it.
    op.drop_table("inbound_wait")
