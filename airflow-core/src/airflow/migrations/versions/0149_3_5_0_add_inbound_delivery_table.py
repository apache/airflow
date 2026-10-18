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
Add inbound_delivery table.

Revision ID: 303f314bf654
Revises: c71065a1b14f
Create Date: 2026-10-18 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "303f314bf654"
down_revision = "c71065a1b14f"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"


def upgrade():
    """Create inbound_delivery table."""
    op.create_table(
        "inbound_delivery",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_kind", sa.String(16), nullable=False),
        sa.Column("webhook", StringID(length=64), nullable=False),
        sa.Column("delivery_id", StringID(length=255), nullable=True),
        sa.Column("body_digest", sa.String(64), nullable=False),
        sa.Column("received_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("retain_until", UtcDateTime(timezone=True), nullable=False),
        sa.Column("source_type", sa.String(64), nullable=False),
        sa.Column("attributes", sa.JSON(), nullable=True),
        sa.Column("candidates", sa.JSON(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("correlation_key", sa.String(255), nullable=True),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("actions", sa.JSON(), nullable=True),
        sa.Column("team_name", sa.String(50), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("inbound_delivery_pkey")),
        sa.UniqueConstraint(
            "webhook",
            "delivery_id",
            "body_digest",
            name=op.f("inbound_delivery_webhook_delivery_id_body_digest_uq"),
        ),
    )
    op.create_index(
        op.f("idx_inbound_delivery_webhook_received_at"),
        "inbound_delivery",
        ["webhook", "received_at"],
    )
    op.create_index(op.f("idx_inbound_delivery_retain_until"), "inbound_delivery", ["retain_until"])
    op.create_index(op.f("idx_inbound_delivery_correlation_key"), "inbound_delivery", ["correlation_key"])


def downgrade():
    """Drop inbound_delivery table."""
    op.drop_index(op.f("idx_inbound_delivery_correlation_key"), table_name="inbound_delivery")
    op.drop_index(op.f("idx_inbound_delivery_retain_until"), table_name="inbound_delivery")
    op.drop_index(op.f("idx_inbound_delivery_webhook_received_at"), table_name="inbound_delivery")
    op.drop_table("inbound_delivery")
