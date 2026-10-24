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
Add webhook_cancel_request table.

Revision ID: 1599e4fa307b
Revises: 1d92505f2d67
Create Date: 2026-10-23 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "1599e4fa307b"
down_revision = "1d92505f2d67"
branch_labels = None
depends_on = None
airflow_version = "3.6.0"


def upgrade():
    """Create webhook_cancel_request table."""
    op.create_table(
        "webhook_cancel_request",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("inbound_delivery_id", sa.Uuid(), nullable=False),
        sa.Column("delivery_id", StringID(length=255), nullable=True),
        sa.Column("webhook", StringID(length=64), nullable=False),
        sa.Column("correlation_key", sa.String(255), nullable=False),
        sa.Column("received_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("targets", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("applied_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("retain_until", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("webhook_cancel_request_pkey")),
        sa.ForeignKeyConstraint(
            ["dag_id"],
            ["dag.dag_id"],
            name=op.f("webhook_cancel_request_dag_id_fkey"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "inbound_delivery_id",
            "dag_id",
            name=op.f("webhook_cancel_request_inbound_delivery_id_dag_id_uq"),
        ),
    )
    op.create_index(
        op.f("idx_webhook_cancel_request_status_received_at"),
        "webhook_cancel_request",
        ["status", "received_at"],
    )
    op.create_index(
        op.f("idx_webhook_cancel_request_dag_id_correlation_key"),
        "webhook_cancel_request",
        ["dag_id", "correlation_key"],
    )
    op.create_index(
        op.f("idx_webhook_cancel_request_retain_until"), "webhook_cancel_request", ["retain_until"]
    )


def downgrade():
    """Drop webhook_cancel_request table."""
    # Dropping the table drops its indexes. MySQL refuses to drop
    # idx_webhook_cancel_request_dag_id_correlation_key on its own, because the foreign key needs it.
    op.drop_table("webhook_cancel_request")
