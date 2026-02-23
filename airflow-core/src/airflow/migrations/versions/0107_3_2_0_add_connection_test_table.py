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
Add connection_test table.

Revision ID: a7e6d4c3b2f1
Revises: 509b94a1042d
Create Date: 2026-02-22 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "a7e6d4c3b2f1"
down_revision = "509b94a1042d"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Create connection_test table."""
    op.create_table(
        "connection_test",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token", sa.String(64), nullable=False),
        sa.Column("connection_id", sa.String(250), nullable=False),
        sa.Column("state", sa.String(10), nullable=False),
        sa.Column("result_message", sa.Text(), nullable=True),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("callback_id", sa.Uuid(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("connection_test_pkey")),
        sa.ForeignKeyConstraint(
            ["callback_id"],
            ["callback.id"],
            name=op.f("connection_test_callback_id_fkey"),
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("token", name=op.f("connection_test_token_uq")),
    )
    op.create_index(
        op.f("idx_connection_test_state_created_at"),
        "connection_test",
        ["state", "created_at"],
    )


def downgrade():
    """Drop connection_test table."""
    op.drop_index(op.f("idx_connection_test_state_created_at"), table_name="connection_test")
    op.drop_table("connection_test")
