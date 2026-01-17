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
Add connection_test_request table for async connection testing on workers.

Revision ID: 9882c124ea54
Revises: e79fc784f145
Create Date: 2026-01-14 10:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "9882c124ea54"
down_revision = "e79fc784f145"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Create connection_test_request table."""
    op.create_table(
        "connection_test_request",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("state", sa.String(10), nullable=False),
        sa.Column("encrypted_connection_uri", sa.Text(), nullable=False),
        sa.Column("conn_type", sa.String(500), nullable=False),
        sa.Column("result_status", sa.Boolean(), nullable=True),
        sa.Column("result_message", sa.Text(), nullable=True),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("started_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("completed_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("timeout", sa.Integer(), nullable=False, default=60),
        sa.Column("worker_hostname", sa.String(500), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("connection_test_request_pkey")),
    )
    op.create_index(
        "idx_connection_test_state_created",
        "connection_test_request",
        ["state", "created_at"],
        unique=False,
    )


def downgrade():
    """Drop connection_test_request table."""
    op.drop_index("idx_connection_test_state_created", table_name="connection_test_request")
    op.drop_table("connection_test_request")
