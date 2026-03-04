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
Add connection_test table for async connection testing.

Revision ID: a7e6d4c3b2f1
<<<<<<<< HEAD:airflow-core/src/airflow/migrations/versions/0109_3_2_0_add_connection_test_table.py
Revises: 888b59e02a5b
========
Revises: 6222ce48e289
>>>>>>>> 189776ee4f (clean ups):airflow-core/src/airflow/migrations/versions/0108_3_2_0_add_connection_test_table.py
Create Date: 2026-02-22 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "a7e6d4c3b2f1"
<<<<<<<< HEAD:airflow-core/src/airflow/migrations/versions/0109_3_2_0_add_connection_test_table.py
down_revision = "888b59e02a5b"
========
down_revision = "6222ce48e289"
>>>>>>>> 189776ee4f (clean ups):airflow-core/src/airflow/migrations/versions/0108_3_2_0_add_connection_test_table.py
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
        sa.Column("executor", sa.String(256), nullable=True),
        sa.Column("queue", sa.String(256), nullable=True),
        sa.Column("connection_snapshot", sa.JSON(), nullable=True),
        sa.Column("reverted", sa.Boolean(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id", name=op.f("connection_test_pkey")),
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
