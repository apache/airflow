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
Add triggered_at to dag_run table.

Revision ID: cd799060f00b
Revises: 888b59e02a5b
Create Date: 2026-03-10 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

revision = "cd799060f00b"
down_revision = "888b59e02a5b"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add triggered_at column to dag_run table."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "triggered_at",
                UtcDateTime,
                nullable=True,
            )
        )
    # Back-fill existing rows using created_at as a reasonable approximation.
    op.execute("UPDATE dag_run SET triggered_at = created_at WHERE triggered_at IS NULL")
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.alter_column("triggered_at", existing_type=UtcDateTime, nullable=False)


def downgrade():
    """Remove triggered_at column from dag_run table."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("triggered_at")
