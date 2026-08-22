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
Add selected_task_ids to backfill.

Adds a nullable JSON column to the backfill table holding the resolved task
ids a backfill is restricted to. A NULL value means the backfill covers the
whole Dag (the existing behaviour).

On PostgreSQL and MySQL 8+, adding a nullable column without a default is a
metadata-only operation (no table rewrite).

Revision ID: 8d1f4a2b6c37
Revises: c7f0a5d2e9b4
Create Date: 2026-08-22 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "8d1f4a2b6c37"
down_revision = "c7f0a5d2e9b4"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add selected_task_ids column to backfill table."""
    op.add_column("backfill", sa.Column("selected_task_ids", sa.JSON(), nullable=True))


def downgrade():
    """Remove selected_task_ids column from backfill table."""
    op.drop_column("backfill", "selected_task_ids")
