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
Add keep_dag_paused column to backfill table.

Revision ID: a1b2c3d4e5f6
Revises: 0099_3_2_0_ui_improvements_for_deadlines
Create Date: 2026-01-20 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "0099_3_2_0_ui_improvements_for_deadlines"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add keep_dag_paused column to backfill table."""
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("keep_dag_paused", sa.Boolean(), nullable=False, server_default="0")
        )


def downgrade():
    """Remove keep_dag_paused column from backfill table."""
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.drop_column("keep_dag_paused")
