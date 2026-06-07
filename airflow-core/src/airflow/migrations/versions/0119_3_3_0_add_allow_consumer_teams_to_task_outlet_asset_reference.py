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
Add allow_consumer_teams columns to task_outlet_asset_reference table.

Revision ID: c9d4e5f6a7b8
Revises: a7e6d4c3b2f1
Create Date: 2026-05-29 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c9d4e5f6a7b8"
down_revision = "a7e6d4c3b2f1"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add consumer-team access control columns to task_outlet_asset_reference."""
    with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
        batch_op.add_column(sa.Column("allow_consumer_teams", sa.JSON(), nullable=True))
        batch_op.add_column(
            sa.Column("allow_global_consumers", sa.Boolean(), nullable=False, server_default=sa.true())
        )


def downgrade():
    """Remove consumer-team access control columns from task_outlet_asset_reference."""
    from airflow.migrations.utils import disable_sqlite_fkeys

    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("task_outlet_asset_reference", schema=None) as batch_op:
            batch_op.drop_column("allow_global_consumers")
            batch_op.drop_column("allow_consumer_teams")
