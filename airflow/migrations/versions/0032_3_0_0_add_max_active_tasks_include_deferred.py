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
Add ``max_active_tasks_include_deferred`` column to DAG table.

Revision ID: 993df96ee59b
Revises: 1cdc775ca98f
Create Date: 2024-08-19 22:12:09.345943

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "993df96ee59b"
down_revision = "1cdc775ca98f"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply add max_active_tasks_include_deferred column to DAG table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "max_active_tasks_include_deferred", sa.Boolean(), nullable=False, server_default=sa.false()
            )
        )


def downgrade():
    """Unapply add max_active_tasks_include_deferred column to DAG table."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("max_active_tasks_include_deferred")
