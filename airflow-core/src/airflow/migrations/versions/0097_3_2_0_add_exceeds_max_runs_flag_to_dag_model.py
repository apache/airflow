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
Add exceeds max runs flag to dag model.

Revision ID: 0b112f49112d
Revises: c47f2e1ab9d4
Create Date: 2025-12-31 13:40:50.550261

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0b112f49112d"
down_revision = "c47f2e1ab9d4"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Apply Add exceeds max runs flag to dag model."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "exceeds_max_non_backfill",
                sa.Boolean(),
                server_default="0",
                nullable=False,
            )
        )


def downgrade():
    """Unapply Add exceeds max runs flag to dag model."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_column("exceeds_max_non_backfill")
