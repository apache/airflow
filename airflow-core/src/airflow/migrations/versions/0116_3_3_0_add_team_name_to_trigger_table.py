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
Add team_name to trigger table.

Revision ID: acc215baed80
Revises: a1b2c3d4e5f6
Create Date: 2026-05-21 21:38:00.122692

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "acc215baed80"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add team_name to trigger table."""
    with op.batch_alter_table("trigger", schema=None) as batch_op:
        batch_op.add_column(sa.Column("team_name", sa.String(length=50), nullable=True))
        batch_op.create_index(batch_op.f("idx_trigger_team_name"), ["team_name"], unique=False)
        batch_op.create_foreign_key(
            batch_op.f("trigger_team_name_fkey"), "team", ["team_name"], ["name"], ondelete="SET NULL"
        )


def downgrade():
    """Remove team_name from trigger table."""
    with op.batch_alter_table("trigger", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("trigger_team_name_fkey"), type_="foreignkey")
        batch_op.drop_index(batch_op.f("idx_trigger_team_name"))
        batch_op.drop_column("team_name")
