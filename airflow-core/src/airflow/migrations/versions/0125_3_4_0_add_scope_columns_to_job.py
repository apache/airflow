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
Add team_name and bundle_names scope columns to job table.

Revision ID: f8c2a1d94e03
Revises: 5a5d3253e946
Create Date: 2026-07-08 02:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "f8c2a1d94e03"
down_revision = "5a5d3253e946"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add triggerer team and dag-processor bundle scope columns to job table."""
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.add_column(sa.Column("team_name", sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column("bundle_names", ExtendedJSON(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("job_team_name_fkey"), "team", ["team_name"], ["name"], ondelete="SET NULL"
        )


def downgrade():
    """Remove scope columns from job table."""
    with op.batch_alter_table("job", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("job_team_name_fkey"), type_="foreignkey")
        batch_op.drop_column("bundle_names")
        batch_op.drop_column("team_name")
