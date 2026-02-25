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
Add team_name column to edge_job and edge_worker tables.

Revision ID: a09c3ee8e1d3
Revises: 9d34dfc2de06
Create Date: 2026-02-07 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a09c3ee8e1d3"
down_revision = "9d34dfc2de06"
branch_labels = None
depends_on = None
edge3_version = "3.1.0"


def upgrade() -> None:
    with op.batch_alter_table("edge_job") as batch_op:
        batch_op.add_column(sa.Column("team_name", sa.String(length=64), nullable=True))

    with op.batch_alter_table("edge_worker") as batch_op:
        batch_op.add_column(sa.Column("team_name", sa.String(length=64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("edge_worker") as batch_op:
        batch_op.drop_column("team_name")

    with op.batch_alter_table("edge_job") as batch_op:
        batch_op.drop_column("team_name")
