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
Add concurrency column to edge_worker table.

Revision ID: b3c4d5e6f7a8
Revises: 9d34dfc2de06
Create Date: 2026-03-04 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b3c4d5e6f7a8"
down_revision = "9d34dfc2de06"
branch_labels = None
depends_on = None
edge3_version = "3.2.0"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = {col["name"] for col in inspector.get_columns("edge_worker")}
    if "concurrency" not in existing_columns:
        op.add_column("edge_worker", sa.Column("concurrency", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("edge_worker", "concurrency")
