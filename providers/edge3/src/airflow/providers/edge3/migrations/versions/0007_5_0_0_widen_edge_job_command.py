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
Change edge_job.command to unbounded text.

Revision ID: cd96a0a0e458
Revises: f2a4b6c8d0e1
Create Date: 2026-10-08 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "cd96a0a0e458"
down_revision = "f2a4b6c8d0e1"
branch_labels = None
depends_on = None
edge3_version = "5.0.0"


def upgrade() -> None:
    with op.batch_alter_table("edge_job") as batch_op:
        batch_op.alter_column(
            "command",
            existing_type=sa.String(length=2048),
            type_=sa.Text(),
            existing_nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("edge_job") as batch_op:
        batch_op.alter_column(
            "command",
            existing_type=sa.Text(),
            type_=sa.String(length=2048),
            existing_nullable=False,
        )
