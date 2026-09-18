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
Add priority_weight column to edge_job table.

Revision ID: d5a2f8b41c07
Revises: c6b3c3d093fd
Create Date: 2026-08-25 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d5a2f8b41c07"
down_revision = "c6b3c3d093fd"
branch_labels = None
depends_on = None
edge3_version = "4.4.0"

NEW_INDEX_COLUMNS: list[str | sa.TextClause] = [
    "state",
    sa.text("priority_weight DESC"),
    "queued_dttm",
    "queue",
]
OLD_INDEX_COLUMNS: list[str | sa.TextClause] = ["state", "queued_dttm", "queue"]


def _recreate_rj_order(columns: list[str | sa.TextClause]) -> None:
    inspector = sa.inspect(op.get_bind())
    if "rj_order" in {idx["name"] for idx in inspector.get_indexes("edge_job")}:
        op.drop_index("rj_order", table_name="edge_job")
    op.create_index("rj_order", "edge_job", columns)


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "priority_weight" not in {c["name"] for c in inspector.get_columns("edge_job")}:
        with op.batch_alter_table("edge_job", schema=None) as batch_op:
            batch_op.add_column(
                sa.Column("priority_weight", sa.Integer(), server_default=sa.text("1"), nullable=False)
            )
    _recreate_rj_order(NEW_INDEX_COLUMNS)


def downgrade() -> None:
    _recreate_rj_order(OLD_INDEX_COLUMNS)
    with op.batch_alter_table("edge_job", schema=None) as batch_op:
        batch_op.drop_column("priority_weight")
