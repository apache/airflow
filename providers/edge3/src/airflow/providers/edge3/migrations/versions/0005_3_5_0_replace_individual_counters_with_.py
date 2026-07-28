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
Replace individual counters with extended JSON based sysinfo.

Revision ID: c6b3c3d093fd
Revises: a09c3ee8e1d3
Create Date: 2026-04-15 21:57:07.662359
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c6b3c3d093fd"
down_revision = "a09c3ee8e1d3"
branch_labels = None
depends_on = None
edge3_version = "3.5.0"


def upgrade() -> None:
    with op.batch_alter_table("edge_worker", schema=None) as batch_op:
        # Can not easuly convert old sysinfo string to new JSON structure, just clear and re-populate by workers on next heartbeat
        batch_op.drop_column("sysinfo")
        batch_op.add_column(sa.Column("sysinfo", sa.JSON(), nullable=True))
        batch_op.drop_column("jobs_failed")
        batch_op.drop_column("jobs_taken")
        batch_op.drop_column("jobs_success")


def downgrade() -> None:
    with op.batch_alter_table("edge_worker", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("jobs_success", sa.INTEGER(), autoincrement=False, default=0, nullable=False)
        )
        batch_op.add_column(
            sa.Column("jobs_taken", sa.INTEGER(), autoincrement=False, default=0, nullable=False)
        )
        batch_op.add_column(
            sa.Column("jobs_failed", sa.INTEGER(), autoincrement=False, default=0, nullable=False)
        )
        batch_op.drop_column("sysinfo")
        batch_op.add_column(sa.Column("sysinfo", sa.VARCHAR(length=256), nullable=True))
