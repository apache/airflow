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
Replace deadline's inline callback fields with foreign key to callback table.

Revision ID: e812941398f4
Revises: b87d2135fa50
Create Date: 2025-10-24 00:34:57.111239

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_jsonfield import JSONField
from sqlalchemy_utils import UUIDType

# revision identifiers, used by Alembic.
revision = "e812941398f4"
down_revision = "b87d2135fa50"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Replace Deadline table's inline callback fields with callback_id foreign key."""
    # Delete all rows for Deadline Alerts. This will be replaced by data-preserving migration soon.
    # TODO: Implement data-preserving migration.
    op.execute("DELETE FROM deadline")

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("missed", sa.Boolean(), nullable=False))
        batch_op.add_column(sa.Column("callback_id", UUIDType(binary=False), nullable=False))
        batch_op.drop_index(batch_op.f("deadline_callback_state_time_idx"))
        batch_op.create_index("deadline_missed_deadline_time_idx", ["missed", "deadline_time"], unique=False)
        batch_op.drop_constraint(batch_op.f("deadline_trigger_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("deadline_callback_id_fkey"), "callback", ["callback_id"], ["id"]
        )
        batch_op.drop_column("callback")
        batch_op.drop_column("trigger_id")
        batch_op.drop_column("callback_state")


def downgrade():
    """Restore Deadline table's inline callback fields from callback_id foreign key."""
    # Delete all rows for Deadline Alerts. See comment in upgrade().
    op.execute("DELETE FROM deadline")

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("callback_state", sa.VARCHAR(length=20), autoincrement=False, nullable=True)
        )
        batch_op.add_column(sa.Column("trigger_id", sa.INTEGER(), autoincrement=False, nullable=True))
        batch_op.add_column(sa.Column("callback", JSONField(), nullable=False))
        batch_op.drop_constraint(batch_op.f("deadline_callback_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_index("deadline_missed_deadline_time_idx")
        batch_op.create_index(
            batch_op.f("deadline_callback_state_time_idx"), ["callback_state", "deadline_time"], unique=False
        )
        batch_op.drop_column("callback_id")
        batch_op.drop_column("missed")
