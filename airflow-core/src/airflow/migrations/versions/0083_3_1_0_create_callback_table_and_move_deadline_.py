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
Create callback table and move deadline callbacks to it.

Revision ID: bdd599ff8039
Revises: 7582ea3f3dd5
Create Date: 2025-08-22 23:52:01.347148

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
import sqlalchemy_utils
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "bdd599ff8039"
down_revision = "7582ea3f3dd5"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Create callback table and move deadline callbacks to it."""
    op.create_table(
        "callback",
        sa.Column("callback_type", sa.String(length=30), nullable=False),
        sa.Column("id", sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False),
        sa.Column("data", sqlalchemy_jsonfield.jsonfield.JSONField(), nullable=True),
        sa.Column("state", sa.String(length=20), nullable=True),
        sa.Column("output", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("callback_pkey")),
    )
    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("missed", sa.Boolean(), nullable=False))
        batch_op.add_column(
            sa.Column("callback_id", sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=False)
        )
        batch_op.drop_index(batch_op.f("deadline_callback_state_time_idx"))
        batch_op.create_index("deadline_missed_deadline_time_idx", ["missed", "deadline_time"], unique=False)
        batch_op.drop_constraint(batch_op.f("deadline_trigger_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("deadline_callback_id_fkey"), "callback", ["callback_id"], ["id"]
        )
        batch_op.drop_column("callback")
        batch_op.drop_column("callback_state")
        batch_op.drop_column("trigger_id")

    with op.batch_alter_table("trigger", schema=None) as batch_op:
        batch_op.add_column(sa.Column("callback_id", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("trigger_callback_id_fkey"), "callback", ["callback_id"], ["id"]
        )


def downgrade():
    """Unapply: create callback table and move deadline callbacks to it."""
    with op.batch_alter_table("trigger", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("trigger_callback_id_fkey"), type_="foreignkey")
        batch_op.drop_column("callback_id")

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.add_column(sa.Column("trigger_id", sa.INTEGER(), autoincrement=False, nullable=True))
        batch_op.add_column(
            sa.Column("callback_state", sa.VARCHAR(length=20), autoincrement=False, nullable=True)
        )
        batch_op.add_column(
            sa.Column("callback", postgresql.JSON(astext_type=sa.Text()), autoincrement=False, nullable=False)
        )
        batch_op.drop_constraint(batch_op.f("deadline_callback_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(batch_op.f("deadline_trigger_id_fkey"), "trigger", ["trigger_id"], ["id"])
        batch_op.drop_index("deadline_missed_deadline_time_idx")
        batch_op.create_index(
            batch_op.f("deadline_callback_state_time_idx"), ["callback_state", "deadline_time"], unique=False
        )
        batch_op.drop_column("callback_id")
        batch_op.drop_column("missed")

    op.drop_table("callback")
