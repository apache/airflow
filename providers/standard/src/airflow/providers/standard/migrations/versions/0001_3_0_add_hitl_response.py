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
Add hitl_response table.

Revision ID: 5e7113ca79cc
Revises:
Create Date: 2025-06-13 17:06:38.040510
"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op
from sqlalchemy.dialects import postgresql

from airflow.settings import json
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision: str = "5e7113ca79cc"
down_revision = None
branch_labels = None
depends_on = None
standard_provider_verison = "1.3.0"


def upgrade() -> None:
    op.create_table(
        "hitl_input_request",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("options", sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default=[]),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("defaults", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        sa.Column("params", sqlalchemy_jsonfield.JSONField(json=json), nullable=True),
        sa.Column("multiple", sa.Boolean, default=False, nullable=False, server_default="0"),
        sa.Column("ti_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql")),
        sa.ForeignKeyConstraint(
            ["ti_id"],
            ["task_instance.id"],
            name="hitl_input_request_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
    op.create_table(
        "hitl_response",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("ti_id", sa.String(length=36).with_variant(postgresql.UUID(), "postgresql")),
        sa.ForeignKeyConstraint(
            ["ti_id"],
            ["task_instance.id"],
            name="hitl_response_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )


def downgrade() -> None:
    op.drop_table("hitl_response")
