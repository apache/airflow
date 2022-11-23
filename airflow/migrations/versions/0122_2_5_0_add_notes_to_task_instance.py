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

"""Add notes to task_instance

Revision ID: 2243be94a694
Revises: 65a852f26899
Create Date: 2022-11-22 16:43:21.448730

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Text, text

from airflow.migrations.db_types import StringID
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "2243be94a694"
down_revision = "65a852f26899"
branch_labels = None
depends_on = None
airflow_version = "2.5.0"


def upgrade():
    """Apply Add notes to task_instance"""
    op.create_table(
        "task_note",
        sa.Column("user_id", sa.Integer, nullable=True),
        sa.Column("task_id", StringID(), primary_key=True, nullable=False),
        sa.Column("dag_id", StringID(), primary_key=True, nullable=False),
        sa.Column("run_id", StringID(), primary_key=True, nullable=False),
        sa.Column("map_index", sa.Integer, primary_key=True, nullable=False, server_default=text("-1")),
        sa.Column("content", sa.String(1000).with_variant(Text(1000), "mysql")),
        sa.Column("created_at", UtcDateTime, default=timezone.utcnow, nullable=False),
        sa.Column("updated_at", UtcDateTime, default=timezone.utcnow, nullable=False),
        sa.ForeignKeyConstraint(
            ("task_id", "dag_id", "run_id", "map_index"),
            [
                "task_instance.task_id",
                "task_instance.dag_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(("user_id",), ["ab_user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("task_id", "dag_id", "run_id", "map_index"),
    )


def downgrade():
    """Unapply Add notes to task_instance"""
    op.drop_table("dag_owner_attributes")
