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
Drop log_template table.

Revision ID: 4f6723e37686
Revises: 436dc127462c
Create Date: 2026-07-05 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "4f6723e37686"
down_revision = "436dc127462c"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Apply Drop log_template table."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.drop_constraint(batch_op.f("task_instance_log_template_id_fkey"), type_="foreignkey")
            batch_op.drop_column("log_template_id")
        op.drop_table("log_template")


def downgrade():
    """Unapply Drop log_template table."""
    with disable_sqlite_fkeys(op):
        op.create_table(
            "log_template",
            sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
            sa.Column("filename", sa.Text(), nullable=False),
            sa.Column("elasticsearch_id", sa.Text(), nullable=False),
            sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id", name="log_template_pkey"),
        )
        with op.batch_alter_table("dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("log_template_id", sa.Integer(), nullable=True))
            batch_op.create_foreign_key(
                batch_op.f("task_instance_log_template_id_fkey"),
                "log_template",
                ["log_template_id"],
                ["id"],
                ondelete="NO ACTION",
            )
