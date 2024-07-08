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
Add DagRunNote and TaskInstanceNote.

Revision ID: 1986afd32c1b
Revises: ee8d93fcc81e
Create Date: 2022-11-22 21:49:05.843439

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "1986afd32c1b"
down_revision = "ee8d93fcc81e"
branch_labels = None
depends_on = None
airflow_version = "2.5.0"


def upgrade():
    """Apply Add DagRunNote and TaskInstanceNote."""
    op.create_table(
        "dag_run_note",
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("dag_run_id", sa.Integer(), nullable=False),
        sa.Column(
            "content", sa.String(length=1000).with_variant(sa.Text(length=1000), "mysql"), nullable=True
        ),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ("dag_run_id",), ["dag_run.id"], name="dag_run_note_dr_fkey", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(("user_id",), ["ab_user.id"], name="dag_run_note_user_fkey"),
        sa.PrimaryKeyConstraint("dag_run_id", name=op.f("dag_run_note_pkey")),
    )

    op.create_table(
        "task_instance_note",
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("task_id", StringID(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("run_id", StringID(), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False),
        sa.Column(
            "content", sa.String(length=1000).with_variant(sa.Text(length=1000), "mysql"), nullable=True
        ),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "task_id", "dag_id", "run_id", "map_index", name=op.f("task_instance_note_pkey")
        ),
        sa.ForeignKeyConstraint(
            ("dag_id", "task_id", "run_id", "map_index"),
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_instance_note_ti_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(("user_id",), ["ab_user.id"], name="task_instance_note_user_fkey"),
    )


def downgrade():
    """Unapply Add DagRunNote and TaskInstanceNote."""
    op.drop_table("task_instance_note")
    op.drop_table("dag_run_note")
