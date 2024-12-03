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
Add tables for backfill.

Revision ID: 522625f6d606
Revises: 1cdc775ca98f
Create Date: 2024-08-23 14:26:08.250493

"""

from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op

import airflow

# revision identifiers, used by Alembic.
revision = "522625f6d606"
down_revision = "1cdc775ca98f"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply Add tables for backfill."""
    op.create_table(
        "backfill",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("from_date", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.Column("to_date", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.Column("dag_run_conf", sqlalchemy_jsonfield.jsonfield.JSONField(), nullable=False),
        sa.Column("is_paused", sa.Boolean(), nullable=True),
        sa.Column("max_active_runs", sa.Integer(), nullable=False),
        sa.Column("created_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.Column("completed_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=True),
        sa.Column("updated_at", airflow.utils.sqlalchemy.UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("backfill_pkey")),
    )
    op.create_table(
        "backfill_dag_run",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("backfill_id", sa.Integer(), nullable=False),
        sa.Column("dag_run_id", sa.Integer(), nullable=True),
        sa.Column("sort_ordinal", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("backfill_dag_run_pkey")),
        sa.UniqueConstraint("backfill_id", "dag_run_id", name="ix_bdr_backfill_id_dag_run_id"),
        sa.ForeignKeyConstraint(
            ["backfill_id"], ["backfill.id"], name="bdr_backfill_fkey", ondelete="cascade"
        ),
        sa.ForeignKeyConstraint(["dag_run_id"], ["dag_run.id"], name="bdr_dag_run_fkey", ondelete="set null"),
    )


def downgrade():
    """Unapply Add tables for backfill."""
    op.drop_table("backfill_dag_run")
    op.drop_table("backfill")
