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
Add asset reference models.

Revision ID: 38770795785f
Revises: 5c9c0231baa2
Create Date: 2024-12-18 11:12:50.639369
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = "38770795785f"
down_revision = "5c9c0231baa2"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

ASSET_STR_FIELD = sa.String(length=1500).with_variant(
    sa.String(length=1500, collation="latin1_general_cs"), "mysql"
)


def upgrade():
    """Add asset reference models."""
    op.create_table(
        "dag_schedule_asset_name_reference",
        sa.Column("name", ASSET_STR_FIELD, primary_key=True, nullable=False),
        sa.Column("dag_id", StringID(), primary_key=True, nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("name", "dag_id", name="dsanr_pkey"),
        sa.ForeignKeyConstraint(
            columns=("dag_id",),
            refcolumns=["dag.dag_id"],
            name="dsanr_dag_id_fkey",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_dag_schedule_asset_name_reference_dag_id",
        "dag_schedule_asset_name_reference",
        ["dag_id"],
        unique=False,
    )

    op.create_table(
        "dag_schedule_asset_uri_reference",
        sa.Column("uri", ASSET_STR_FIELD, primary_key=True, nullable=False),
        sa.Column("dag_id", StringID(), primary_key=True, nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("uri", "dag_id", name="dsaur_pkey"),
        sa.ForeignKeyConstraint(
            columns=("dag_id",),
            refcolumns=["dag.dag_id"],
            name="dsaur_dag_id_fkey",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "idx_dag_schedule_asset_uri_reference_dag_id",
        "dag_schedule_asset_uri_reference",
        ["dag_id"],
        unique=False,
    )


def downgrade():
    """Unadd asset reference models."""
    op.drop_table("dag_schedule_asset_name_reference")
    op.drop_table("dag_schedule_asset_uri_reference")
