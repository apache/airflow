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
Add tables to store teams and associations with dag bundles.

Update also `connection`, `variable` and `pool` tables to add `team_id` as column.

Revision ID: a3c7f2b18d4e
Revises: 7582ea3f3dd5
Create Date: 2025-07-30 15:46:40.122517

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "a3c7f2b18d4e"
down_revision = "7582ea3f3dd5"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Create team table."""
    op.create_table(
        "team",
        sa.Column("id", UUIDType(binary=False), nullable=False),
        sa.Column("name", sa.String(50), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("team_pkey")),
        sa.UniqueConstraint("name", name="team_name_uq"),
    )

    """Create Dag bundle <-> team association table."""
    op.create_table(
        "dag_bundle_team",
        sa.Column("dag_bundle_name", StringID(), nullable=False),
        sa.Column("team_id", UUIDType(binary=False), nullable=False),
        sa.PrimaryKeyConstraint("dag_bundle_name", "team_id", name=op.f("dag_bundle_team_pkey")),
        sa.ForeignKeyConstraint(
            columns=("dag_bundle_name",),
            refcolumns=["dag_bundle.name"],
            name="dag_bundle_team_dag_bundle_name_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            columns=("team_id",),
            refcolumns=["team.id"],
            name="dag_bundle_team_team_id_fkey",
            ondelete="CASCADE",
        ),
        # For now, we limit one team to be associated with a Dag bundle, hence the unique constraint.
        # We might want to revisit that later, that is why we opted for a separate table instead of adding a column to the `dag_bundle` table
        sa.Index("idx_dag_bundle_team_dag_bundle_name", "dag_bundle_name", unique=True),
        sa.Index("idx_dag_bundle_team_team_id", "team_id", unique=False),
    )

    """Update `connection` table to add `team_id` column"""
    with op.batch_alter_table("connection") as batch_op:
        batch_op.add_column(sa.Column("team_id", UUIDType(binary=False), nullable=True))
        batch_op.create_foreign_key(batch_op.f("connection_team_id_fkey"), "team", ["team_id"], ["id"])
    """Update `variable` table to add `team_id` column"""
    with op.batch_alter_table("variable") as batch_op:
        batch_op.add_column(sa.Column("team_id", UUIDType(binary=False), nullable=True))
        batch_op.create_foreign_key(batch_op.f("variable_team_id_fkey"), "team", ["team_id"], ["id"])
    """Update `slot_pool` table to add `team_id` column"""
    with op.batch_alter_table("slot_pool") as batch_op:
        batch_op.add_column(sa.Column("team_id", UUIDType(binary=False), nullable=True))
        batch_op.create_foreign_key(batch_op.f("slot_pool_team_id_fkey"), "team", ["team_id"], ["id"])


def downgrade():
    with op.batch_alter_table("connection") as batch_op:
        batch_op.drop_constraint(batch_op.f("connection_team_id_fkey"), type_="foreignkey")
        batch_op.drop_column("team_id")
    with op.batch_alter_table("variable") as batch_op:
        batch_op.drop_constraint(batch_op.f("variable_team_id_fkey"), type_="foreignkey")
        batch_op.drop_column("team_id")
    with op.batch_alter_table("slot_pool") as batch_op:
        batch_op.drop_constraint(batch_op.f("slot_pool_team_id_fkey"), type_="foreignkey")
        batch_op.drop_column("team_id")
    op.drop_table("dag_bundle_team")
    op.drop_table("team")
