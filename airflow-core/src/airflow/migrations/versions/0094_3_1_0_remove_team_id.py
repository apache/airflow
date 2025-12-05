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
Drop ``id`` column from ``team`` table and make ``name`` the primary key.

Revision ID: b12d4f98a91e
Revises: 665854ef0536
Create Date: 2025-12-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "b12d4f98a91e"
down_revision = "665854ef0536"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    # Drop team id references
    for table in ("connection", "variable", "slot_pool"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_constraint(batch_op.f(f"{table}_team_id_fkey"), type_="foreignkey")

    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.drop_constraint("dag_bundle_team_team_id_fkey", type_="foreignkey")
        batch_op.drop_index("idx_dag_bundle_team_team_id")

    for table in ("connection", "variable", "slot_pool"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.alter_column(
                "team_id",
                new_column_name="team_name",
                type_=sa.String(50),
                existing_type=sa.String(),
                nullable=True,
            )

    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.alter_column(
            "team_id",
            new_column_name="team_name",
            type_=sa.String(50),
            nullable=False,
        )

    # Team table
    with op.batch_alter_table("team") as batch_op:
        batch_op.drop_constraint("team_pkey", type_="primary")
        batch_op.drop_constraint("team_name_uq", type_="unique")
        batch_op.drop_column("id")
        batch_op.create_primary_key("team_pkey", ["name"])

    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.create_index("idx_dag_bundle_team_team_name", ["team_name"])
        batch_op.create_foreign_key(
            "dag_bundle_team_team_name_fkey",
            "team",
            ["team_name"],
            ["name"],
            ondelete="CASCADE",
        )

    # Recreate foreign keys referencing team.name
    for table in ("connection", "variable", "slot_pool"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.create_foreign_key(
                batch_op.f(f"{table}_team_name_fkey"),
                "team",
                local_cols=["team_name"],
                remote_cols=["name"],
                ondelete="SET NULL",
            )


def downgrade():
    # Drop FKs pointing to name
    for table in ("connection", "variable", "slot_pool"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_constraint(batch_op.f(f"{table}_team_name_fkey"), type_="foreignkey")

    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.drop_constraint("dag_bundle_team_team_name_fkey", type_="foreignkey")
        batch_op.drop_index("idx_dag_bundle_team_team_name")

    # Add back team.id
    with op.batch_alter_table("team") as batch_op:
        batch_op.drop_constraint("team_pkey", type_="primary")
        batch_op.add_column(sa.Column("id", sa.String(36), nullable=False))
        batch_op.create_unique_constraint("team_name_uq", ["name"])
        batch_op.create_primary_key("team_pkey", ["id"])

    # Rename team_name → team_id
    for table in ("connection", "variable", "slot_pool"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.alter_column(
                "team_name",
                new_column_name="team_id",
                type_=sa.String(36),
                nullable=True,
            )

    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.alter_column(
            "team_name",
            new_column_name="team_id",
            type_=sa.String(36),
            nullable=False,
        )

    # Re-create FK on old id
    for table in ("connection", "variable", "slot_pool"):
        with op.batch_alter_table(table) as batch_op:
            batch_op.create_foreign_key(
                batch_op.f(f"{table}_team_id_fkey"),
                "team",
                ["team_id"],
                ["id"],
            )

    with op.batch_alter_table("dag_bundle_team") as batch_op:
        batch_op.create_foreign_key(
            "dag_bundle_team_team_id_fkey",
            "team",
            ["team_id"],
            ["id"],
            ondelete="CASCADE",
        )
