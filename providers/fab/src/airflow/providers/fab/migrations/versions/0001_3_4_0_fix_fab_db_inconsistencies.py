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
Fix fab db inconsistencies.

Revision ID: 02ca36b0235b
Revises: 6709f7a774b9
Create Date: 2026-03-10 14:07:31.559184

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "02ca36b0235b"
down_revision = "6709f7a774b9"
branch_labels = None
depends_on = None
fab_version = "3.4.0"


def upgrade() -> None:
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        op.create_index(
            "idx_ab_user_username",
            "ab_user",
            [sa.literal_column("lower(username::text)")],
            unique=True,
            if_not_exists=True,
        )
        op.create_index(
            "idx_ab_register_user_username",
            "ab_register_user",
            [sa.literal_column("lower(username::text)")],
            unique=True,
            if_not_exists=True,
        )
    with op.batch_alter_table("ab_permission_view_role", schema=None) as batch_op:
        batch_op.create_index("idx_permission_view_id", ["permission_view_id"], unique=False)
        batch_op.create_index("idx_role_id", ["role_id"], unique=False)
        batch_op.drop_constraint(batch_op.f("ab_permission_view_role_role_id_fkey"), type_="foreignkey")
        batch_op.drop_constraint(
            batch_op.f("ab_permission_view_role_permission_view_id_fkey"), type_="foreignkey"
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_permission_view_role_permission_view_id_fkey"),
            "ab_permission_view",
            ["permission_view_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_permission_view_role_role_id_fkey"),
            "ab_role",
            ["role_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("ab_register_user", schema=None) as batch_op:
        batch_op.create_unique_constraint(batch_op.f("ab_register_user_email_uq"), ["email"])

    if dialect_name != "mysql":
        with op.batch_alter_table("ab_user_group", schema=None) as batch_op:
            batch_op.drop_index(batch_op.f("idx_user_group_id"))
            batch_op.drop_index(batch_op.f("idx_user_id"))

    with op.batch_alter_table("ab_user_role", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("ab_user_role_role_id_fkey"), type_="foreignkey")
        batch_op.drop_constraint(batch_op.f("ab_user_role_user_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("ab_user_role_role_id_fkey"), "ab_role", ["role_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_user_role_user_id_fkey"), "ab_user", ["user_id"], ["id"], ondelete="CASCADE"
        )


def downgrade() -> None:
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        op.drop_index("idx_ab_register_user_username", table_name="ab_register_user", if_exists=True)
        op.drop_index("idx_ab_user_username", table_name="ab_user", if_exists=True)
    with op.batch_alter_table("ab_user_role", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("ab_user_role_user_id_fkey"), type_="foreignkey")
        batch_op.drop_constraint(batch_op.f("ab_user_role_role_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(batch_op.f("ab_user_role_user_id_fkey"), "ab_user", ["user_id"], ["id"])
        batch_op.create_foreign_key(batch_op.f("ab_user_role_role_id_fkey"), "ab_role", ["role_id"], ["id"])
    if dialect_name != "mysql":
        with op.batch_alter_table("ab_user_group", schema=None) as batch_op:
            batch_op.create_index(batch_op.f("idx_user_id"), ["user_id"], unique=False)
            batch_op.create_index(batch_op.f("idx_user_group_id"), ["group_id"], unique=False)

    with op.batch_alter_table("ab_register_user", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("ab_register_user_email_uq"), type_="unique")

    with op.batch_alter_table("ab_permission_view_role", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("ab_permission_view_role_role_id_fkey"), type_="foreignkey")
        batch_op.drop_constraint(
            batch_op.f("ab_permission_view_role_permission_view_id_fkey"), type_="foreignkey"
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_permission_view_role_permission_view_id_fkey"),
            "ab_permission_view",
            ["permission_view_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            batch_op.f("ab_permission_view_role_role_id_fkey"), "ab_role", ["role_id"], ["id"]
        )
        batch_op.drop_index("idx_role_id")
        batch_op.drop_index("idx_permission_view_id")
