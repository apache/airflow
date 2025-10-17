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
Add Group tables for flask-appbuilder 4.6.3 compatibility.

Revision ID: a1b2c3d4e5f6  
Revises: 5f2621c13b39
Create Date: 2025-10-17 15:55:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "5f2621c13b39"
branch_labels = None
depends_on = None
airflow_version = "2.11.0"


def upgrade() -> None:
    """Apply migration."""
    # Create ab_group table
    op.create_table(
        "ab_group",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("label", sa.String(length=150), nullable=True),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("ab_group_pkey")),
        sa.UniqueConstraint("name", name=op.f("ab_group_name_uq")),
        if_not_exists=True,
    )
    
    # Create ab_group_role association table
    op.create_table(
        "ab_group_role",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("role_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["group_id"], ["ab_group.id"], name=op.f("ab_group_role_group_id_fkey"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["role_id"], ["ab_role.id"], name=op.f("ab_group_role_role_id_fkey"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("ab_group_role_pkey")),
        sa.UniqueConstraint("group_id", "role_id", name=op.f("ab_group_role_group_id_role_id_uq")),
        if_not_exists=True,
    )
    with op.batch_alter_table("ab_group_role", schema=None) as batch_op:
        batch_op.create_index("idx_group_id", ["group_id"], unique=False, if_not_exists=True)
        batch_op.create_index("idx_group_role_id", ["role_id"], unique=False, if_not_exists=True)

    # Create ab_user_group association table
    op.create_table(
        "ab_user_group",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["group_id"], ["ab_group.id"], name=op.f("ab_user_group_group_id_fkey"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["ab_user.id"], name=op.f("ab_user_group_user_id_fkey"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("ab_user_group_pkey")),
        sa.UniqueConstraint("user_id", "group_id", name=op.f("ab_user_group_user_id_group_id_uq")),
        if_not_exists=True,
    )
    with op.batch_alter_table("ab_user_group", schema=None) as batch_op:
        batch_op.create_index("idx_user_group_id", ["group_id"], unique=False, if_not_exists=True)
        batch_op.create_index("idx_user_id", ["user_id"], unique=False, if_not_exists=True)

    # Update ab_user_role table to add CASCADE deletes if not already present
    # This is needed for flask-appbuilder 4.6.3 compatibility
    with op.batch_alter_table("ab_user_role", schema=None) as batch_op:
        # Drop existing foreign keys and recreate with CASCADE
        batch_op.drop_constraint("ab_user_role_user_id_fkey", type_="foreignkey")
        batch_op.drop_constraint("ab_user_role_role_id_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "ab_user_role_user_id_fkey", "ab_user", ["user_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.create_foreign_key(
            "ab_user_role_role_id_fkey", "ab_role", ["role_id"], ["id"], ondelete="CASCADE"
        )

    # Update ab_permission_view_role table to add CASCADE deletes if not already present
    with op.batch_alter_table("ab_permission_view_role", schema=None) as batch_op:
        # Drop existing foreign keys and recreate with CASCADE
        batch_op.drop_constraint("ab_permission_view_role_permission_view_id_fkey", type_="foreignkey")
        batch_op.drop_constraint("ab_permission_view_role_role_id_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "ab_permission_view_role_permission_view_id_fkey", 
            "ab_permission_view", 
            ["permission_view_id"], 
            ["id"], 
            ondelete="CASCADE"
        )
        batch_op.create_foreign_key(
            "ab_permission_view_role_role_id_fkey", "ab_role", ["role_id"], ["id"], ondelete="CASCADE"
        )


def downgrade() -> None:
    """Unapply migration."""
    # Drop the new tables in reverse order
    op.drop_table("ab_user_group")
    op.drop_table("ab_group_role") 
    op.drop_table("ab_group")
    
    # Revert foreign key constraints back to original state
    with op.batch_alter_table("ab_user_role", schema=None) as batch_op:
        batch_op.drop_constraint("ab_user_role_user_id_fkey", type_="foreignkey")
        batch_op.drop_constraint("ab_user_role_role_id_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "ab_user_role_user_id_fkey", "ab_user", ["user_id"], ["id"]
        )
        batch_op.create_foreign_key(
            "ab_user_role_role_id_fkey", "ab_role", ["role_id"], ["id"]
        )

    with op.batch_alter_table("ab_permission_view_role", schema=None) as batch_op:
        batch_op.drop_constraint("ab_permission_view_role_permission_view_id_fkey", type_="foreignkey")
        batch_op.drop_constraint("ab_permission_view_role_role_id_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "ab_permission_view_role_permission_view_id_fkey", 
            "ab_permission_view", 
            ["permission_view_id"], 
            ["id"]
        )
        batch_op.create_foreign_key(
            "ab_permission_view_role_role_id_fkey", "ab_role", ["role_id"], ["id"]
        )