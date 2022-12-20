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

"""Add case-insensitive unique constraint for username

Revision ID: e07f49787c9d
Revises: b0d31815b5a6
Create Date: 2022-10-25 17:29:46.432326

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e07f49787c9d"
down_revision = "b0d31815b5a6"
branch_labels = None
depends_on = None
airflow_version = "2.4.3"


def upgrade():
    """Apply Add case-insensitive unique constraint"""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        op.create_index("idx_ab_user_username", "ab_user", [sa.text("LOWER(username)")], unique=True)
        op.create_index(
            "idx_ab_register_user_username", "ab_register_user", [sa.text("LOWER(username)")], unique=True
        )
    elif conn.dialect.name == "sqlite":
        with op.batch_alter_table("ab_user") as batch_op:
            batch_op.alter_column(
                "username",
                existing_type=sa.String(64),
                _type=sa.String(64, collation="NOCASE"),
                unique=True,
                nullable=False,
            )
        with op.batch_alter_table("ab_register_user") as batch_op:
            batch_op.alter_column(
                "username",
                existing_type=sa.String(64),
                _type=sa.String(64, collation="NOCASE"),
                unique=True,
                nullable=False,
            )


def downgrade():
    """Unapply Add case-insensitive unique constraint"""
    conn = op.get_bind()
    if conn.dialect.name == "postgresql":
        op.drop_index("idx_ab_user_username", table_name="ab_user")
        op.drop_index("idx_ab_register_user_username", table_name="ab_register_user")
    elif conn.dialect.name == "sqlite":
        with op.batch_alter_table("ab_user") as batch_op:
            batch_op.alter_column(
                "username",
                existing_type=sa.String(64, collation="NOCASE"),
                _type=sa.String(64),
                unique=True,
                nullable=False,
            )
        with op.batch_alter_table("ab_register_user") as batch_op:
            batch_op.alter_column(
                "username",
                existing_type=sa.String(64, collation="NOCASE"),
                _type=sa.String(64),
                unique=True,
                nullable=False,
            )
