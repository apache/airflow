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
Update migration for FAB tables to add missing constraints.

Revision ID: 909884dea523
Revises: 48925b2719cb
Create Date: 2022-03-21 08:33:01.635688

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import get_mssql_table_constraints

# revision identifiers, used by Alembic.
revision = "909884dea523"
down_revision = "48925b2719cb"
branch_labels = None
depends_on = None
airflow_version = "2.3.0"


def upgrade():
    """Apply Update migration for FAB tables to add missing constraints."""
    conn = op.get_bind()
    if conn.dialect.name == "sqlite":
        op.execute("PRAGMA foreign_keys=OFF")
        with op.batch_alter_table("ab_view_menu", schema=None) as batch_op:
            batch_op.create_unique_constraint(batch_op.f("ab_view_menu_name_uq"), ["name"])
        op.execute("PRAGMA foreign_keys=ON")
    elif conn.dialect.name == "mysql":
        with op.batch_alter_table("ab_register_user", schema=None) as batch_op:
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=False)
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=False)
        with op.batch_alter_table("ab_user", schema=None) as batch_op:
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=False)
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=False)
    elif conn.dialect.name == "mssql":
        with op.batch_alter_table("ab_register_user") as batch_op:
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_register_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=False)
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=False)
        with op.batch_alter_table("ab_user") as batch_op:
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=False)
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=False)
            batch_op.create_unique_constraint(None, ["email"])


def downgrade():
    """Unapply Update migration for FAB tables to add missing constraints."""
    conn = op.get_bind()
    if conn.dialect.name == "sqlite":
        op.execute("PRAGMA foreign_keys=OFF")
        with op.batch_alter_table("ab_view_menu", schema=None) as batch_op:
            batch_op.drop_constraint("ab_view_menu_name_uq", type_="unique")
        op.execute("PRAGMA foreign_keys=ON")
    elif conn.dialect.name == "mysql":
        with op.batch_alter_table("ab_user", schema=None) as batch_op:
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=True)
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=True, unique=True)
        with op.batch_alter_table("ab_register_user", schema=None) as batch_op:
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=True)
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=True, unique=True)
    elif conn.dialect.name == "mssql":
        with op.batch_alter_table("ab_register_user") as batch_op:
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_register_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=False, unique=True)
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=False, unique=True)
        with op.batch_alter_table("ab_user") as batch_op:
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", existing_type=sa.String(256), nullable=True)
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", existing_type=sa.String(256), nullable=True, unique=True)
            batch_op.create_unique_constraint(None, ["email"])
