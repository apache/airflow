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

"""Increase length of user identifier columns in ``ab_user`` and ``ab_register_user`` tables

Revision ID: 98ae134e6fff
Revises: 6abdffdd4815
Create Date: 2023-01-18 16:21:09.420958

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import get_mssql_table_constraints

# revision identifiers, used by Alembic.
revision = "98ae134e6fff"
down_revision = "6abdffdd4815"
branch_labels = None
depends_on = None
airflow_version = "2.6.0"


def upgrade():
    """Increase length of user identifier columns in ab_user and ab_register_user tables"""
    with op.batch_alter_table("ab_user") as batch_op:
        batch_op.alter_column("first_name", type_=sa.String(256), existing_nullable=False)
        batch_op.alter_column("last_name", type_=sa.String(256), existing_nullable=False)
        batch_op.alter_column(
            "username",
            type_=sa.String(512).with_variant(sa.String(512, collation="NOCASE"), "sqlite"),
            existing_nullable=False,
        )
        batch_op.alter_column("email", type_=sa.String(512), existing_nullable=False)
    with op.batch_alter_table("ab_register_user") as batch_op:
        batch_op.alter_column("first_name", type_=sa.String(256), existing_nullable=False)
        batch_op.alter_column("last_name", type_=sa.String(256), existing_nullable=False)
        batch_op.alter_column(
            "username",
            type_=sa.String(512).with_variant(sa.String(512, collation="NOCASE"), "sqlite"),
            existing_nullable=False,
        )
        batch_op.alter_column("email", type_=sa.String(512), existing_nullable=False)


def downgrade():
    """Revert length of user identifier columns in ab_user and ab_register_user tables"""
    conn = op.get_bind()
    if conn.dialect.name != "mssql":
        with op.batch_alter_table("ab_user") as batch_op:
            batch_op.alter_column("first_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column("last_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column(
                "username",
                type_=sa.String(256).with_variant(sa.String(256, collation="NOCASE"), "sqlite"),
                existing_nullable=False,
            )
            batch_op.alter_column("email", type_=sa.String(256), existing_nullable=False)
        with op.batch_alter_table("ab_register_user") as batch_op:
            batch_op.alter_column("first_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column("last_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column(
                "username",
                type_=sa.String(256).with_variant(sa.String(256, collation="NOCASE"), "sqlite"),
                existing_nullable=False,
            )
            batch_op.alter_column("email", type_=sa.String(256), existing_nullable=False)
    else:
        # MSSQL doesn't drop implicit unique constraints it created
        # We need to drop the two unique constraints explicitly
        with op.batch_alter_table("ab_user") as batch_op:
            batch_op.alter_column("first_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column("last_name", type_=sa.String(64), existing_nullable=False)
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", type_=sa.String(256), existing_nullable=False)
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", type_=sa.String(256), existing_nullable=False)
            batch_op.create_unique_constraint(None, ["email"])

        with op.batch_alter_table("ab_register_user") as batch_op:
            batch_op.alter_column("first_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column("last_name", type_=sa.String(64), existing_nullable=False)
            batch_op.alter_column("email", type_=sa.String(256), existing_nullable=False)
            # Drop the unique constraint on username
            constraints = get_mssql_table_constraints(conn, "ab_register_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", type_=sa.String(256), existing_nullable=False)
            batch_op.create_unique_constraint(None, ["username"])
