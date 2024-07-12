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
Increase length of email and username in ``ab_user`` and ``ab_register_user`` table to ``256`` characters.

Revision ID: 5e3ec427fdd3
Revises: 587bdf053233
Create Date: 2021-12-01 11:49:26.390210

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import get_mssql_table_constraints

# revision identifiers, used by Alembic.
revision = "5e3ec427fdd3"
down_revision = "587bdf053233"
branch_labels = None
depends_on = None
airflow_version = "2.3.0"


def upgrade():
    """Increase length of email from 64 to 256 characters."""
    with op.batch_alter_table("ab_user") as batch_op:
        batch_op.alter_column("username", type_=sa.String(256))
        batch_op.alter_column("email", type_=sa.String(256))
    with op.batch_alter_table("ab_register_user") as batch_op:
        batch_op.alter_column("username", type_=sa.String(256))
        batch_op.alter_column("email", type_=sa.String(256))


def downgrade():
    """Revert length of email from 256 to 64 characters."""
    conn = op.get_bind()
    if conn.dialect.name != "mssql":
        with op.batch_alter_table("ab_user") as batch_op:
            batch_op.alter_column("username", type_=sa.String(64), nullable=False)
            batch_op.alter_column("email", type_=sa.String(64))
        with op.batch_alter_table("ab_register_user") as batch_op:
            batch_op.alter_column("username", type_=sa.String(64))
            batch_op.alter_column("email", type_=sa.String(64))
    else:
        # MSSQL doesn't drop implicit unique constraints it created
        # We need to drop the two unique constraints explicitly
        with op.batch_alter_table("ab_user") as batch_op:
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_user")
            unique_key, _ = constraints["UNIQUE"].popitem()
            batch_op.drop_constraint(unique_key, type_="unique")
            unique_key, _ = constraints["UNIQUE"].popitem()
            batch_op.drop_constraint(unique_key, type_="unique")
            batch_op.alter_column("username", type_=sa.String(64), nullable=False)
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", type_=sa.String(64))
            batch_op.create_unique_constraint(None, ["email"])

        with op.batch_alter_table("ab_register_user") as batch_op:
            # Drop the unique constraint on username and email
            constraints = get_mssql_table_constraints(conn, "ab_register_user")
            for k, _ in constraints.get("UNIQUE").items():
                batch_op.drop_constraint(k, type_="unique")
            batch_op.alter_column("username", type_=sa.String(64))
            batch_op.create_unique_constraint(None, ["username"])
            batch_op.alter_column("email", type_=sa.String(64))
