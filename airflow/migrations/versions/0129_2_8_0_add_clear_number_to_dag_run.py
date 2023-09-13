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

"""add new field 'clear_number' to dagrun

Revision ID: 375a816bbbf4
Revises: 405de8318b3a
Create Date: 2023-09-05 19:27:30.531558

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "375a816bbbf4"
down_revision = "405de8318b3a"
branch_labels = None
depends_on = None
airflow_version = "2.8.0"


def upgrade():
    """Apply add cleared column to dagrun"""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(
            sa.Column(
                "clear_number",
                sa.Integer,
                server_default="0",
                nullable=False,
            )
        )


def downgrade():
    """Unapply add cleared column to pool"""
    conn = op.get_bind()
    with op.batch_alter_table("dag_run") as batch_op:
        if conn.dialect.name == "mssql":
            constraints = get_mssql_table_constraints(conn, "dag_run")
            for k, cols in constraints.get("NOT NULL").items():
                if "clear_number" in cols:
                    batch_op.drop_constraint(k)
            for k, cols in constraints.get("DEFAULT").items():
                if "clear_number" in cols:
                    batch_op.drop_constraint(k)
        batch_op.drop_column("clear_number")
