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

"""Add item to TaskMap.

Revision ID: c555f970eaa1
Revises: 3c94c427fdf6
Create Date: 2022-06-02 11:55:25.727690
"""

from alembic import op
from sqlalchemy import Column, Text

from airflow.migrations.utils import get_mssql_table_constraints

# Revision identifiers, used by Alembic.
revision = "c555f970eaa1"
down_revision = "3c94c427fdf6"
branch_labels = None
depends_on = None

airflow_version = "2.4.0"


def upgrade():
    """Add item to TaskMap."""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        old_pkey_name, _ = get_mssql_table_constraints(conn, "task_map")["PRIMARY KEY"].popitem()
    else:
        old_pkey_name = "task_map_pkey"
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_constraint(old_pkey_name, type_="primary")
        batch_op.add_column(Column("item", Text(), nullable=False, server_default=""))
        batch_op.create_primary_key("task_map_pkey", ["dag_id", "task_id", "run_id", "map_index", "item"])


def downgrade():
    """Remove item from TaskMap."""
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_constraint("task_map_pkey", type_="primary")
        batch_op.execute("DELETE FROM task_map WHERE item != ''")
        batch_op.drop_column("item")
        batch_op.create_primary_key("task_map_pkey", ["dag_id", "task_id", "run_id", "map_index"])
