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

"""fix_exec_date_rendered_task_instance_fields_for_MSSQL

Revision ID: 52d53670a240
Revises: 98271e7606e2
Create Date: 2020-10-13 15:13:24.911486

"""
from sqlalchemy import Column
from sqlalchemy.dialects import mssql
from alembic import op

# revision identifiers, used by Alembic.
revision = '52d53670a240'
down_revision = '98271e7606e2'
branch_labels = None
depends_on = None

TABLE_NAME = 'rendered_task_instance_fields'


def upgrade():
    """
        Change timestamp to datetime2(6) when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table(TABLE_NAME) as rendered_task_instance_fields:
            rendered_task_instance_fields.drop_constraint('rendered_task_instance_fields_pkey', type_='primary')
            rendered_task_instance_fields.drop_column("execution_date")
            rendered_task_instance_fields.add_column(Column('execution_date', mssql.DATETIME2))
            rendered_task_instance_fields.create_primary_key("rendered_task_instance_fields_pkey",
                                                             ["dag_id", "task_id", "execution_date"])


def downgrade():
    """Drop RenderedTaskInstanceFields table"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table(TABLE_NAME) as rendered_task_instance_fields:
            rendered_task_instance_fields.drop_constraint('rendered_task_instance_fields_pkey', type_='primary')
            rendered_task_instance_fields.drop_column('execution_date')
            rendered_task_instance_fields.add_column(Column('execution_date', mssql.TIMESTAMP, primary_key=True))
            rendered_task_instance_fields.create_primary_key("rendered_task_instance_fields_pkey",
                                                             ["dag_id", "task_id", "execution_date"])
