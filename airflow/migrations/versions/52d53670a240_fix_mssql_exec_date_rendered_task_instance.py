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

"""fix_mssql_exec_date_rendered_task_instance_fields_for_MSSQL

Revision ID: 52d53670a240
Revises: 98271e7606e2
Create Date: 2020-10-13 15:13:24.911486

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mssql

# revision identifiers, used by Alembic.
revision = '52d53670a240'
down_revision = '98271e7606e2'
branch_labels = None
depends_on = None

TABLE_NAME = 'rendered_task_instance_fields'


def upgrade():
    """
    Recreate RenderedTaskInstanceFields table changing timestamp to datetime2(6) when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        json_type = sa.Text
        op.drop_table(TABLE_NAME)  # pylint: disable=no-member

        op.create_table(
            TABLE_NAME,  # pylint: disable=no-member
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('execution_date', mssql.DATETIME2, nullable=False),
            sa.Column('rendered_fields', json_type(), nullable=False),
            sa.PrimaryKeyConstraint('dag_id', 'task_id', 'execution_date')
        )


def downgrade():
    """
    Recreate RenderedTaskInstanceFields table changing datetime2(6) to timestamp when using MSSQL as backend
    """
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        json_type = sa.Text
        op.drop_table(TABLE_NAME)  # pylint: disable=no-member

        op.create_table(
            TABLE_NAME,  # pylint: disable=no-member
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('execution_date', sa.TIMESTAMP, nullable=False),
            sa.Column('rendered_fields', json_type(), nullable=False),
            sa.PrimaryKeyConstraint('dag_id', 'task_id', 'execution_date')
        )
