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

"""Add Precision to execution_date in RenderedTaskInstanceFields table

Revision ID: a66efa278eea
Revises: 952da73b5eff
Create Date: 2020-06-16 21:44:02.883132

"""

from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'a66efa278eea'
down_revision = '952da73b5eff'
branch_labels = None
depends_on = None

TABLE_NAME = 'rendered_task_instance_fields'
COLUMN_NAME = 'execution_date'


def upgrade():
    """Add Precision to execution_date in RenderedTaskInstanceFields table for MySQL"""
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        op.alter_column(
            table_name=TABLE_NAME,
            column_name=COLUMN_NAME,
            type_=mysql.TIMESTAMP(fsp=6),
            nullable=False
        )


def downgrade():
    """Unapply Add Precision to execution_date in RenderedTaskInstanceFields table"""
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        op.alter_column(
            table_name=TABLE_NAME,
            column_name=COLUMN_NAME,
            type_=mysql.TIMESTAMP(),
            nullable=False
        )
