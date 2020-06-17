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

"""Add task_tag table

Revision ID: cb96106a9894
Revises: 8f966b9c467a
Create Date: 2020-06-11 14:39:02.764770

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql
from airflow.models.base import COLLATION_ARGS


# revision identifiers, used by Alembic.
revision = 'cb96106a9894'
down_revision = '8f966b9c467a'
branch_labels = None
depends_on = None


def get_execution_date_type():
    conn = op.get_bind()
    if conn.dialect.name == 'mysql':
        return mysql.TIMESTAMP(fsp=6)
    else:
        return sa.TIMESTAMP(timezone=True)

def upgrade():
    """Apply Add TaskTag table"""
    op.create_table(
        'task_tag',
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('task_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('execution_date', get_execution_date_type(), nullable=False, server_default=None),
        sa.ForeignKeyConstraint(('task_id', 'dag_id', 'execution_date'),
                                ('task_instance.task_id',
                                 'task_instance.dag_id',
                                 'task_instance.execution_date'),
                                ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('name', 'dag_id', 'task_id', 'execution_date')
    )


def downgrade():
    """Unapply Add TaskTag table"""
    op.drop_table('task_tag')

