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

"""add task_reschedule table

Revision ID: 0a2a5b66e19d
Revises: 9635ae0956e7
Create Date: 2018-06-17 22:50:00.053620

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = '0a2a5b66e19d'
down_revision = '9635ae0956e7'
branch_labels = None
depends_on = None

TABLE_NAME = 'task_reschedule'
INDEX_NAME = 'idx_' + TABLE_NAME + '_dag_task_date'


# For Microsoft SQL Server, TIMESTAMP is a row-id type,
# having nothing to do with date-time.  DateTime() will
# be sufficient.
def mssql_timestamp():   # noqa: D103
    return sa.DateTime()


def mysql_timestamp():   # noqa: D103
    return mysql.TIMESTAMP(fsp=6)


def sa_timestamp():   # noqa: D103
    return sa.TIMESTAMP(timezone=True)


def upgrade():   # noqa: D103
    # See 0e2a74e0fc9f_add_time_zone_awareness
    conn = op.get_bind()
    if conn.dialect.name == 'mysql':
        timestamp = mysql_timestamp
    elif conn.dialect.name == 'mssql':
        timestamp = mssql_timestamp
    else:
        timestamp = sa_timestamp

    op.create_table(
        TABLE_NAME,
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('task_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        # use explicit server_default=None otherwise mysql implies defaults for first timestamp column
        sa.Column('execution_date', timestamp(), nullable=False, server_default=None),
        sa.Column('try_number', sa.Integer(), nullable=False),
        sa.Column('start_date', timestamp(), nullable=False),
        sa.Column('end_date', timestamp(), nullable=False),
        sa.Column('duration', sa.Integer(), nullable=False),
        sa.Column('reschedule_date', timestamp(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(
            ['task_id', 'dag_id', 'execution_date'],
            ['task_instance.task_id', 'task_instance.dag_id', 'task_instance.execution_date'],
            name='task_reschedule_dag_task_date_fkey')
    )
    op.create_index(
        INDEX_NAME,
        TABLE_NAME,
        ['dag_id', 'task_id', 'execution_date'],
        unique=False
    )


def downgrade():   # noqa: D103
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
    op.drop_table(TABLE_NAME)
