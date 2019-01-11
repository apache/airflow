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

"""Add fractional seconds to mysql tables

Revision ID: 4addfa1236f1
Revises: f2ca10b85618
Create Date: 2016-09-11 13:39:18.592072

"""

from alembic import op
from sqlalchemy.dialects import mysql
from alembic import context

# revision identifiers, used by Alembic.
revision = '4addfa1236f1'
down_revision = 'f2ca10b85618'
branch_labels = None
depends_on = None


def upgrade():
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.alter_column(table_name='dag', column_name='last_scheduler_run',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='dag', column_name='last_pickled',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='dag', column_name='last_expired',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='dag_pickle', column_name='created_dttm',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='dag_run', column_name='execution_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='dag_run', column_name='start_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='dag_run', column_name='end_date',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='import_error', column_name='timestamp',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='job', column_name='start_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='job', column_name='end_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='job', column_name='latest_heartbeat',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='log', column_name='dttm',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='log', column_name='execution_date',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='sla_miss', column_name='execution_date',
                        type_=mysql.DATETIME(fsp=6),
                        nullable=False)
        op.alter_column(table_name='sla_miss', column_name='timestamp',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='task_fail', column_name='execution_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='task_fail', column_name='start_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='task_fail', column_name='end_date',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='task_instance', column_name='execution_date',
                        type_=mysql.DATETIME(fsp=6),
                        nullable=False)
        op.alter_column(table_name='task_instance', column_name='start_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='task_instance', column_name='end_date',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='task_instance', column_name='queued_dttm',
                        type_=mysql.DATETIME(fsp=6))

        op.alter_column(table_name='xcom', column_name='timestamp',
                        type_=mysql.DATETIME(fsp=6))
        op.alter_column(table_name='xcom', column_name='execution_date',
                        type_=mysql.DATETIME(fsp=6))


def downgrade():
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.alter_column(table_name='dag', column_name='last_scheduler_run',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='dag', column_name='last_pickled',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='dag', column_name='last_expired',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='dag_pickle', column_name='created_dttm',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='dag_run', column_name='execution_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='dag_run', column_name='start_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='dag_run', column_name='end_date',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='import_error', column_name='timestamp',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='job', column_name='start_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='job', column_name='end_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='job', column_name='latest_heartbeat',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='log', column_name='dttm',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='log', column_name='execution_date',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='sla_miss', column_name='execution_date',
                        type_=mysql.DATETIME(), nullable=False)
        op.alter_column(table_name='sla_miss', column_name='timestamp',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='task_fail', column_name='execution_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='task_fail', column_name='start_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='task_fail', column_name='end_date',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='task_instance', column_name='execution_date',
                        type_=mysql.DATETIME(),
                        nullable=False)
        op.alter_column(table_name='task_instance', column_name='start_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='task_instance', column_name='end_date',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='task_instance', column_name='queued_dttm',
                        type_=mysql.DATETIME())

        op.alter_column(table_name='xcom', column_name='timestamp',
                        type_=mysql.DATETIME())
        op.alter_column(table_name='xcom', column_name='execution_date',
                        type_=mysql.DATETIME())
