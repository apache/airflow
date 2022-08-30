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

"""Add map_index to TaskFail

Drop index idx_task_fail_dag_task_date
Add run_id and map_index
Drop execution_date
Add FK `task_fail_ti_fkey`: TF -> TI ([dag_id, task_id, run_id, map_index])


Revision ID: 48925b2719cb
Revises: 4eaab2fe6582
Create Date: 2022-03-14 10:31:11.220720
"""

from typing import List

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import ColumnElement, Update, and_, select

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = '48925b2719cb'
down_revision = '4eaab2fe6582'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'

ID_LEN = 250


def tables():
    global task_instance, task_fail, dag_run
    metadata = sa.MetaData()
    task_instance = sa.Table(
        'task_instance',
        metadata,
        sa.Column('task_id', StringID()),
        sa.Column('dag_id', StringID()),
        sa.Column('run_id', StringID()),
        sa.Column('map_index', sa.Integer(), server_default='-1'),
        sa.Column('execution_date', TIMESTAMP),
    )
    task_fail = sa.Table(
        'task_fail',
        metadata,
        sa.Column('dag_id', StringID()),
        sa.Column('task_id', StringID()),
        sa.Column('run_id', StringID()),
        sa.Column('map_index', StringID()),
        sa.Column('execution_date', TIMESTAMP),
    )
    dag_run = sa.Table(
        'dag_run',
        metadata,
        sa.Column('dag_id', StringID()),
        sa.Column('run_id', StringID()),
        sa.Column('execution_date', TIMESTAMP),
    )


def _update_value_from_dag_run(
    dialect_name: str,
    target_table: sa.Table,
    target_column: ColumnElement,
    join_columns: List[str],
) -> Update:
    """
    Grabs a value from the source table ``dag_run`` and updates target with this value.
    :param dialect_name: dialect in use
    :param target_table: the table to update
    :param target_column: the column to update
    """
    # for run_id:  dag_id, execution_date
    # otherwise:  dag_id, run_id
    condition_list = [getattr(dag_run.c, x) == getattr(target_table.c, x) for x in join_columns]
    condition = and_(*condition_list)

    if dialect_name == "sqlite":
        # Most SQLite versions don't support multi table update (and SQLA doesn't know about it anyway), so we
        # need to do a Correlated subquery update
        sub_q = select([dag_run.c[target_column.name]]).where(condition)

        return target_table.update().values({target_column: sub_q})
    else:
        return target_table.update().where(condition).values({target_column: dag_run.c[target_column.name]})


def upgrade():
    tables()
    dialect_name = op.get_bind().dialect.name

    op.drop_index('idx_task_fail_dag_task_date', table_name='task_fail')

    with op.batch_alter_table('task_fail') as batch_op:
        batch_op.add_column(sa.Column('map_index', sa.Integer(), server_default='-1', nullable=False))
        batch_op.add_column(sa.Column('run_id', type_=StringID(), nullable=True))

    update_query = _update_value_from_dag_run(
        dialect_name=dialect_name,
        target_table=task_fail,
        target_column=task_fail.c.run_id,
        join_columns=['dag_id', 'execution_date'],
    )
    op.execute(update_query)
    with op.batch_alter_table('task_fail') as batch_op:
        batch_op.alter_column('run_id', existing_type=StringID(), existing_nullable=True, nullable=False)
        batch_op.drop_column('execution_date')
        batch_op.create_foreign_key(
            'task_fail_ti_fkey',
            'task_instance',
            ['dag_id', 'task_id', 'run_id', 'map_index'],
            ['dag_id', 'task_id', 'run_id', 'map_index'],
            ondelete='CASCADE',
        )


def downgrade():
    tables()
    dialect_name = op.get_bind().dialect.name
    op.add_column('task_fail', sa.Column('execution_date', TIMESTAMP, nullable=True))
    update_query = _update_value_from_dag_run(
        dialect_name=dialect_name,
        target_table=task_fail,
        target_column=task_fail.c.execution_date,
        join_columns=['dag_id', 'run_id'],
    )
    op.execute(update_query)
    with op.batch_alter_table('task_fail') as batch_op:
        batch_op.alter_column('execution_date', existing_type=TIMESTAMP, nullable=False)
        if dialect_name != 'sqlite':
            batch_op.drop_constraint('task_fail_ti_fkey', type_='foreignkey')
        batch_op.drop_column('map_index', mssql_drop_default=True)
        batch_op.drop_column('run_id')
    op.create_index(
        index_name='idx_task_fail_dag_task_date',
        table_name='task_fail',
        columns=['dag_id', 'task_id', 'execution_date'],
        unique=False,
    )
