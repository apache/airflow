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

"""Add map_index to SlaMiss

Main changes:
    * Add run_id
    * Add map_index
    * Drop execution_date
    * Add slamiss FK

Revision ID: c65c10f6c311
Revises: 4eaab2fe6582
Create Date: 2022-03-10 22:39:54.462272

"""
from typing import List

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import ColumnElement, Update, and_, select

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.migrations.utils import get_mssql_table_constraints

# revision identifiers, used by Alembic.
revision = 'c65c10f6c311'
down_revision = '4eaab2fe6582'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'

ID_LEN = 250


# Just Enough Table to run the conditions for update.
def tables():
    global task_instance, sla_miss, dag_run
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
    sla_miss = sa.Table(
        'sla_miss',
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

    with op.batch_alter_table('sla_miss') as batch_op:
        batch_op.add_column(sa.Column('map_index', sa.Integer(), server_default='-1', nullable=False))
        batch_op.add_column(sa.Column('run_id', type_=StringID(), nullable=True))

    update_query = _update_value_from_dag_run(
        dialect_name=dialect_name,
        target_table=sla_miss,
        target_column=sla_miss.c.run_id,
        join_columns=['dag_id', 'execution_date'],
    )
    op.execute(update_query)
    with op.batch_alter_table('sla_miss') as batch_op:
        if dialect_name == 'mssql':
            constraints = get_mssql_table_constraints(op.get_bind(), 'sla_miss')
            pk, _ = constraints['PRIMARY KEY'].popitem()
            batch_op.drop_constraint(pk, type_='primary')
        elif dialect_name != 'sqlite':  # sqlite PK is managed by SQLA
            batch_op.drop_constraint('sla_miss_pkey', type_='primary')
        batch_op.alter_column('run_id', existing_type=StringID(), existing_nullable=True, nullable=False)
        batch_op.drop_column('execution_date')
        batch_op.create_primary_key('sla_miss_pkey', ['dag_id', 'task_id', 'run_id', 'map_index'])


def downgrade():
    tables()
    dialect_name = op.get_bind().dialect.name
    op.add_column('sla_miss', sa.Column('execution_date', TIMESTAMP, nullable=True))
    update_query = _update_value_from_dag_run(
        dialect_name=dialect_name,
        target_table=sla_miss,
        target_column=sla_miss.c.execution_date,
        join_columns=['dag_id', 'run_id'],
    )
    op.execute(update_query)
    with op.batch_alter_table('sla_miss', copy_from=sla_miss) as batch_op:
        batch_op.alter_column('execution_date', existing_type=TIMESTAMP, nullable=False)
        if dialect_name != 'sqlite':
            batch_op.drop_constraint('sla_miss_pkey', type_='primary')
        batch_op.create_primary_key('sla_miss_pkey', ['dag_id', 'task_id', 'execution_date'])
        batch_op.drop_column('map_index', mssql_drop_default=True)
        batch_op.drop_column('run_id')
