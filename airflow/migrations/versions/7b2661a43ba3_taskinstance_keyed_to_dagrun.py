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

"""TaskInstance keyed to DagRun

Revision ID: 7b2661a43ba3
Revises: 142555e44c17
Create Date: 2021-07-15 15:26:12.710749

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import and_, column, select, table

from airflow.models.base import COLLATION_ARGS

ID_LEN = 250

# revision identifiers, used by Alembic.
revision = '7b2661a43ba3'
down_revision = '142555e44c17'
branch_labels = None
depends_on = None


def _mssql_datetime():
    from sqlalchemy.dialects import mssql

    return mssql.DATETIME2(precision=6)


# Just Enough Table to run the conditions for update.
task_instance = table(
    'task_instance',
    column('task_id', sa.String),
    column('dag_id', sa.String),
    column('run_id', sa.String),
    column('execution_date', sa.TIMESTAMP),
)
task_reschedule = table(
    'task_reschedule',
    column('task_id', sa.String),
    column('dag_id', sa.String),
    column('run_id', sa.String),
    column('execution_date', sa.TIMESTAMP),
)
dag_run = table(
    'dag_run',
    column('dag_id', sa.String),
    column('run_id', sa.String),
    column('execution_date', sa.TIMESTAMP),
)


def upgrade():
    """Apply TaskInstance keyed to DagRun"""
    dialect_name = op.get_bind().dialect.name

    run_id_col_type = sa.String(length=ID_LEN, **COLLATION_ARGS)

    # First create column nullable
    op.add_column('task_instance', sa.Column('run_id', run_id_col_type, nullable=True))
    op.add_column('task_reschedule', sa.Column('run_id', run_id_col_type, nullable=True))

    # Then update the new column by selecting the right value from DagRun
    update_query = _multi_table_update(dialect_name, task_instance, task_instance.c.run_id)
    op.execute(update_query)

    #
    # TaskReschedule has a FK to TaskInstance, so we have to update that before
    # we can drop the TI.execution_date column

    update_query = _multi_table_update(dialect_name, task_reschedule, task_reschedule.c.run_id)
    op.execute(update_query)

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.alter_column('run_id', existing_type=run_id_col_type, existing_nullable=True, nullable=False)

        batch_op.drop_constraint('task_reschedule_dag_task_date_fkey')
        batch_op.drop_index('idx_task_reschedule_dag_task_date')

    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        # Then make it non-nullable
        batch_op.alter_column('run_id', existing_type=run_id_col_type, existing_nullable=True, nullable=False)

        # TODO: Is this right for non-postgres?
        batch_op.drop_constraint('task_instance_pkey', type_='primary')
        batch_op.create_primary_key('task_instance_pkey', ['dag_id', 'task_id', 'run_id'])

        batch_op.drop_index('ti_dag_date')
        batch_op.drop_index('ti_state_lkp')
        batch_op.drop_column('execution_date')
        batch_op.create_foreign_key(
            'task_instance_dag_run_fkey',
            'dag_run',
            ['dag_id', 'run_id'],
            ['dag_id', 'run_id'],
            ondelete='CASCADE',
        )

        batch_op.create_index('ti_dag_run', ['dag_id', 'run_id', 'state'])
        batch_op.create_index('ti_state_lkp', ['dag_id', 'task_id', 'run_id', 'state'])

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.drop_column('execution_date')
        batch_op.create_index(
            'idx_task_reschedule_dag_task_run',
            ['dag_id', 'task_id', 'run_id'],
            unique=False,
        )
        # _Now_ there is a unique constraint on the columns in TI we can re-create the FK from TaskReschedule
        batch_op.create_foreign_key(
            'task_reschedule_ti_fkey',
            'task_instance',
            ['dag_id', 'task_id', 'run_id'],
            ['dag_id', 'task_id', 'run_id'],
            ondelete='CASCADE',
        )


def downgrade():
    """Unapply TaskInstance keyed to DagRun"""
    dialect_name = op.get_bind().dialect.name

    if dialect_name == "mssql":
        col_type = _mssql_datetime()
    else:
        col_type = sa.TIMESTAMP(timezone=True)

    op.add_column('task_instance', sa.Column('execution_date', col_type, nullable=True))
    op.add_column('task_reschedule', sa.Column('execution_date', col_type, nullable=True))

    update_query = _multi_table_update(dialect_name, task_instance, task_instance.c.execution_date)
    op.execute(update_query)

    update_query = _multi_table_update(dialect_name, task_reschedule, task_reschedule.c.execution_date)
    op.execute(update_query)

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.alter_column(
            'execution_date', existing_type=col_type, existing_nullable=True, nullable=False
        )

        # Can't drop PK index while there is a FK referencing it
        batch_op.drop_constraint('task_reschedule_ti_fkey')
        batch_op.drop_index('idx_task_reschedule_dag_task_run')

    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.alter_column(
            'execution_date', existing_type=col_type, existing_nullable=True, nullable=False
        )

        batch_op.drop_constraint('task_instance_pkey', type_='primary')
        batch_op.create_primary_key('task_instance_pkey', ['dag_id', 'task_id', 'execution_date'])

        batch_op.drop_constraint('task_instance_dag_run_fkey', type_='foreignkey')
        batch_op.drop_index('ti_dag_run')
        batch_op.drop_index('ti_state_lkp')
        batch_op.create_index('ti_state_lkp', ['dag_id', 'task_id', 'execution_date', 'state'])
        batch_op.create_index('ti_dag_date', ['dag_id', 'execution_date'], unique=False)

        batch_op.drop_column('run_id')

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.drop_column('run_id')
        batch_op.create_index(
            'idx_task_reschedule_dag_task_date',
            ['dag_id', 'task_id', 'execution_date'],
            unique=False,
        )
        # Can only create FK once there is an index on these columns
        batch_op.create_foreign_key(
            'task_reschedule_dag_task_date_fkey',
            'task_instance',
            ['dag_id', 'task_id', 'execution_date'],
            ['dag_id', 'task_id', 'execution_date'],
            ondelete='CASCADE',
        )


def _multi_table_update(dialect_name, target, column):
    condition = dag_run.c.dag_id == target.c.dag_id
    if column == target.c.run_id:
        condition = and_(condition, dag_run.c.execution_date == target.c.execution_date)
    else:
        condition = and_(condition, dag_run.c.run_id == target.c.run_id)

    if dialect_name == "sqlite":
        # Most SQLite versions don't support multi table update (and SQLA doesn't know about it anyway), so we
        # need to do a Correlated subquery update
        sub_q = select([dag_run.c[column.name]]).where(condition)

        return target.update().values({column: sub_q})
    else:
        return target.update().where(condition).values({column: dag_run.c[column.name]})
