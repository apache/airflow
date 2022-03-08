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

"""Change ``TaskInstance`` and ``TaskReschedule`` tables from execution_date to run_id.

Revision ID: 7b2661a43ba3
Revises: 142555e44c17
Create Date: 2021-07-15 15:26:12.710749

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import and_, column, select, table

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.migrations.utils import get_mssql_table_constraints

ID_LEN = 250

# revision identifiers, used by Alembic.
revision = '7b2661a43ba3'
down_revision = '142555e44c17'
branch_labels = None
depends_on = None
airflow_version = '2.2.0'


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
    """Apply Change ``TaskInstance`` and ``TaskReschedule`` tables from execution_date to run_id."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    dt_type = TIMESTAMP
    string_id_col_type = StringID()

    if dialect_name == 'sqlite':
        naming_convention = {
            "uq": "%(table_name)s_%(column_0_N_name)s_key",
        }
        # The naming_convention force the previously un-named UNIQUE constraints to have the right name
        with op.batch_alter_table(
            'dag_run', naming_convention=naming_convention, recreate="always"
        ) as batch_op:
            batch_op.alter_column('dag_id', existing_type=string_id_col_type, nullable=False)
            batch_op.alter_column('run_id', existing_type=string_id_col_type, nullable=False)
            batch_op.alter_column('execution_date', existing_type=dt_type, nullable=False)
    elif dialect_name == 'mysql':
        with op.batch_alter_table('dag_run') as batch_op:
            batch_op.alter_column(
                'dag_id', existing_type=sa.String(length=ID_LEN), type_=string_id_col_type, nullable=False
            )
            batch_op.alter_column(
                'run_id', existing_type=sa.String(length=ID_LEN), type_=string_id_col_type, nullable=False
            )
            batch_op.alter_column('execution_date', existing_type=dt_type, nullable=False)
            batch_op.drop_constraint('dag_id', 'unique')
            batch_op.drop_constraint('dag_id_2', 'unique')
            batch_op.create_unique_constraint(
                'dag_run_dag_id_execution_date_key', ['dag_id', 'execution_date']
            )
            batch_op.create_unique_constraint('dag_run_dag_id_run_id_key', ['dag_id', 'run_id'])
    elif dialect_name == 'mssql':

        with op.batch_alter_table('dag_run') as batch_op:
            batch_op.drop_index('idx_not_null_dag_id_execution_date')
            batch_op.drop_index('idx_not_null_dag_id_run_id')

            batch_op.drop_index('dag_id_state')
            batch_op.drop_index('idx_dag_run_dag_id')
            batch_op.drop_index('idx_dag_run_running_dags')
            batch_op.drop_index('idx_dag_run_queued_dags')

            batch_op.alter_column('dag_id', existing_type=string_id_col_type, nullable=False)
            batch_op.alter_column('execution_date', existing_type=dt_type, nullable=False)
            batch_op.alter_column('run_id', existing_type=string_id_col_type, nullable=False)

            # _Somehow_ mssql was missing these constraints entirely
            batch_op.create_unique_constraint(
                'dag_run_dag_id_execution_date_key', ['dag_id', 'execution_date']
            )
            batch_op.create_unique_constraint('dag_run_dag_id_run_id_key', ['dag_id', 'run_id'])

            batch_op.create_index('dag_id_state', ['dag_id', 'state'], unique=False)
            batch_op.create_index('idx_dag_run_dag_id', ['dag_id'])
            batch_op.create_index(
                'idx_dag_run_running_dags',
                ["state", "dag_id"],
                mssql_where=sa.text("state='running'"),
            )
            batch_op.create_index(
                'idx_dag_run_queued_dags',
                ["state", "dag_id"],
                mssql_where=sa.text("state='queued'"),
            )
    else:
        # Make sure DagRun PK columns are non-nullable
        with op.batch_alter_table('dag_run', schema=None) as batch_op:
            batch_op.alter_column('dag_id', existing_type=string_id_col_type, nullable=False)
            batch_op.alter_column('execution_date', existing_type=dt_type, nullable=False)
            batch_op.alter_column('run_id', existing_type=string_id_col_type, nullable=False)

    # First create column nullable
    op.add_column('task_instance', sa.Column('run_id', type_=string_id_col_type, nullable=True))
    op.add_column('task_reschedule', sa.Column('run_id', type_=string_id_col_type, nullable=True))

    #
    # TaskReschedule has a FK to TaskInstance, so we have to update that before
    # we can drop the TI.execution_date column

    update_query = _multi_table_update(dialect_name, task_reschedule, task_reschedule.c.run_id)
    op.execute(update_query)

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.alter_column(
            'run_id', existing_type=string_id_col_type, existing_nullable=True, nullable=False
        )

        batch_op.drop_constraint('task_reschedule_dag_task_date_fkey', 'foreignkey')
        if dialect_name == "mysql":
            # Mysql creates an index and a constraint -- we have to drop both
            batch_op.drop_index('task_reschedule_dag_task_date_fkey')
            batch_op.alter_column(
                'dag_id', existing_type=sa.String(length=ID_LEN), type_=string_id_col_type, nullable=False
            )
        batch_op.drop_index('idx_task_reschedule_dag_task_date')

    # Then update the new column by selecting the right value from DagRun
    # But first we will drop and recreate indexes to make it faster
    if dialect_name == 'postgresql':
        # Recreate task_instance, without execution_date and with dagrun.run_id
        op.execute(
            """
            CREATE TABLE new_task_instance AS SELECT
                ti.task_id,
                ti.dag_id,
                dag_run.run_id,
                ti.start_date,
                ti.end_date,
                ti.duration,
                ti.state,
                ti.try_number,
                ti.hostname,
                ti.unixname,
                ti.job_id,
                ti.pool,
                ti.queue,
                ti.priority_weight,
                ti.operator,
                ti.queued_dttm,
                ti.pid,
                ti.max_tries,
                ti.executor_config,
                ti.pool_slots,
                ti.queued_by_job_id,
                ti.external_executor_id,
                ti.trigger_id,
                ti.trigger_timeout,
                ti.next_method,
                ti.next_kwargs
            FROM task_instance ti
            INNER JOIN dag_run ON dag_run.dag_id = ti.dag_id AND dag_run.execution_date = ti.execution_date;
        """
        )
        op.drop_table('task_instance')
        op.rename_table('new_task_instance', 'task_instance')

        # Fix up columns after the 'create table as select'
        with op.batch_alter_table('task_instance', schema=None) as batch_op:
            batch_op.alter_column(
                'pool', existing_type=string_id_col_type, existing_nullable=True, nullable=False
            )
            batch_op.alter_column('max_tries', existing_type=sa.Integer(), server_default="-1")
            batch_op.alter_column(
                'pool_slots', existing_type=sa.Integer(), existing_nullable=True, nullable=False
            )
    else:
        update_query = _multi_table_update(dialect_name, task_instance, task_instance.c.run_id)
        op.execute(update_query)

    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        if dialect_name != 'postgresql':
            # TODO: Is this right for non-postgres?
            if dialect_name == 'mssql':
                constraints = get_mssql_table_constraints(conn, "task_instance")
                pk, _ = constraints['PRIMARY KEY'].popitem()
                batch_op.drop_constraint(pk, type_='primary')
            elif dialect_name not in ('sqlite'):
                batch_op.drop_constraint('task_instance_pkey', type_='primary')
            batch_op.drop_index('ti_dag_date')
            batch_op.drop_index('ti_state_lkp')
            batch_op.drop_column('execution_date')

        # Then make it non-nullable
        batch_op.alter_column(
            'run_id', existing_type=string_id_col_type, existing_nullable=True, nullable=False
        )
        batch_op.alter_column(
            'dag_id', existing_type=string_id_col_type, existing_nullable=True, nullable=False
        )

        batch_op.create_primary_key('task_instance_pkey', ['dag_id', 'task_id', 'run_id'])
        batch_op.create_foreign_key(
            'task_instance_dag_run_fkey',
            'dag_run',
            ['dag_id', 'run_id'],
            ['dag_id', 'run_id'],
            ondelete='CASCADE',
        )

        batch_op.create_index('ti_dag_run', ['dag_id', 'run_id'])
        batch_op.create_index('ti_state_lkp', ['dag_id', 'task_id', 'run_id', 'state'])
        if dialect_name == 'postgresql':
            batch_op.create_index('ti_dag_state', ['dag_id', 'state'])
            batch_op.create_index('ti_job_id', ['job_id'])
            batch_op.create_index('ti_pool', ['pool', 'state', 'priority_weight'])
            batch_op.create_index('ti_state', ['state'])
            batch_op.create_foreign_key(
                'task_instance_trigger_id_fkey', 'trigger', ['trigger_id'], ['id'], ondelete="CASCADE"
            )
            batch_op.create_index('ti_trigger_id', ['trigger_id'])

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

        # https://docs.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-1785-database-engine-error?view=sql-server-ver15
        ondelete = 'CASCADE' if dialect_name != 'mssql' else 'NO ACTION'
        batch_op.create_foreign_key(
            'task_reschedule_dr_fkey',
            'dag_run',
            ['dag_id', 'run_id'],
            ['dag_id', 'run_id'],
            ondelete=ondelete,
        )


def downgrade():
    """Unapply Change ``TaskInstance`` and ``TaskReschedule`` tables from execution_date to run_id."""
    dialect_name = op.get_bind().dialect.name
    dt_type = TIMESTAMP
    string_id_col_type = StringID()

    op.add_column('task_instance', sa.Column('execution_date', dt_type, nullable=True))
    op.add_column('task_reschedule', sa.Column('execution_date', dt_type, nullable=True))

    update_query = _multi_table_update(dialect_name, task_instance, task_instance.c.execution_date)
    op.execute(update_query)

    update_query = _multi_table_update(dialect_name, task_reschedule, task_reschedule.c.execution_date)
    op.execute(update_query)

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.alter_column('execution_date', existing_type=dt_type, existing_nullable=True, nullable=False)

        # Can't drop PK index while there is a FK referencing it
        batch_op.drop_constraint('task_reschedule_ti_fkey', type_='foreignkey')
        batch_op.drop_constraint('task_reschedule_dr_fkey', type_='foreignkey')
        batch_op.drop_index('idx_task_reschedule_dag_task_run')

    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.drop_constraint('task_instance_pkey', type_='primary')
        batch_op.alter_column('execution_date', existing_type=dt_type, existing_nullable=True, nullable=False)
        if dialect_name != 'mssql':
            batch_op.alter_column(
                'dag_id', existing_type=string_id_col_type, existing_nullable=False, nullable=True
            )

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

    if dialect_name == "mssql":

        with op.batch_alter_table('dag_run', schema=None) as batch_op:
            batch_op.drop_constraint('dag_run_dag_id_execution_date_key', 'unique')
            batch_op.drop_constraint('dag_run_dag_id_run_id_key', 'unique')
            batch_op.drop_index('dag_id_state')
            batch_op.drop_index('idx_dag_run_running_dags')
            batch_op.drop_index('idx_dag_run_queued_dags')
            batch_op.drop_index('idx_dag_run_dag_id')

            batch_op.alter_column('dag_id', existing_type=string_id_col_type, nullable=True)
            batch_op.alter_column('execution_date', existing_type=dt_type, nullable=True)
            batch_op.alter_column('run_id', existing_type=string_id_col_type, nullable=True)

            batch_op.create_index('dag_id_state', ['dag_id', 'state'], unique=False)
            batch_op.create_index('idx_dag_run_dag_id', ['dag_id'])
            batch_op.create_index(
                'idx_dag_run_running_dags',
                ["state", "dag_id"],
                mssql_where=sa.text("state='running'"),
            )
            batch_op.create_index(
                'idx_dag_run_queued_dags',
                ["state", "dag_id"],
                mssql_where=sa.text("state='queued'"),
            )
        op.execute(
            """CREATE UNIQUE NONCLUSTERED INDEX idx_not_null_dag_id_execution_date
                    ON dag_run(dag_id,execution_date)
                    WHERE dag_id IS NOT NULL and execution_date is not null"""
        )
        op.execute(
            """CREATE UNIQUE NONCLUSTERED INDEX idx_not_null_dag_id_run_id
                     ON dag_run(dag_id,run_id)
                     WHERE dag_id IS NOT NULL and run_id is not null"""
        )
    else:
        with op.batch_alter_table('dag_run', schema=None) as batch_op:
            batch_op.drop_index('dag_id_state')
            batch_op.alter_column('run_id', existing_type=sa.VARCHAR(length=250), nullable=True)
            batch_op.alter_column('execution_date', existing_type=dt_type, nullable=True)
            batch_op.alter_column('dag_id', existing_type=sa.VARCHAR(length=250), nullable=True)
            batch_op.create_index('dag_id_state', ['dag_id', 'state'], unique=False)


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
