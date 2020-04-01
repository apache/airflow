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

"""Make TaskInstance.pool not nullable

Revision ID: 6e96a59344a4
Revises: 939bb1e647c8
Create Date: 2019-06-13 21:51:32.878437

"""

import dill
import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column, Float, Integer, PickleType, String
from sqlalchemy.ext.declarative import declarative_base

from airflow.models.base import COLLATION_ARGS
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = '6e96a59344a4'
down_revision = '939bb1e647c8'
branch_labels = None
depends_on = None

Base = declarative_base()
ID_LEN = 250


class TaskInstance(Base):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50), nullable=False)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))


def upgrade():
    """
    Make TaskInstance.pool field not nullable.
    """
    with create_session() as session:
        session.query(TaskInstance) \
            .filter(TaskInstance.pool.is_(None)) \
            .update({TaskInstance.pool: 'default_pool'},
                    synchronize_session=False)  # Avoid select updated rows
        session.commit()

    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.drop_index('ti_pool', table_name='task_instance')

    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.alter_column(
            column_name='pool',
            type_=sa.String(50),
            nullable=False,
        )

    if conn.dialect.name == "mssql":
        op.create_index('ti_pool', 'task_instance', ['pool', 'state', 'priority_weight'])


def downgrade():
    """
    Make TaskInstance.pool field nullable.
    """

    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.drop_index('ti_pool', table_name='task_instance')

    # use batch_alter_table to support SQLite workaround
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.alter_column(
            column_name='pool',
            type_=sa.String(50),
            nullable=True,
        )

    if conn.dialect.name == "mssql":
        op.create_index('ti_pool', 'task_instance', ['pool', 'state', 'priority_weight'])

    with create_session() as session:
        session.query(TaskInstance) \
            .filter(TaskInstance.pool == 'default_pool') \
            .update({TaskInstance.pool: None},
                    synchronize_session=False)  # Avoid select updated rows
        session.commit()
