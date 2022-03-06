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

"""Add ``sensor_instance`` table

Revision ID: e38be357a868
Revises: 8d48763f6d53
Create Date: 2019-06-07 04:03:17.003939

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import func

from airflow.compat.sqlalchemy import inspect
from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = 'e38be357a868'
down_revision = '8d48763f6d53'
branch_labels = None
depends_on = None
airflow_version = '2.0.0'


def upgrade():

    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    if 'sensor_instance' in tables:
        return

    op.create_table(
        'sensor_instance',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('task_id', StringID(), nullable=False),
        sa.Column('dag_id', StringID(), nullable=False),
        sa.Column('execution_date', TIMESTAMP, nullable=False),
        sa.Column('state', sa.String(length=20), nullable=True),
        sa.Column('try_number', sa.Integer(), nullable=True),
        sa.Column('start_date', TIMESTAMP, nullable=True),
        sa.Column('operator', sa.String(length=1000), nullable=False),
        sa.Column('op_classpath', sa.String(length=1000), nullable=False),
        sa.Column('hashcode', sa.BigInteger(), nullable=False),
        sa.Column('shardcode', sa.Integer(), nullable=False),
        sa.Column('poke_context', sa.Text(), nullable=False),
        sa.Column('execution_context', sa.Text(), nullable=True),
        sa.Column('created_at', TIMESTAMP, default=func.now(), nullable=False),
        sa.Column('updated_at', TIMESTAMP, default=func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ti_primary_key', 'sensor_instance', ['dag_id', 'task_id', 'execution_date'], unique=True)
    op.create_index('si_hashcode', 'sensor_instance', ['hashcode'], unique=False)
    op.create_index('si_shardcode', 'sensor_instance', ['shardcode'], unique=False)
    op.create_index('si_state_shard', 'sensor_instance', ['state', 'shardcode'], unique=False)
    op.create_index('si_updated_at', 'sensor_instance', ['updated_at'], unique=False)


def downgrade():
    conn = op.get_bind()
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    if 'sensor_instance' in tables:
        op.drop_table('sensor_instance')
