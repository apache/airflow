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

"""Add Dataset model

Revision ID: 0038cd0c28b4
Revises: 44b7034f6bdc
Create Date: 2022-06-22 14:37:20.880672

"""
import sqlalchemy as sa
import sqlalchemy_jsonfield
from alembic import op
from sqlalchemy import Integer, String, func

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.settings import json

revision = '0038cd0c28b4'
down_revision = '44b7034f6bdc'
branch_labels = None
depends_on = None
airflow_version = '2.4.0'


def _create_dataset_table():
    op.create_table(
        'dataset',
        sa.Column('id', Integer, primary_key=True, autoincrement=True),
        sa.Column(
            'uri',
            String(length=3000).with_variant(
                String(
                    length=3000,
                    # latin1 allows for more indexed length in mysql
                    # and this field should only be ascii chars
                    collation='latin1_general_cs',
                ),
                'mysql',
            ),
            nullable=False,
        ),
        sa.Column('extra', sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={}),
        sa.Column('created_at', TIMESTAMP, nullable=False),
        sa.Column('updated_at', TIMESTAMP, nullable=False),
        sqlite_autoincrement=True,  # ensures PK values not reused
    )
    op.create_index('idx_uri_unique', 'dataset', ['uri'], unique=True)


def _create_dag_schedule_dataset_reference_table():
    op.create_table(
        'dag_schedule_dataset_reference',
        sa.Column('dataset_id', Integer, primary_key=True, nullable=False),
        sa.Column('dag_id', String(250), primary_key=True, nullable=False),
        sa.Column('created_at', TIMESTAMP, default=func.now, nullable=False),
        sa.Column('updated_at', TIMESTAMP, default=func.now, nullable=False),
        sa.ForeignKeyConstraint(
            ('dataset_id',),
            ['dataset.id'],
            name="dsdr_dataset_fkey",
            ondelete="CASCADE",
        ),
        sqlite_autoincrement=True,  # ensures PK values not reused
    )


def _create_task_outlet_dataset_reference_table():
    op.create_table(
        'task_outlet_dataset_reference',
        sa.Column('dataset_id', Integer, primary_key=True, nullable=False),
        sa.Column('dag_id', String(250), primary_key=True, nullable=False),
        sa.Column('task_id', String(250), primary_key=True, nullable=False),
        sa.Column('created_at', TIMESTAMP, default=func.now, nullable=False),
        sa.Column('updated_at', TIMESTAMP, default=func.now, nullable=False),
        sa.ForeignKeyConstraint(
            ('dataset_id',),
            ['dataset.id'],
            name="todr_dataset_fkey",
            ondelete="CASCADE",
        ),
    )


def _create_dataset_dag_run_queue_table():
    op.create_table(
        'dataset_dag_run_queue',
        sa.Column('dataset_id', Integer, primary_key=True, nullable=False),
        sa.Column('target_dag_id', StringID(), primary_key=True, nullable=False),
        sa.Column('created_at', TIMESTAMP, default=func.now, nullable=False),
        sa.ForeignKeyConstraint(
            ('dataset_id',),
            ['dataset.id'],
            name="ddrq_dataset_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ('target_dag_id',),
            ['dag.dag_id'],
            name="ddrq_dag_fkey",
            ondelete="CASCADE",
        ),
    )


def _create_dataset_event_table():
    op.create_table(
        'dataset_event',
        sa.Column('id', Integer, primary_key=True, autoincrement=True),
        sa.Column('dataset_id', Integer, nullable=False),
        sa.Column('extra', sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={}),
        sa.Column('source_task_id', String(250), nullable=True),
        sa.Column('source_dag_id', String(250), nullable=True),
        sa.Column('source_run_id', String(250), nullable=True),
        sa.Column('source_map_index', sa.Integer(), nullable=True, server_default='-1'),
        sa.Column('timestamp', TIMESTAMP, nullable=False),
        sqlite_autoincrement=True,  # ensures PK values not reused
    )
    op.create_index('idx_dataset_id_timestamp', 'dataset_event', ['dataset_id', 'timestamp'])


def _create_dataset_event_dag_run_table():
    op.create_table(
        'dagrun_dataset_event',
        sa.Column('dag_run_id', sa.Integer(), nullable=False),
        sa.Column('event_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ['dag_run_id'],
            ['dag_run.id'],
            name=op.f('dagrun_dataset_events_dag_run_id_fkey'),
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['event_id'],
            ['dataset_event.id'],
            name=op.f('dagrun_dataset_events_event_id_fkey'),
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('dag_run_id', 'event_id', name=op.f('dagrun_dataset_events_pkey')),
    )
    with op.batch_alter_table('dagrun_dataset_event') as batch_op:
        batch_op.create_index('idx_dagrun_dataset_events_dag_run_id', ['dag_run_id'], unique=False)
        batch_op.create_index('idx_dagrun_dataset_events_event_id', ['event_id'], unique=False)


def upgrade():
    """Apply Add Dataset model"""
    _create_dataset_table()
    _create_dag_schedule_dataset_reference_table()
    _create_task_outlet_dataset_reference_table()
    _create_dataset_dag_run_queue_table()
    _create_dataset_event_table()
    _create_dataset_event_dag_run_table()


def downgrade():
    """Unapply Add Dataset model"""
    op.drop_table('dag_schedule_dataset_reference')
    op.drop_table('task_outlet_dataset_reference')
    op.drop_table('dataset_dag_run_queue')
    op.drop_table('dagrun_dataset_event')
    op.drop_table('dataset_event')
    op.drop_table('dataset')
