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

"""Update missing constraints

Revision ID: 686269002441
Revises: 677fdbb7fc54
Create Date: 2024-04-15 14:19:49.913797

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '686269002441'
down_revision = '677fdbb7fc54'
branch_labels = None
depends_on = None
airflow_version = '2.10.0'


def upgrade():
    """Apply Update missing constraints"""
    with op.batch_alter_table('connection') as batch_op:
        batch_op.drop_constraint('unique_conn_id', type_='unique')
        batch_op.create_unique_constraint(batch_op.f('connection_conn_id_uq'), ['conn_id'])

    with op.batch_alter_table('dag') as batch_op:
        batch_op.alter_column('max_consecutive_failed_dag_runs',
               existing_type=sa.Integer(),
               nullable=False)

    with op.batch_alter_table('dag_run') as batch_op:
        batch_op.drop_constraint('dag_run_dag_id_execution_date_uq', type_='unique')
        batch_op.drop_constraint('dag_run_dag_id_run_id_uq', type_='unique')
        batch_op.create_unique_constraint('dag_run_dag_id_execution_date_key', ['dag_id', 'execution_date'])
        batch_op.create_unique_constraint('dag_run_dag_id_run_id_key', ['dag_id', 'run_id'])


def downgrade():
    """Unapply Update missing constraints"""
    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.drop_constraint('dag_run_dag_id_run_id_key', type_='unique')
        batch_op.drop_constraint('dag_run_dag_id_execution_date_key', type_='unique')
        batch_op.create_unique_constraint('dag_run_dag_id_run_id_uq', ['dag_id', 'run_id'])
        batch_op.create_unique_constraint('dag_run_dag_id_execution_date_uq', ['dag_id', 'execution_date'])

    with op.batch_alter_table('dag', schema=None) as batch_op:
        batch_op.alter_column('max_consecutive_failed_dag_runs',
               existing_type=sa.INTEGER(),
               nullable=True)

    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('connection_conn_id_uq'), type_='unique')
        batch_op.create_unique_constraint('unique_conn_id', ['conn_id'])
