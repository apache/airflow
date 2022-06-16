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

"""Add indexes for CASCADE deletes

Revision ID: f5fcbda3e651
Revises: 3c94c427fdf6
Create Date: 2022-06-15 18:04:54.081789

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = 'f5fcbda3e651'
down_revision = '3c94c427fdf6'
branch_labels = None
depends_on = None
airflow_version = '2.3.3'


def upgrade():
    """Apply Add indexes for CASCADE deletes"""
    with op.batch_alter_table('task_fail', schema=None) as batch_op:
        batch_op.create_index('idx_task_fail_task_instance', ['dag_id', 'task_id', 'run_id', 'map_index'])

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.create_index('idx_task_reschedule_dag_run', ['dag_id', 'run_id'])

    with op.batch_alter_table('xcom', schema=None) as batch_op:
        batch_op.create_index('idx_xcom_task_instance', ['dag_id', 'task_id', 'run_id', 'map_index'])


def downgrade():
    """Unapply Add indexes for CASCADE deletes"""
    with op.batch_alter_table('xcom', schema=None) as batch_op:
        batch_op.drop_index('idx_xcom_task_instance')

    with op.batch_alter_table('task_reschedule', schema=None) as batch_op:
        batch_op.drop_index('idx_task_reschedule_dag_run')

    with op.batch_alter_table('task_fail', schema=None) as batch_op:
        batch_op.drop_index('idx_task_fail_task_instance')
