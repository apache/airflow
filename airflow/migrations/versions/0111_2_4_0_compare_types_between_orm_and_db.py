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

"""compare types between ORM and DB

Revision ID: 44b7034f6bdc
Revises: 3c94c427fdf6
Create Date: 2022-05-31 09:16:44.558754

"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = '44b7034f6bdc'
down_revision = 'f5fcbda3e651'
branch_labels = None
depends_on = None
airflow_version = '2.4.0'


def upgrade():
    """Apply compare types between ORM and DB"""
    conn = op.get_bind()
    with op.batch_alter_table('log_template', schema=None) as batch_op:
        batch_op.alter_column(
            'created_at', existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=False
        )
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.alter_column(
            'trigger_timeout', existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=True
        )
    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        # drop server_default by not providing existing_server_default
        batch_op.alter_column(
            'dag_hash',
            existing_type=sa.String(32),
            server_default=None,
            type_=sa.String(32),
            existing_nullable=False,
        )
    # pool_slots server_default mistakenly dropped in 7b2661a43ba3 for postgresql.
    # existing_server_default not used in alter
    if conn.dialect.name == 'postgresql':
        with op.batch_alter_table('task_instance', schema=None) as batch_op:
            batch_op.alter_column(
                'pool_slots', existing_type=sa.Integer(), server_default=sa.text('1'), existing_nullable=False
            )


def downgrade():
    """Unapply compare types between ORM and DB"""
    with op.batch_alter_table('log_template', schema=None) as batch_op:
        batch_op.alter_column(
            'created_at', existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=False
        )
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.alter_column(
            'trigger_timeout', existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=True
        )
