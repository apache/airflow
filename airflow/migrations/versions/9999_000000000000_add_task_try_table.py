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

"""Add ``task_try`` table

Revision ID: 1b38cef5b76e
Revises: 52d714495f0
Create Date: 2015-10-27 08:31:48.475140

"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = '0000000000000'
down_revision = '0000000000000'
branch_labels = None
depends_on = None
airflow_version = '9.9.9'


def upgrade():
    op.create_table(
        'task_try',
        sa.Column('dag_id', StringID(), nullable=False),
        sa.Column('run_id', StringID(), nullable=False),
        sa.Column('task_id', StringID(), nullable=False),
        sa.Column('try_number', sa.Integer(), nullable=False),
        sa.Column('hostname', sa.String(length=1000), nullable=True),
        sa.PrimaryKeyConstraint('dag_id', 'task_id', 'run_id', 'try_number'),
    )


def downgrade():
    op.drop_table('task_try')
