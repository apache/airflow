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

"""Add event timestamp to DDRQ

Revision ID: 2b72b0fd20ef
Revises: ee8d93fcc81e
Create Date: 2022-09-21 21:28:23.896961

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = '2b72b0fd20ef'
down_revision = 'ecb43d2a1842'
branch_labels = None
depends_on = None
airflow_version = '2.4.2'


def upgrade():
    """Apply Add event timestamp to DDRQ"""
    op.add_column('dataset_dag_run_queue', sa.Column('event_timestamp', TIMESTAMP, nullable=False))


def downgrade():
    """Unapply Add event timestamp to DDRQ"""
    with op.batch_alter_table('dataset_dag_run_queue', schema=None) as batch_op:
        batch_op.drop_column('event_timestamp')
