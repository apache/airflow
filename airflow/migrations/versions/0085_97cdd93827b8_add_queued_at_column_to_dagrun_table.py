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

"""Add ``queued_at`` column in ``dag_run`` table

Revision ID: 97cdd93827b8
Revises: a13f7613ad25
Create Date: 2021-06-29 21:53:48.059438

"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = '97cdd93827b8'
down_revision = 'a13f7613ad25'
branch_labels = None
depends_on = None
airflow_version = '2.1.3'


def upgrade():
    """Apply Add ``queued_at`` column in ``dag_run`` table"""
    op.add_column('dag_run', sa.Column('queued_at', TIMESTAMP, nullable=True))


def downgrade():
    """Unapply Add ``queued_at`` column in ``dag_run`` table"""
    with op.batch_alter_table('dag_run') as batch_op:
        batch_op.drop_column('queued_at')
