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

"""add last_scheduling_decision column to taskinstance

Revision ID: 3457dcfe0528
Revises: e655c0453f75
Create Date: 2021-12-20 21:35:33.133670

"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = '3457dcfe0528'
down_revision = 'e655c0453f75'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add last_scheduling_decision column to taskinstance"""
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.add_column(sa.Column('last_scheduling_decision', TIMESTAMP, nullable=True))
        batch_op.create_index('idx_ti_last_scheduling_decision', ['last_scheduling_decision'], unique=False)


def downgrade():
    """Unapply add last_scheduling_decision column to taskinstance"""
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.drop_index('idx_ti_last_scheduling_decision')
        batch_op.drop_column('last_scheduling_decision')
