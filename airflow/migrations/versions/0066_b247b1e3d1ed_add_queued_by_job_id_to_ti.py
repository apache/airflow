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

"""Add queued by Job ID to TI

Revision ID: b247b1e3d1ed
Revises: e38be357a868
Create Date: 2020-09-04 11:53:00.978882

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b247b1e3d1ed'
down_revision = 'e38be357a868'
branch_labels = None
depends_on = None
airflow_version = '2.0.0'


def upgrade():
    """Apply Add queued by Job ID to TI"""
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.add_column(sa.Column('queued_by_job_id', sa.Integer(), nullable=True))


def downgrade():
    """Unapply Add queued by Job ID to TI"""
    with op.batch_alter_table('task_instance') as batch_op:
        batch_op.drop_column('queued_by_job_id')
