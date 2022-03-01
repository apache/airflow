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

"""add max_active_runs_reached to DagModel

Revision ID: 2c65a8cad3ff
Revises: c306b5b5ae4a
Create Date: 2022-03-01 11:48:03.612293

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '2c65a8cad3ff'
down_revision = 'c306b5b5ae4a'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


def upgrade():
    """Apply add ``max_active_runs_reached`` to DagModel"""
    with op.batch_alter_table('dag', schema=None) as batch_op:
        batch_op.add_column(sa.Column('max_active_runs_reached', sa.Boolean(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    """Unapply add ``max_active_runs_reached`` to DagModel"""
    with op.batch_alter_table('dag', schema=None) as batch_op:
        batch_op.drop_column('max_active_runs_reached')
