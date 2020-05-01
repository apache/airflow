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

"""add sla_checked in dagrun

Revision ID: 0c65beae7fa7
Revises: 14d508160edd
Create Date: 2020-05-01 00:54:33.298760

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '0c65beae7fa7'
down_revision = '14d508160edd'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add sla_checked in dagrun"""
    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.add_column(sa.Column('sla_checked', sa.Boolean(), nullable=True))


def downgrade():
    """Unapply add sla_checked in dagrun"""
    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.drop_column('sla_checked')
