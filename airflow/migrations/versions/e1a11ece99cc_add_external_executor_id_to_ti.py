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

"""Add external executor ID to TI

Revision ID: e1a11ece99cc
Revises: b247b1e3d1ed
Create Date: 2020-09-12 08:23:45.698865

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'e1a11ece99cc'
down_revision = 'b247b1e3d1ed'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add external executor ID to TI"""
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.add_column(sa.Column('external_executor_id', sa.String(length=250), nullable=True))


def downgrade():
    """Unapply Add external executor ID to TI"""
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.drop_column('external_executor_id')
