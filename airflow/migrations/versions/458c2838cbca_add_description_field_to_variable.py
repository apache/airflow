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

"""add description field to variable

Revision ID: 458c2838cbca
Revises: 2e42bb497a22
Create Date: 2021-04-01 21:38:38.030300

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '458c2838cbca'
down_revision = '2e42bb497a22'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add description field to variable"""
    with op.batch_alter_table('variable') as batch_op:
        batch_op.add_column(sa.Column('description', sa.String(length=5000), nullable=True))


def downgrade():
    """Unapply add description field to variable"""
    with op.batch_alter_table('variable', schema=None) as batch_op:
        batch_op.drop_column('description')
