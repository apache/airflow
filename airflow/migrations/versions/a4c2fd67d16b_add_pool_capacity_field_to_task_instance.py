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

"""add pool_capacity field to task_instance

Revision ID: a4c2fd67d16b
Revises: fe461863935f
Create Date: 2019-12-31 13:50:02.227835

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a4c2fd67d16b'
down_revision = 'fe461863935f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('task_instance', sa.Column('pool_capacity', sa.Integer, default=1))


def downgrade():
    op.drop_column('task_instance', 'pool_capacity')
