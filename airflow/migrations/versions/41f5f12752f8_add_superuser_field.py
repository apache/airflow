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

"""add superuser field

Revision ID: 41f5f12752f8
Revises: 03bc53e68815
Create Date: 2018-12-04 15:50:04.456875

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '41f5f12752f8'
down_revision = '03bc53e68815'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('users', sa.Column('superuser', sa.Boolean(), default=False))


def downgrade():
    op.drop_column('users', 'superuser')
