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

"""create device type table

Revision ID: 65763ea18e7e
Revises: eb7c33dbe41b
Create Date: 2021-06-21 23:20:18.513831

"""

# revision identifiers, used by Alembic.
revision = '65763ea18e7e'
down_revision = 'eb7c33dbe41b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('device_type',
                    sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
                    sa.Column('name', sa.String(length=100), nullable=False, unique=True),
                    sa.Column('view_config', sa.String(length=500), nullable=True),
                    sa.PrimaryKeyConstraint('id'),
                    )


def downgrade():
    op.drop_table('device_type')
