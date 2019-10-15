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

"""create heartbeat table

Revision ID: 20e66a0ce79c
Revises: 6c3619221807
Create Date: 2019-10-15 14:00:35.166383

"""

# revision identifiers, used by Alembic.
revision = '20e66a0ce79c'
down_revision = '6c3619221807'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'scheduler_heartbeat',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('dag_id', sa.String, nullable=False),
        sa.Column('last_heartbeat', sa.DateTime, nullable=False),
    )


def downgrade():
    op.drop_table('scheduler_heartbeat')
