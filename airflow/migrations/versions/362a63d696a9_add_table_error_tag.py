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

"""add table error_tag

Revision ID: 362a63d696a9
Revises: fc0e0ab79365
Create Date: 2020-05-13 13:26:25.393514

"""

# revision identifiers, used by Alembic.
revision = '362a63d696a9'
down_revision = 'fc0e0ab79365'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'error_tag',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('label', sa.String(length=1000), nullable=False),
        sa.Column('value', sa.String(length=1000), nullable=False),
    )


def downgrade():
    op.drop_table('error_tag')

