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

"""add session table to db

Revision ID: c381b21cb7e4
Revises: be2bfac3da23
Create Date: 2022-01-25 13:56:35.069429

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'c381b21cb7e4'
down_revision = 'be2bfac3da23'
branch_labels = None
depends_on = None

TABLE_NAME = 'session'


def upgrade():
    """Apply add session table to db"""
    op.create_table(
        TABLE_NAME,
        sa.Column('id', sa.Integer()),
        sa.Column('session_id', sa.String(255)),
        sa.Column('data', sa.LargeBinary()),
        sa.Column('expiry', sa.DateTime()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('session_id'),
    )


def downgrade():
    """Unapply add session table to db"""
    op.drop_table(TABLE_NAME)
