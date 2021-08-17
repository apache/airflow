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

"""increase_length_for_connection_password

Revision ID: fe461863935f
Revises: 08364691d074
Create Date: 2019-12-08 09:47:09.033009

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'fe461863935f'
down_revision = '08364691d074'
branch_labels = None
depends_on = None


def upgrade():
    """Apply increase_length_for_connection_password"""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column(
            'password',
            existing_type=sa.VARCHAR(length=500),
            type_=sa.String(length=5000),
            existing_nullable=True,
        )


def downgrade():
    """Unapply increase_length_for_connection_password"""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column(
            'password',
            existing_type=sa.String(length=5000),
            type_=sa.VARCHAR(length=500),
            existing_nullable=True,
        )
