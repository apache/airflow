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

"""Increase connection.extra field size

Revision ID: 33c24d6e80ed
Revises: 952da73b5eff
Create Date: 2020-03-16 13:09:35.445176

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '33c24d6e80ed'
down_revision = '952da73b5eff'
branch_labels = None
depends_on = None


def upgrade():
    """Apply increase_length_for_connection_password"""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column('extra',
                              existing_type=sa.VARCHAR(length=5000),
                              type_=sa.String(length=10000),
                              existing_nullable=True)


def downgrade():
    """Unapply increase_length_for_connection_password"""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column('extra',
                              existing_type=sa.String(length=10000),
                              type_=sa.VARCHAR(length=5000),
                              existing_nullable=True)
