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

"""support more types of import errors

Revision ID: 6baa51abb3d7
Revises: 2c6edca13270
Create Date: 2020-10-30 02:23:55.723031

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6baa51abb3d7'
down_revision = '2c6edca13270'
branch_labels = None
depends_on = None


def upgrade():
    """Apply support more types of import errors"""
    with op.batch_alter_table('import_error', schema=None) as batch_op:
        batch_op.add_column(sa.Column('category', sa.String(length=64), nullable=False))
        batch_op.add_column(sa.Column('error_hash', sa.BigInteger(), nullable=False))
        batch_op.create_index(batch_op.f('ix_import_error_error_hash'), ['error_hash'], unique=False)


def downgrade():
    """Unapply support more types of import errors"""
    with op.batch_alter_table('import_error', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_import_error_error_hash'))
        batch_op.drop_column('error_hash')
        batch_op.drop_column('category')
