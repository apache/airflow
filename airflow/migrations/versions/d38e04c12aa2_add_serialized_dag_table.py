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

"""add serialized_dag table

Revision ID: d38e04c12aa2
Revises: 6e96a59344a4
Create Date: 2019-08-01 14:39:35.616417

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd38e04c12aa2'
down_revision = '6e96a59344a4'
branch_labels = None
depends_on = None


def upgrade():
    """Upgrade version."""
    op.create_table('serialized_dag',  # pylint: disable=no-member
                    sa.Column('dag_id', sa.String(length=250), nullable=False),
                    sa.Column('fileloc', sa.String(length=2000), nullable=False),
                    sa.Column('fileloc_hash', sa.Integer(), nullable=False),
                    sa.Column('data', sa.Text(), nullable=False),
                    sa.Column('last_updated', sa.DateTime(), nullable=False),
                    sa.PrimaryKeyConstraint('dag_id'))
    op.create_index(   # pylint: disable=no-member
        'idx_fileloc_hash', 'serialized_dag', ['fileloc_hash'])


def downgrade():
    """Downgrade version."""
    op.drop_table('serialized_dag')   # pylint: disable=no-member
