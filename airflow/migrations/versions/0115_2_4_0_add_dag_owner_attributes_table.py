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

"""add dag_owner_attributes table

Revision ID: 679989279cf4
Revises: 0038cd0c28b4
Create Date: 2022-07-29 09:45:15.972777

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '679989279cf4'
down_revision = '0038cd0c28b4'
branch_labels = None
depends_on = None
airflow_version = '2.4.0'


def upgrade():
    """Apply Add ``DagOwnerAttributes`` table"""
    op.create_table(
        'dag_owner_attributes',
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('owner', sa.String(length=100), nullable=False),
        sa.Column('link', sa.String(length=500), nullable=False),
        sa.ForeignKeyConstraint(['dag_id'], ['dag.dag_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('dag_id', 'owner'),
    )


def downgrade():
    """Unapply Add Dataset model"""
    op.drop_table('dag_owner_attributes')
