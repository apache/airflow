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

"""add root_dag_id to DAG

Revision ID: b3b105409875
Revises: d38e04c12aa2
Create Date: 2019-09-28 23:20:01.744775

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b3b105409875'
down_revision = 'd38e04c12aa2'
branch_labels = None
depends_on = None


def upgrade():
    """Apply add root_dag_id to DAG"""
    op.add_column('dag', sa.Column('root_dag_id', sa.String(length=250), nullable=True))
    op.create_index('idx_root_dag_id', 'dag', ['root_dag_id'], unique=False)


def downgrade():
    """Unapply add root_dag_id to DAG"""
    op.drop_index('idx_root_dag_id', table_name='dag')
    op.drop_column('dag', 'root_dag_id')
