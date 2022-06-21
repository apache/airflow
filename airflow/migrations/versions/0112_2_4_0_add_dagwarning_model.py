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

"""Add DagWarning model

Revision ID: 424117c37d18
Revises: 3c94c427fdf6
Create Date: 2022-04-27 15:57:36.736743
"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.


revision = '424117c37d18'
down_revision = 'f5fcbda3e651'
branch_labels = None
depends_on = None
airflow_version = '2.4.0'


def upgrade():
    """Apply Add DagWarning model"""
    op.create_table(
        'dag_warning',
        sa.Column('dag_id', StringID(), primary_key=True),
        sa.Column('warning_type', sa.String(length=50), primary_key=True),
        sa.Column('message', sa.String(1000), nullable=False),
        sa.Column('timestamp', TIMESTAMP, nullable=False),
        sa.ForeignKeyConstraint(
            ('dag_id',),
            ['dag.dag_id'],
            name='dcw_dag_id_fkey',
            ondelete='CASCADE',
        ),
    )


def downgrade():
    """Unapply Add DagWarning model"""
    op.drop_table('dag_warning')
