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

"""Add TaskTag table

Revision ID: 19d8a998c007
Revises: 952da73b5eff
Create Date: 2020-05-29 14:20:37.831692

"""

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

# revision identifiers, used by Alembic.
revision = '19d8a998c007'
down_revision = '952da73b5eff'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add TaskTag table"""
    op.create_table(
        'task_tag',
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('task_id', sa.String(length=250), nullable=False),
        sa.Column('execution_date', UtcDateTime, nullable=False),
        sa.ForeignKeyConstraint(('dag_id', 'task_id', 'execution_date'),
                                ('task_instance.dag_id',
                                 'task_instance.task_id',
                                 'task_instance.execution_date'),
                                ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('name', 'dag_id', 'task_id', 'execution_date')
    )


def downgrade():
    """Unapply Add TaskTag table"""
    op.drop_table('task_tag')
