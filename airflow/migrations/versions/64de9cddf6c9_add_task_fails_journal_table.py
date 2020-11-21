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

"""add task fails journal table

Revision ID: 64de9cddf6c9
Revises: 211e584da130
Create Date: 2016-08-03 14:02:59.203021

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = '64de9cddf6c9'
down_revision = '211e584da130'
branch_labels = None
depends_on = None


def upgrade():  # noqa: D103
    op.create_table(
        'task_fail',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('task_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('execution_date', sa.DateTime(), nullable=False),
        sa.Column('start_date', sa.DateTime(), nullable=True),
        sa.Column('end_date', sa.DateTime(), nullable=True),
        sa.Column('duration', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():  # noqa: D103
    op.drop_table('task_fail')
