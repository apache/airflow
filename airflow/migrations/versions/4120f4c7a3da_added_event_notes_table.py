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

"""Added event notes table

Revision ID: 4120f4c7a3da
Revises: 97cdd93827b8
Create Date: 2021-07-15 15:09:03.032937

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.base import COLLATION_ARGS

revision = '4120f4c7a3da'
down_revision = '97cdd93827b8'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Added event notes table"""
    op.create_table(
        'event_note',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250, **COLLATION_ARGS), nullable=False),
        sa.Column('task_id', sa.String(length=250, **COLLATION_ARGS), nullable=True),
        sa.Column('execution_date', sa.DateTime(), nullable=False),
        sa.Column("event", sa.String(length=30), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('owner', sa.String(length=50), nullable=True),
        sa.Column('event_note', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    """Unapply Added event notes table"""
    op.drop_table('event_note')
