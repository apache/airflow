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

"""fix sqlite foreign key

Revision ID: 856955da8476
Revises: f23433877c24
Create Date: 2018-06-17 15:54:53.844230

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '856955da8476'
down_revision = 'f23433877c24'
branch_labels = None
depends_on = None


def upgrade():
    """Fix broken foreign-key constraint for existing SQLite DBs."""
    conn = op.get_bind()
    if conn.dialect.name == 'sqlite':
        # Fix broken foreign-key constraint for existing SQLite DBs.
        #
        # Re-define tables and use copy_from to avoid reflection
        # which would fail because referenced user table doesn't exist.
        #
        # Use batch_alter_table to support SQLite workaround.
        chart_table = sa.Table(
            'chart',
            sa.MetaData(),
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('label', sa.String(length=200), nullable=True),
            sa.Column('conn_id', sa.String(length=250), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=True),
            sa.Column('chart_type', sa.String(length=100), nullable=True),
            sa.Column('sql_layout', sa.String(length=50), nullable=True),
            sa.Column('sql', sa.Text(), nullable=True),
            sa.Column('y_log_scale', sa.Boolean(), nullable=True),
            sa.Column('show_datatable', sa.Boolean(), nullable=True),
            sa.Column('show_sql', sa.Boolean(), nullable=True),
            sa.Column('height', sa.Integer(), nullable=True),
            sa.Column('default_params', sa.String(length=5000), nullable=True),
            sa.Column('x_is_date', sa.Boolean(), nullable=True),
            sa.Column('iteration_no', sa.Integer(), nullable=True),
            sa.Column('last_modified', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
        )
        with op.batch_alter_table('chart', copy_from=chart_table) as batch_op:
            batch_op.create_foreign_key('chart_user_id_fkey', 'users', ['user_id'], ['id'])


def downgrade():  # noqa: D103
    # Downgrade would fail because the broken FK constraint can't be re-created.
    pass
