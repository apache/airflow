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

"""drop_user_and_chart

Revision ID: cf5dc11e79ad
Revises: 41f5f12752f8
Create Date: 2019-01-24 15:30:35.834740

"""
from alembic import op
from sqlalchemy.dialects import mysql
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cf5dc11e79ad'
down_revision = '41f5f12752f8'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table("chart")
    op.drop_table("users")


def downgrade():
    conn = op.get_bind()

    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('username', sa.String(length=250), nullable=True),
        sa.Column('email', sa.String(length=500), nullable=True),
        sa.Column('password', sa.String(255)),
        sa.Column('superuser', sa.Boolean(), default=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('username')
    )

    op.create_table(
        'chart',
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
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    if conn.dialect.name == 'mysql':
        conn.execute("SET time_zone = '+00:00'")
        op.alter_column(table_name='chart', column_name='last_modified', type_=mysql.TIMESTAMP(fsp=6))
    else:
        if conn.dialect.name in ('sqlite', 'mssql'):
            return

        if conn.dialect.name == 'postgresql':
            conn.execute("set timezone=UTC")

        op.alter_column(table_name='chart', column_name='last_modified', type_=sa.TIMESTAMP(timezone=True))
