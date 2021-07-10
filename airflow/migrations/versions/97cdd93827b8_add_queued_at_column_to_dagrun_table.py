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

"""Add queued_at column to dagrun table

Revision ID: 97cdd93827b8
Revises: 30867afad44a
Create Date: 2021-06-29 21:53:48.059438

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mssql

# revision identifiers, used by Alembic.
revision = '97cdd93827b8'
down_revision = '30867afad44a'
branch_labels = None
depends_on = None


def upgrade():
    """Apply Add queued_at column to dagrun table"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        op.add_column('dag_run', sa.Column('queued_at', mssql.DATETIME2(precision=6), nullable=True))
    else:
        op.add_column('dag_run', sa.Column('queued_at', sa.DateTime(), nullable=True))


def downgrade():
    """Unapply Add queued_at column to dagrun table"""
    op.drop_column('dag_run', "queued_at")
