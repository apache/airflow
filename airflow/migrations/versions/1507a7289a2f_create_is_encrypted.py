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

"""create is_encrypted

Revision ID: 1507a7289a2f
Revises: e3a246e0dc1
Create Date: 2015-08-18 18:57:51.927315

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = '1507a7289a2f'
down_revision = 'e3a246e0dc1'
branch_labels = None
depends_on = None

connectionhelper = sa.Table(
    'connection',
    sa.MetaData(),
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('is_encrypted')
)


def upgrade():
    # first check if the user already has this done. This should only be
    # true for users who are upgrading from a previous version of Airflow
    # that predates Alembic integration
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)

    # this will only be true if 'connection' already exists in the db,
    # but not if alembic created it in a previous migration
    if 'connection' in inspector.get_table_names():
        col_names = [c['name'] for c in inspector.get_columns('connection')]
        if 'is_encrypted' in col_names:
            return

    op.add_column(
        'connection',
        sa.Column('is_encrypted', sa.Boolean, unique=False, default=False))

    conn = op.get_bind()
    conn.execute(
        connectionhelper.update().values(is_encrypted=False)
    )


def downgrade():
    op.drop_column('connection', 'is_encrypted')
