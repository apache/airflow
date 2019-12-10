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

"""Increase length of password column in connection table

Revision ID: c1840b4bcf1a
Revises: 004c1210f153
Create Date: 2019-10-02 16:56:54.865550

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'c1840b4bcf1a'
down_revision = '004c1210f153'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == 'sqlite':
        # SQLite does not allow column modifications so we need to skip this migration
        return

    op.alter_column(table_name='connection',
                    column_name='password',
                    type_=sa.String(length=5000))


def downgrade():
    # Can't be undone
    pass
