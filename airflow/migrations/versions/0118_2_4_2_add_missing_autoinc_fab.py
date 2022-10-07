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

"""Add missing auto-increment to columns on FAB tables

Revision ID: b0d31815b5a6
Revises: ecb43d2a1842
Create Date: 2022-10-05 13:16:45.638490

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b0d31815b5a6'
down_revision = 'ecb43d2a1842'
branch_labels = None
depends_on = None
airflow_version = '2.4.2'


def upgrade():
    """Apply migration.

    If these columns are already of the right type (i.e. created by our
    migration in 1.10.13 rather than FAB itself in an earlier version), this
    migration will issue an alter statement to change them to what they already
    are -- i.e. its a no-op.

    These tables are small (100 to low 1k rows at most), so it's not too costly
    to change them.
    """
    conn = op.get_bind()
    if conn.dialect.name in ['mssql', 'sqlite']:
        # 1.10.12 didn't support SQL Server, so it couldn't have gotten this wrong --> nothing to correct
        # SQLite autoinc was "implicit" for an INTEGER NOT NULL PRIMARY KEY
        return

    for table in (
        'ab_permission',
        'ab_view_menu',
        'ab_role',
        'ab_permission_view',
        'ab_permission_view_role',
        'ab_user',
        'ab_user_role',
        'ab_register_user',
    ):
        with op.batch_alter_table(table) as batch:
            kwargs = {}
            if conn.dialect.name == 'postgresql':
                kwargs['type_'] = sa.Sequence(f'{table}_id_seq').next_value()
            else:
                kwargs['autoincrement'] = True
            batch.alter_column("id", existing_type=sa.Integer(), existing_nullable=False, **kwargs)


def downgrade():
    """Unapply add_missing_autoinc_fab"""
    # No downgrade needed, these _should_ have applied from 1.10.13 but didn't due to a previous bug!
