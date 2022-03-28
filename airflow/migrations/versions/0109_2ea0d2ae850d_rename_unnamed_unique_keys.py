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

"""rename unnamed unique keys

Revision ID: 2ea0d2ae850d
Revises: 0152485aedf5
Create Date: 2022-03-25 19:06:16.134764

"""

import sqlalchemy as sa
from alembic import op
from alembic.operations.ops import CreateForeignKeyOp

from airflow.models.base import naming_convention

# revision identifiers, used by Alembic.
revision = '2ea0d2ae850d'
down_revision = '0152485aedf5'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'

TABLES_WITH_UNNAMED_UNIQUES = [
    'ab_permission',
    'ab_permission_view',
    'ab_permission_view_role',
    'ab_register_user',
    'ab_role',
    'ab_user',
    'ab_user_role',
    'ab_view_menu',
    'slot_pool',
    'variable',
]


def _drop_and_recreate(constraints, table_name, convention):
    for unique_cons in constraints:
        op.drop_constraint(unique_cons['name'], table_name, type_='unique')
        with op.batch_alter_table(table_name, naming_convention=convention) as batch_op:
            batch_op.create_unique_constraint(None, unique_cons['column_names'])


def drop_and_recreate_unique_key(insp, table, table_name, dialect='mysql', convention=None):
    fks = table.foreign_key_constraints
    for constraint in fks:
        op.drop_constraint(constraint.name, table_name, type_='foreignkey')
    if dialect == 'mysql':
        constraints = insp.get_unique_constraints(table_name)
        _drop_and_recreate(constraints, table_name, convention)
    elif dialect == 'mssql':
        constraints = insp.get_indexes(table_name)
        _drop_and_recreate(constraints, table_name, convention)
    for constraint in fks:
        op.invoke(CreateForeignKeyOp.from_constraint(constraint))


def upgrade():
    """Apply rename unnamed unique keys"""
    conn = op.get_bind()
    dialect_name = conn.engine.dialect.name
    with op.batch_alter_table('connection', naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint('unique_conn_id', type_='unique')
        batch_op.create_unique_constraint(None, ['conn_id'])
    if dialect_name not in ['mysql', 'mssql']:
        return
    meta = sa.MetaData(naming_convention=naming_convention)
    insp = sa.inspect(conn)

    for table_name in TABLES_WITH_UNNAMED_UNIQUES:
        t = sa.Table(table_name, meta, autoload_with=conn)
        drop_and_recreate_unique_key(insp, t, table_name, dialect=dialect_name, convention=naming_convention)


def downgrade():
    """Unapply rename unnamed unique keys"""
    with op.batch_alter_table('connection', naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint('connection_conn_id_key', type_='unique')
        batch_op.create_unique_constraint('unique_conn_id', ['conn_id'])
