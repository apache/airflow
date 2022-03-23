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

"""rename unnamed unique and foreign keys

Revision ID: 0152485aedf5
Revises: f490ff2fb77b
Create Date: 2022-03-23 11:37:55.527724

"""

from alembic import op
from alembic.operations.ops import CreateForeignKeyOp
from sqlalchemy import MetaData, Table

from airflow.migrations.utils import exclude_table
from airflow.models.base import naming_convention

# revision identifiers, used by Alembic.
revision = '0152485aedf5'
down_revision = 'f490ff2fb77b'
branch_labels = None
depends_on = None
airflow_version = '2.3.0'


def upgrade():
    """Apply rename unnamed unique and foreign keys"""
    conn = op.get_bind()
    engine = conn.engine
    meta = MetaData(naming_convention=naming_convention)

    for table_name in engine.table_names():
        if table_name == 'alembic_version' or exclude_table(table_name):
            continue
        t = Table(table_name, meta, autoload_with=conn)
        for constraint in t.foreign_key_constraints:
            if constraint.name.endswith('_fkey'):
                continue
            op.drop_constraint(constraint.name, table_name, type_='foreignkey')
            constraint.name = None
            op.invoke(CreateForeignKeyOp.from_constraint(constraint))


def downgrade():
    """Unapply rename unnamed unique and foreign keys"""
    conn = op.get_bind()
    engine = conn.engine
    meta = MetaData()

    for table_name in engine.table_names():
        if table_name == 'alembic_version' or exclude_table(table_name):
            continue
        t = Table(table_name, meta, autoload_with=conn)
        for constraint in t.foreign_key_constraints:
            if constraint.name.endswith('_fkey'):
                continue
            op.drop_constraint(constraint.name, table_name, type_='foreignkey')
            constraint.name = None
            op.invoke(CreateForeignKeyOp.from_constraint(constraint))
