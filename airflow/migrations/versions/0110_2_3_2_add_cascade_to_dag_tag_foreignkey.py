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
"""Add cascade to dag_tag foreign key

Revision ID: 3c94c427fdf6
Revises: 1de7bc13c950
Create Date: 2022-05-03 09:47:41.957710

"""
from __future__ import annotations

from alembic import op
from sqlalchemy import inspect

from airflow.migrations.utils import get_mssql_table_constraints

# revision identifiers, used by Alembic.
revision = '3c94c427fdf6'
down_revision = '1de7bc13c950'
branch_labels = None
depends_on = None
airflow_version = '2.3.2'


def upgrade():
    """Apply Add cascade to dag_tag foreignkey"""
    conn = op.get_bind()
    if conn.dialect.name in ['sqlite', 'mysql']:
        inspector = inspect(conn.engine)
        foreignkey = inspector.get_foreign_keys('dag_tag')
        with op.batch_alter_table(
            'dag_tag',
        ) as batch_op:
            batch_op.drop_constraint(foreignkey[0]['name'], type_='foreignkey')
            batch_op.create_foreign_key(
                "dag_tag_dag_id_fkey", 'dag', ['dag_id'], ['dag_id'], ondelete='CASCADE'
            )
    else:
        with op.batch_alter_table('dag_tag') as batch_op:
            if conn.dialect.name == 'mssql':
                constraints = get_mssql_table_constraints(conn, 'dag_tag')
                Fk, _ = constraints['FOREIGN KEY'].popitem()
                batch_op.drop_constraint(Fk, type_='foreignkey')
            if conn.dialect.name == 'postgresql':
                batch_op.drop_constraint('dag_tag_dag_id_fkey', type_='foreignkey')
            batch_op.create_foreign_key(
                "dag_tag_dag_id_fkey", 'dag', ['dag_id'], ['dag_id'], ondelete='CASCADE'
            )


def downgrade():
    """Unapply Add cascade to dag_tag foreignkey"""
    conn = op.get_bind()
    if conn.dialect.name == 'sqlite':
        with op.batch_alter_table('dag_tag') as batch_op:
            batch_op.drop_constraint('dag_tag_dag_id_fkey', type_='foreignkey')
            batch_op.create_foreign_key("fk_dag_tag_dag_id_dag", 'dag', ['dag_id'], ['dag_id'])
    else:
        with op.batch_alter_table('dag_tag') as batch_op:
            batch_op.drop_constraint('dag_tag_dag_id_fkey', type_='foreignkey')
            batch_op.create_foreign_key(
                None,
                'dag',
                ['dag_id'],
                ['dag_id'],
            )
