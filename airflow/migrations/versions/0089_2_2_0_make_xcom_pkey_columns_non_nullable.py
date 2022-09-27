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
"""Make XCom primary key columns non-nullable

Revision ID: e9304a3141f0
Revises: 83f031fd9f1c
Create Date: 2021-04-06 13:22:02.197726

"""
from __future__ import annotations

from alembic import op

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = 'e9304a3141f0'
down_revision = '83f031fd9f1c'
branch_labels = None
depends_on = None
airflow_version = '2.2.0'


def upgrade():
    """Apply Make XCom primary key columns non-nullable"""
    conn = op.get_bind()
    with op.batch_alter_table('xcom') as bop:
        bop.alter_column("key", type_=StringID(length=512), nullable=False)
        bop.alter_column("execution_date", type_=TIMESTAMP, nullable=False)
        if conn.dialect.name == 'mssql':
            bop.create_primary_key('pk_xcom', ['dag_id', 'task_id', 'key', 'execution_date'])


def downgrade():
    """Unapply Make XCom primary key columns non-nullable"""
    conn = op.get_bind()
    with op.batch_alter_table('xcom') as bop:
        # regardless of what the model defined, the `key` and `execution_date`
        # columns were always non-nullable for mysql, sqlite and postgres, so leave them alone

        if conn.dialect.name == 'mssql':
            bop.drop_constraint('pk_xcom', 'primary')
            # execution_date and key wasn't nullable in the other databases
            bop.alter_column("key", type_=StringID(length=512), nullable=True)
            bop.alter_column("execution_date", type_=TIMESTAMP, nullable=True)
