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

"""make xcom pkey columns non-nullable

Revision ID: e9304a3141f0
Revises: 83f031fd9f1c
Create Date: 2021-04-06 13:22:02.197726

"""
from alembic import op

from airflow.migrations.db_types import TIMESTAMP, StringID

# revision identifiers, used by Alembic.
revision = 'e9304a3141f0'
down_revision = '83f031fd9f1c'
branch_labels = None
depends_on = None


def upgrade():
    """Apply make xcom pkey columns non-nullable"""
    conn = op.get_bind()
    with op.batch_alter_table('xcom') as bop:
        bop.alter_column("key", type_=StringID(length=512), nullable=False)
        bop.alter_column("execution_date", type_=TIMESTAMP, nullable=False)
        if conn.dialect.name == 'mssql':
            bop.create_primary_key('pk_xcom', ['dag_id', 'task_id', 'key', 'execution_date'])


def downgrade():
    """Unapply make xcom pkey columns non-nullable"""
    conn = op.get_bind()
    with op.batch_alter_table('xcom') as bop:
        if conn.dialect.name == 'mssql':
            bop.drop_constraint('pk_xcom', 'primary')
        bop.alter_column("key", type_=StringID(length=512), nullable=True)
        bop.alter_column("execution_date", type_=TIMESTAMP, nullable=True)
