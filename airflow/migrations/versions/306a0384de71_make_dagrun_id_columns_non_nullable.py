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

"""Make DagRun id columns non-nullable

Revision ID: 306a0384de71
Revises: 7b2661a43ba3
Create Date: 2021-10-07 12:24:49.209978

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '306a0384de71'
down_revision = '7b2661a43ba3'
branch_labels = None
depends_on = None


def _mssql_datetime():
    from sqlalchemy.dialects import mssql

    return mssql.DATETIME2(precision=6)


def upgrade():
    """Apply Make DagRun id columns non-nullable"""
    dialect_name = op.get_bind().dialect.name
    dt_type = _mssql_datetime() if dialect_name == "mssql" else sa.TIMESTAMP(timezone=True)

    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.alter_column('dag_id', existing_type=sa.VARCHAR(length=250), nullable=False)
        batch_op.alter_column('execution_date', existing_type=dt_type, nullable=False)
        batch_op.alter_column('run_id', existing_type=sa.VARCHAR(length=250), nullable=False)


def downgrade():
    """Unapply Make DagRun id columns non-nullable"""
    dialect_name = op.get_bind().dialect.name
    dt_type = _mssql_datetime() if dialect_name == "mssql" else sa.TIMESTAMP(timezone=True)

    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.alter_column('run_id', existing_type=sa.VARCHAR(length=250), nullable=True)
        batch_op.alter_column('execution_date', existing_type=dt_type, nullable=True)
        batch_op.alter_column('dag_id', existing_type=sa.VARCHAR(length=250), nullable=True)
