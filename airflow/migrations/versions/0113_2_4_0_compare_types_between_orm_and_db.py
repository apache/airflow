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

"""compare types between ORM and DB.

Revision ID: 44b7034f6bdc
Revises: 424117c37d18
Create Date: 2022-05-31 09:16:44.558754

"""

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import TIMESTAMP

# revision identifiers, used by Alembic.
revision = '44b7034f6bdc'
down_revision = '424117c37d18'
branch_labels = None
depends_on = None
airflow_version = '2.4.0'


def upgrade():
    """Apply compare types between ORM and DB."""
    conn = op.get_bind()
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column(
            'extra',
            existing_type=sa.TEXT(),
            type_=sa.Text(),
            existing_nullable=True,
        )
    with op.batch_alter_table('log_template', schema=None) as batch_op:
        batch_op.alter_column(
            'created_at', existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=False
        )

    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        # drop server_default
        batch_op.alter_column(
            'dag_hash',
            existing_type=sa.String(32),
            server_default=None,
            type_=sa.String(32),
            existing_nullable=False,
        )
    with op.batch_alter_table('trigger', schema=None) as batch_op:
        batch_op.alter_column(
            'created_date', existing_type=sa.DateTime(), type_=TIMESTAMP(), existing_nullable=False
        )

    if conn.dialect.name != 'sqlite':
        return
    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        batch_op.alter_column('fileloc_hash', existing_type=sa.Integer, type_=sa.BigInteger())
    # Some sqlite date are not in db_types.TIMESTAMP. Convert these to TIMESTAMP.
    with op.batch_alter_table('dag', schema=None) as batch_op:
        batch_op.alter_column(
            'last_pickled', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'last_expired', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('dag_pickle', schema=None) as batch_op:
        batch_op.alter_column(
            'created_dttm', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.alter_column(
            'execution_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=False
        )
        batch_op.alter_column(
            'start_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'end_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('import_error', schema=None) as batch_op:
        batch_op.alter_column(
            'timestamp', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('job', schema=None) as batch_op:
        batch_op.alter_column(
            'start_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'end_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'latest_heartbeat', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('log', schema=None) as batch_op:
        batch_op.alter_column('dttm', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True)
        batch_op.alter_column(
            'execution_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        batch_op.alter_column(
            'last_updated', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=False
        )

    with op.batch_alter_table('sla_miss', schema=None) as batch_op:
        batch_op.alter_column(
            'execution_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=False
        )
        batch_op.alter_column(
            'timestamp', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('task_fail', schema=None) as batch_op:
        batch_op.alter_column(
            'start_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'end_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )

    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.alter_column(
            'start_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'end_date', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )
        batch_op.alter_column(
            'queued_dttm', existing_type=sa.DATETIME(), type_=TIMESTAMP(), existing_nullable=True
        )


def downgrade():
    """Unapply compare types between ORM and DB."""
    with op.batch_alter_table('connection', schema=None) as batch_op:
        batch_op.alter_column(
            'extra',
            existing_type=sa.Text(),
            type_=sa.TEXT(),
            existing_nullable=True,
        )
    with op.batch_alter_table('log_template', schema=None) as batch_op:
        batch_op.alter_column(
            'created_at', existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=False
        )
    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        # add server_default
        batch_op.alter_column(
            'dag_hash',
            existing_type=sa.String(32),
            server_default='Hash not calculated yet',
            type_=sa.String(32),
            existing_nullable=False,
        )
    with op.batch_alter_table('trigger', schema=None) as batch_op:
        batch_op.alter_column(
            'created_date', existing_type=TIMESTAMP(), type_=sa.DateTime(), existing_nullable=False
        )
    conn = op.get_bind()

    if conn.dialect.name != 'sqlite':
        return
    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        batch_op.alter_column('fileloc_hash', existing_type=sa.BigInteger, type_=sa.Integer())
    # Change these column back to sa.DATETIME()
    with op.batch_alter_table('task_instance', schema=None) as batch_op:
        batch_op.alter_column(
            'queued_dttm', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'end_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'start_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )

    with op.batch_alter_table('task_fail', schema=None) as batch_op:
        batch_op.alter_column(
            'end_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'start_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )

    with op.batch_alter_table('sla_miss', schema=None) as batch_op:
        batch_op.alter_column(
            'timestamp', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'execution_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=False
        )

    with op.batch_alter_table('serialized_dag', schema=None) as batch_op:
        batch_op.alter_column(
            'last_updated', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=False
        )

    with op.batch_alter_table('log', schema=None) as batch_op:
        batch_op.alter_column(
            'execution_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column('dttm', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True)

    with op.batch_alter_table('job', schema=None) as batch_op:
        batch_op.alter_column(
            'latest_heartbeat', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'end_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'start_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )

    with op.batch_alter_table('import_error', schema=None) as batch_op:
        batch_op.alter_column(
            'timestamp', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )

    with op.batch_alter_table('dag_run', schema=None) as batch_op:
        batch_op.alter_column(
            'end_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'start_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'execution_date', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=False
        )

    with op.batch_alter_table('dag_pickle', schema=None) as batch_op:
        batch_op.alter_column(
            'created_dttm', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )

    with op.batch_alter_table('dag', schema=None) as batch_op:
        batch_op.alter_column(
            'last_expired', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
        batch_op.alter_column(
            'last_pickled', existing_type=TIMESTAMP(), type_=sa.DATETIME(), existing_nullable=True
        )
