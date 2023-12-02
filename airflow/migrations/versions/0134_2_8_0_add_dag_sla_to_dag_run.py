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

"""add dag sla to dag_run

Revision ID: 106e2751d462
Revises: 10b52ebd31f7
Create Date: 2023-12-02 14:48:12.289123

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '106e2751d462'
down_revision = '10b52ebd31f7'
branch_labels = None
depends_on = None
airflow_version = '2.8.0'


def upgrade():
    """Apply Add has_import_errors column to dag_run"""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(sa.Column("sla_missed", sa.Boolean))
    # Different databases support different literal for FALSE. This is fine.
    op.execute(sa.text(f"UPDATE dag_run SET sla_missed = {sa.false().compile(op.get_bind())}"))
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.alter_column("sla_missed", existing_type=sa.Boolean, nullable=False)


def downgrade():
    """Unapply add dag sla to dag_run"""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_column("sla_missed", mssql_drop_default=True)
