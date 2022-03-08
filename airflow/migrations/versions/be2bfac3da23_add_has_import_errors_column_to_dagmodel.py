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

"""Add has_import_errors column to DagModel

Revision ID: be2bfac3da23
Revises: 7b2661a43ba3
Create Date: 2021-11-04 20:33:11.009547

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'be2bfac3da23'
down_revision = '7b2661a43ba3'
branch_labels = None
depends_on = None
airflow_version = '2.2.3'


def upgrade():
    """Apply Add has_import_errors column to DagModel"""
    op.add_column("dag", sa.Column("has_import_errors", sa.Boolean(), server_default='0'))


def downgrade():
    """Unapply Add has_import_errors column to DagModel"""
    with op.batch_alter_table('dag') as batch_op:
        batch_op.drop_column('has_import_errors', mssql_drop_default=True)
