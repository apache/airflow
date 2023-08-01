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

"""Add is_setup to task_instance

Revision ID: 0646b768db47
Revises: 788397e78828
Create Date: 2023-07-24 10:12:07.630608

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "0646b768db47"
down_revision = "788397e78828"
branch_labels = None
depends_on = None
airflow_version = "2.7.0"


TABLE_NAME = "task_instance"


def upgrade():
    """Apply is_setup column to task_instance"""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("is_setup", sa.Boolean(), nullable=False, server_default="0"))


def downgrade():
    """Remove is_setup column from task_instance"""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_column("is_setup", mssql_drop_default=True)
