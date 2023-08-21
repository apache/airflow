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

"""add trigger_timeout_reason to task_instance table

Revision ID: 064a46e082e5
Revises: 405de8318b3a
Create Date: 2023-08-22 00:28:21.752145

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "064a46e082e5"
down_revision = "405de8318b3a"
branch_labels = None
depends_on = None
airflow_version = "2.7.1"

TABLE_NAME = "task_instance"


def upgrade():
    """Apply add trigger_timeout_reason to task_instance table"""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("trigger_timeout_reason", sa.VARCHAR(length=256), nullable=True))


def downgrade():
    """Unapply add trigger_timeout_reason to task_instance table"""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_column("trigger_timeout_reason")
