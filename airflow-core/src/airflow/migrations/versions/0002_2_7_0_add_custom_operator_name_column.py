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

"""
Add custom_operator_name column.

Revision ID: 788397e78828
Revises: 937cbd173ca1
Create Date: 2023-06-12 10:46:52.125149

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "788397e78828"
down_revision = "937cbd173ca1"
branch_labels = None
depends_on = None
airflow_version = "2.7.0"

TABLE_NAME = "task_instance"


def upgrade():
    """Apply Add custom_operator_name column."""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("custom_operator_name", sa.VARCHAR(length=1000), nullable=True))


def downgrade():
    """Unapply Add custom_operator_name column."""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_column("custom_operator_name")
