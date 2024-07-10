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
Increase maximum length of pool name in ``task_instance`` table to ``256`` characters.

Revision ID: 90d1635d7b86
Revises: 2e42bb497a22
Create Date: 2021-04-05 09:37:54.848731

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "90d1635d7b86"
down_revision = "2e42bb497a22"
branch_labels = None
depends_on = None
airflow_version = "2.1.0"


def upgrade():
    """Apply Increase maximum length of pool name in ``task_instance`` table to ``256`` characters."""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.alter_column("pool", type_=sa.String(256), nullable=False)


def downgrade():
    """Unapply Increase maximum length of pool name in ``task_instance`` table to ``256`` characters."""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.drop_index("ti_pool")
            batch_op.alter_column("pool", type_=sa.String(50), nullable=False)
            batch_op.create_index("ti_pool", ["pool"])
    else:
        with op.batch_alter_table("task_instance") as batch_op:
            batch_op.alter_column("pool", type_=sa.String(50), nullable=False)
