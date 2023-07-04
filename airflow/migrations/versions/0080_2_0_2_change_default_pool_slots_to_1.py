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
"""Change default ``pool_slots`` to ``1``

Revision ID: 8646922c8a04
Revises: 449b4072c2da
Create Date: 2021-02-23 23:19:22.409973

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8646922c8a04"
down_revision = "449b4072c2da"
branch_labels = None
depends_on = None
airflow_version = "2.0.2"


def upgrade():
    """Change default ``pool_slots`` to ``1`` and make pool_slots not nullable"""
    op.execute("UPDATE task_instance SET pool_slots = 1 WHERE pool_slots IS NULL")
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.alter_column("pool_slots", existing_type=sa.Integer, nullable=False, server_default="1")


def downgrade():
    """Unapply Change default ``pool_slots`` to ``1``"""
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        inspector = sa.inspect(conn.engine)
        columns = inspector.get_columns("task_instance")
        for col in columns:
            if col["name"] == "pool_slots" and col["default"] == "('1')":
                with op.batch_alter_table("task_instance", schema=None) as batch_op:
                    batch_op.alter_column(
                        "pool_slots", existing_type=sa.Integer, nullable=True, server_default=None
                    )
    else:
        with op.batch_alter_table("task_instance", schema=None) as batch_op:
            batch_op.alter_column("pool_slots", existing_type=sa.Integer, nullable=True, server_default=None)
