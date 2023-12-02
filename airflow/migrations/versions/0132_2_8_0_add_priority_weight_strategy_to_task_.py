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

"""add priority_weight_strategy to task_instance

Revision ID: 624ecf3b6a5e
Revises: bd5dfbe21f88
Create Date: 2023-10-29 02:01:34.774596

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "624ecf3b6a5e"
down_revision = "bd5dfbe21f88"
branch_labels = None
depends_on = None
airflow_version = "2.8.0"


def upgrade():
    json_type = sa.JSON
    conn = op.get_bind()
    if conn.dialect.name != "postgresql":
        # Mysql 5.7+/MariaDB 10.2.3 has JSON support. Rather than checking for
        # versions, check for the function existing.
        try:
            conn.execute(text("SELECT JSON_VALID(1)")).fetchone()
        except (sa.exc.OperationalError, sa.exc.ProgrammingError):
            json_type = sa.Text
    """Apply add priority_weight_strategy to task_instance"""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.add_column(sa.Column("priority_weight_strategy", json_type()))


def downgrade():
    """Unapply add priority_weight_strategy to task_instance"""
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_column("priority_weight_strategy")
