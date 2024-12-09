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
add ti_id to task instance table.

Revision ID: cc02d476ee0f
Revises: eed27faa34e3
Create Date: 2024-12-08 22:16:15.636742

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "cc02d476ee0f"
down_revision = "eed27faa34e3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"

table_names = [
    "task_instance_note",
    "task_map",
    "xcom",
    "task_reschedule",
    "rendered_task_instance_fields",
]


def upgrade():
    """Apply add ti_id to task instance table."""
    for table_name in table_names:
        op.add_column(
            table_name,
            sa.Column(
                "ti_id",
                sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
                nullable=False,
            ),
        )

        # Update the ti_id column using the task_instance table
        op.execute(f"""
            UPDATE {table_name}
            SET ti_id = task_instance.id
            FROM task_instance
            WHERE xcom.run_id = task_instance.run_id
            AND xcom.task_id = task_instance.task_id
            AND xcom.dag_id = task_instance.dag_id
        """)

        op.create_foreign_key(
            f"{table_name}_ti_fkey", table_name, "task_instance", ["ti_id"], ["id"], ondelete="CASCADE"
        )


def downgrade():
    """Revert add ti_id to task instance table."""
    for table_name in table_names:
        op.drop_constraint(f"{table_name}_ti_fkey", table_name, type_="foreignkey")

        op.drop_column(table_name, "ti_id")
