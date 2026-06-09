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
Add indexes on dag_run.created_dag_version_id and task_instance.dag_version_id.

Revision ID: 9ff64e1c35d3
Revises: dd5f3a8e2b91
Create Date: 2026-04-06 22:54:17.279733
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "9ff64e1c35d3"
down_revision = "dd5f3a8e2b91"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Apply Add indexes on dag_run.created_dag_version_id and task_instance.dag_version_id."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.create_index("idx_dag_run_created_dag_version_id", ["created_dag_version_id"], unique=False)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index("ti_dag_version_id", ["dag_version_id"], unique=False)


def downgrade():
    """Unapply Add indexes on dag_run.created_dag_version_id and task_instance.dag_version_id."""
    conn = op.get_bind()

    with op.batch_alter_table("task_instance") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
            batch_op.drop_index("ti_dag_version_id")
            batch_op.create_foreign_key(
                batch_op.f("task_instance_dag_version_id_fkey"),
                "dag_version",
                ["dag_version_id"],
                ["id"],
                ondelete="RESTRICT",
            )
        else:
            batch_op.drop_index("ti_dag_version_id")

    with op.batch_alter_table("dag_run") as batch_op:
        if conn.dialect.name == "mysql":
            batch_op.drop_constraint(batch_op.f("created_dag_version_id_fkey"), type_="foreignkey")
            batch_op.drop_index("idx_dag_run_created_dag_version_id")
            batch_op.create_foreign_key(
                batch_op.f("created_dag_version_id_fkey"),
                "dag_version",
                ["created_dag_version_id"],
                ["id"],
                ondelete="SET NULL",
            )
        else:
            batch_op.drop_index("idx_dag_run_created_dag_version_id")
