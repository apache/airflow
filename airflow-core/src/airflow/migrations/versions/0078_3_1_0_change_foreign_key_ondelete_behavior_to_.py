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
Change foreign key ondelete behavior to cascade for backfill and task instance to fix the error while deleting dags.

Revision ID: 933ae596918b
Revises: 40f7c30a228b
Create Date: 2025-07-18 13:42:06.887463

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "933ae596918b"
down_revision = "40f7c30a228b"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply change_foreign_key_ondelete_behavior_to_cascade_for_backfill_and_task_instance."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_run_backfill_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("dag_run_backfill_id_fkey"), "backfill", ["backfill_id"], ["id"], ondelete="CASCADE"
        )

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("task_instance_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade():
    """Unapply change_foreign_key_ondelete_behavior_to_cascade_for_backfill_and_task_instance."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("task_instance_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="RESTRICT",
        )

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_run_backfill_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("dag_run_backfill_id_fkey"), "backfill", ["backfill_id"], ["id"]
        )
