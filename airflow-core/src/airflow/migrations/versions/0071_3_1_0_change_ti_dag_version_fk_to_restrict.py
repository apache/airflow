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
Change the on-delete behaviour of task_instance.dag_version_id foreign key constraint to RESTRICT.

Revision ID: 3ac9e5732b1f
Revises: 0242ac120002
Create Date: 2025-05-27 12:30:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "3ac9e5732b1f"
down_revision = "0242ac120002"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Alter task_instance.dag_version_id foreign key to use ON DELETE RESTRICT."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("task_instance_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="RESTRICT",
        )


def downgrade():
    """Revert task_instance.dag_version_id foreign key to ON DELETE CASCADE."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
        batch_op.create_foreign_key(
            batch_op.f("task_instance_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
