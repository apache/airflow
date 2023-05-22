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

"""Add ``onupdate`` cascade to ``task_map`` table

Revision ID: c804e5c76e3e
Revises: 98ae134e6fff
Create Date: 2023-05-19 23:30:57.368617

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "c804e5c76e3e"
down_revision = "98ae134e6fff"
branch_labels = None
depends_on = None
airflow_version = "2.6.2"


def upgrade():
    """Apply Add onupdate cascade to taskmap"""
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_constraint("task_map_task_instance_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_map_task_instance_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )


def downgrade():
    """Unapply Add onupdate cascade to taskmap"""
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_constraint("task_map_task_instance_fkey", type_="foreignkey")
        batch_op.create_foreign_key(
            "task_map_task_instance_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )
