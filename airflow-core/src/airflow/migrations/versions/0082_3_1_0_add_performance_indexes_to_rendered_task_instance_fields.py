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
Add performance indexes to rendered_task_instance_fields.

Revision ID: 3f8c5d2a1b9e
Revises: 808787349f22
Create Date: 2025-08-12 10:30:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "3f8c5d2a1b9e"
down_revision = "808787349f22"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add performance indexes to rendered_task_instance_fields table."""
    # Index for delete_old_records query pattern (dag_id, task_id)
    # This accelerates cleanup operations which filter by dag_id and task_id
    op.create_index("idx_rtif_dag_task", "rendered_task_instance_fields", ["dag_id", "task_id"], unique=False)

    # Index for run_id lookups within a dag/task context
    # This helps with queries that need to find records for a specific run
    op.create_index(
        "idx_rtif_dag_task_run",
        "rendered_task_instance_fields",
        ["dag_id", "task_id", "run_id"],
        unique=False,
    )

    # Covering index for common SELECT operations
    # This can serve many queries without accessing the main table
    # Including map_index to make it a covering index for the most common query pattern
    op.create_index(
        "idx_rtif_covering",
        "rendered_task_instance_fields",
        ["dag_id", "task_id", "run_id", "map_index"],
        unique=False,
    )


def downgrade():
    """Remove performance indexes from rendered_task_instance_fields table."""
    # Drop indexes in reverse order
    op.drop_index("idx_rtif_covering", "rendered_task_instance_fields")
    op.drop_index("idx_rtif_dag_task_run", "rendered_task_instance_fields")
    op.drop_index("idx_rtif_dag_task", "rendered_task_instance_fields")
