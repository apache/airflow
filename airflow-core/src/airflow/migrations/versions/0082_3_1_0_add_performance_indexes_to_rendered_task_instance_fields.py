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

Revision ID: a169942745c2oqg3edt6yt
Revises: a169942745c2
Create Date: 2025-08-14 10:30:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "8eoqg3edt6yt"
down_revision = "a169942745c2"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Add performance indexes to rendered_task_instance_fields table."""
    op.create_index("idx_rtif_dag_task", "rendered_task_instance_fields", ["dag_id", "task_id"], unique=False)
    op.create_index(
        "idx_rtif_dag_task_run",
        "rendered_task_instance_fields",
        ["dag_id", "task_id", "run_id"],
        unique=False,
    )
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
