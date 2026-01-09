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
Add indexes on dag_version_id columns for db cleanup performance.

When running `airflow db clean -t dag_version`, the cleanup process needs to
check for foreign key references in task_instance and dag_run tables. Without
indexes on these columns, this results in full table scans that can take several
minutes per batch on tables with hundreds of thousands of rows.

This migration adds the missing indexes to speed up the cleanup operation.

See: https://github.com/apache/airflow/issues/60145

Revision ID: 62fb1d0a1252
Revises: 0b112f49112d
Create Date: 2025-01-09 12:00:00.000000

"""

from __future__ import annotations

from alembic import op

revision = "62fb1d0a1252"
down_revision = "0b112f49112d"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add indexes on dag_version_id columns to speed up db cleanup."""
    # task_instance.dag_version_id is used when cleaning dag_version records.
    # The FK constraint alone doesn't create an index on the referencing side.
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index(
            "idx_task_instance_dag_version_id",
            ["dag_version_id"],
            unique=False,
        )

    # dag_run.created_dag_version_id is also checked during dag_version cleanup.
    # Same situation - FK exists but no index on the column.
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.create_index(
            "idx_dag_run_created_dag_version_id",
            ["created_dag_version_id"],
            unique=False,
        )


def downgrade():
    """Remove dag_version_id indexes."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_index("idx_dag_run_created_dag_version_id")

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_index("idx_task_instance_dag_version_id")
