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

When running `airflow db clean -t dag_version`, the cleanup process checks
foreign key references from task_instance and dag_run. These columns are
unindexed, so the checks can degrade into full table scans.

This migration adds the missing indexes to speed up dag_version cleanup.

See: https://github.com/apache/airflow/issues/60145

Revision ID: c9e9e8c38cc7
Revises: 6222ce48e289
Create Date: 2026-03-11 23:10:00.000000

"""

from __future__ import annotations

from alembic import op

revision = "c9e9e8c38cc7"
down_revision = "6222ce48e289"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


def upgrade():
    """Add dag_version cleanup indexes."""
    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.create_index("idx_task_instance_dag_version_id", ["dag_version_id"], unique=False)

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.create_index("idx_dag_run_created_dag_version_id", ["created_dag_version_id"], unique=False)


def downgrade():
    """Remove dag_version cleanup indexes."""
    op.drop_index("idx_dag_run_created_dag_version_id", table_name="dag_run", if_exists=True)
    op.drop_index("idx_task_instance_dag_version_id", table_name="task_instance", if_exists=True)
