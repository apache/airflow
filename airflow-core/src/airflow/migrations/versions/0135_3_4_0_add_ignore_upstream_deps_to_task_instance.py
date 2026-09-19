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
Add ignore_upstream_deps to task_instance and task_instance_history.

Set by the clear-task-instances API when a user force-runs a task instance. Read by the
scheduler when it evaluates the instance's dependencies on other task instances; the per-try
snapshot in task_instance_history records which tries were forced.

Revision ID: 806e4be38d60
Revises: b6a9c2e7d410
Create Date: 2026-09-19 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "806e4be38d60"
down_revision = "b6a9c2e7d410"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add ignore_upstream_deps to task_instance and task_instance_history."""
    for table in ("task_instance", "task_instance_history"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.add_column(
                sa.Column("ignore_upstream_deps", sa.Boolean(), nullable=False, server_default="0")
            )


def downgrade():
    """Remove ignore_upstream_deps from task_instance and task_instance_history."""
    for table in ("task_instance", "task_instance_history"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.drop_column("ignore_upstream_deps")
