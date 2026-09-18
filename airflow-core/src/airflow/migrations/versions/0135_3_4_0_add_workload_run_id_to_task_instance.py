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
Add workload_run_id to task_instance and task_instance_history.

Per-invocation UUID generated on every scheduler enqueue so executor
completion events can be matched to the attempt that produced them.
This prevents stale SUCCESS from a defer-exit worker from failing a
resumed attempt that shares the same TaskInstanceKey.

Revision ID: c7d4e8f1a203
Revises: b6a9c2e7d410
Create Date: 2026-09-18 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c7d4e8f1a203"
down_revision = "b6a9c2e7d410"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add workload_run_id to task_instance and task_instance_history."""
    for table in ("task_instance", "task_instance_history"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.add_column(sa.Column("workload_run_id", sa.Text(), nullable=True))


def downgrade():
    """Remove workload_run_id from task_instance and task_instance_history."""
    for table in ("task_instance", "task_instance_history"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.drop_column("workload_run_id")
