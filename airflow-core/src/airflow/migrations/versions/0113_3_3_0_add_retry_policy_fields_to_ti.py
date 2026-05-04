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
Add retry_delay_override and retry_reason to task_instance.

These nullable columns store retry policy overrides set by the task
worker when a RetryPolicy is configured.  On the live row they are
read by next_retry_datetime() and cleared when the task enters
RUNNING; the per-try snapshot in task_instance_history preserves the
audit trail of "why did the policy decide N seconds, reason X" for
each historical try.

On PostgreSQL and MySQL 8+, adding nullable columns without defaults
is a metadata-only operation (no table rewrite).

Revision ID: b8f3e4a1d2c9
Revises: fde9ed84d07b
Create Date: 2026-04-16 12:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "b8f3e4a1d2c9"
down_revision = "fde9ed84d07b"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add retry policy fields to task_instance and task_instance_history."""
    for table in ("task_instance", "task_instance_history"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.add_column(sa.Column("retry_delay_override", sa.Float, nullable=True))
            batch_op.add_column(sa.Column("retry_reason", sa.String(500), nullable=True))


def downgrade():
    """Remove retry policy fields from task_instance and task_instance_history."""
    for table in ("task_instance", "task_instance_history"):
        with op.batch_alter_table(table, schema=None) as batch_op:
            batch_op.drop_column("retry_reason")
            batch_op.drop_column("retry_delay_override")
