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
Add task_group_retries to DagRun.

Records, per DagRun, how many times each retryable TaskGroup has already been
retried as a unit. The scheduler reads and increments this map to bound
TaskGroup-level retries (the ``retries`` argument on ``TaskGroup``); see
https://github.com/apache/airflow/issues/21867.

Revision ID: 0dd0802788b0
Revises: c4e7a1f9b2d0
Create Date: 2026-06-05 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import ExtendedJSON

revision = "0dd0802788b0"
down_revision = "c4e7a1f9b2d0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add ``task_group_retries`` to ``dag_run``."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("task_group_retries", ExtendedJSON, nullable=True))


def downgrade():
    """Drop ``task_group_retries`` from ``dag_run``."""
    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("task_group_retries")
