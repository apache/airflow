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
Add partition_date to asset_partition_dag_run.

The target datetime is frozen at APDR creation time so the consumer DagRun's
``partition_date`` is consistent with the partition mapper that produced its
``partition_key``.

Revision ID: d2f4e1b3c5a7
Revises: 9ff64e1c35d3
Create Date: 2026-05-21 09:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import UtcDateTime

revision = "d2f4e1b3c5a7"
down_revision = "9ff64e1c35d3"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add partition_date column to asset_partition_dag_run."""
    with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("partition_date", UtcDateTime, nullable=True))


def downgrade():
    """Remove partition_date column from asset_partition_dag_run."""
    with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
        batch_op.drop_column("partition_date")
