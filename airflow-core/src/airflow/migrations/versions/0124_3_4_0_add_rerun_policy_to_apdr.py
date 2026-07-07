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
Add rerun_policy to AssetPartitionDagRun.

The ``rerun_policy`` column stores the RerunPolicy (as its string value) that
governs how the scheduler fires this provisional partition Dag run. A run stamped
``"refresh"`` fires immediately after an upstream partition was cleared and
re-run; everything else is stamped ``"hold"`` and goes through normal evaluation.

Revision ID: 623bce373cdf
Revises: d2f4e1b3c5a7
Create Date: 2026-06-17 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "623bce373cdf"
down_revision = "d2f4e1b3c5a7"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add ``rerun_policy`` to ``asset_partition_dag_run``."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.add_column(
                sa.Column("rerun_policy", sa.String(length=20), nullable=False, server_default="hold")
            )


def downgrade():
    """Drop the APDR ``rerun_policy`` column."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.drop_column("rerun_policy")
