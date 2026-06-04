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
Add rollup_fingerprint to AssetPartitionDagRun and index partitioned_asset_key_log.asset_partition_dag_run_id.

The new ``rollup_fingerprint`` JSON column lets the scheduler discard provisional
partition Dag runs whose mapper / window definition has changed under them.
Unlike a ``dag_version_id`` UUID, this fingerprint captures only the rollup
definition (mapper + window for each partitioned asset), so unrelated Dag edits
(task changes, description updates) do not trigger a stale-APDR cleanup.
The index on ``partitioned_asset_key_log.asset_partition_dag_run_id`` supports
the bulk DELETE that the stale-APDR cleanup runs against the log table
(see ``SchedulerJobRunner._create_dagruns_for_partitioned_asset_dags``).

Revision ID: dd5f3a8e2b91
Revises: c20871fbf23a
Create Date: 2026-05-28 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "dd5f3a8e2b91"
down_revision = "c20871fbf23a"
branch_labels = None
depends_on = None
airflow_version = "3.3.0"


def upgrade():
    """Add ``rollup_fingerprint`` to ``asset_partition_dag_run`` and index ``partitioned_asset_key_log.asset_partition_dag_run_id``."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.add_column(sa.Column("rollup_fingerprint", sa.JSON(), nullable=True))
        with op.batch_alter_table("partitioned_asset_key_log", schema=None) as batch_op:
            batch_op.create_index("idx_pakl_apdr_id", ["asset_partition_dag_run_id"], unique=False)


def downgrade():
    """Drop the ``partitioned_asset_key_log.asset_partition_dag_run_id`` index and the APDR ``rollup_fingerprint`` column."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("partitioned_asset_key_log", schema=None) as batch_op:
            batch_op.drop_index("idx_pakl_apdr_id")
        with op.batch_alter_table("asset_partition_dag_run", schema=None) as batch_op:
            batch_op.drop_column("rollup_fingerprint")
