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
Add pending_partition_key to asset_partition_dag_run and enforce single pending row per key.

Asset events from two producer assets that map to the same downstream partition key
could each create a pending AssetPartitionDagRun (APDR), splitting the events between
two rows that each wait forever for the other's events. ``pending_partition_key``
mirrors ``partition_key`` on a pending row, and a unique constraint on
(target_dag_id, pending_partition_key) stops a second one from being created. It stands
in for a partial unique index, which MySQL does not support.

Existing pending rows are neither merged nor deleted: a Dag without ``batch_asset_events``
legitimately has several pending rows for one key, one per event. Only the latest (highest id)
pending row per (target_dag_id, partition_key) is marked; older duplicates keep a NULL
``pending_partition_key``, which the unique constraint treats as distinct, so they do not
conflict. A pending row split by the old race is not healed by this migration. This is plain
SQL, so it behaves the same in offline mode.

Revision ID: ee86eed19e24
Revises: 9cffe90fa0c0
Create Date: 2026-08-04 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.db_types import StringID
from airflow.migrations.utils import disable_sqlite_fkeys

revision = "ee86eed19e24"
down_revision = "9cffe90fa0c0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_TABLE = "asset_partition_dag_run"
_UQ_NAME = "apdr_target_dag_id_pending_partition_key_uq"


def upgrade():
    """Add pending_partition_key to asset_partition_dag_run and enforce single pending row per key."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.add_column(sa.Column("pending_partition_key", StringID(), nullable=True))

        # The derived table lets MySQL read the table it updates.
        op.execute(
            f"UPDATE {_TABLE} SET pending_partition_key = partition_key WHERE id IN ("
            "SELECT latest_id FROM ("
            f"SELECT MAX(id) AS latest_id FROM {_TABLE} WHERE created_dag_run_id IS NULL "
            "GROUP BY target_dag_id, partition_key"
            ") latest_pending)"
        )

        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.create_unique_constraint(_UQ_NAME, ["target_dag_id", "pending_partition_key"])


def downgrade():
    """Drop the pending-partition uniqueness guard and pending_partition_key column."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.drop_constraint(_UQ_NAME, type_="unique")
            batch_op.drop_column("pending_partition_key")
