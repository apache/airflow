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

Existing duplicate pending rows are merged before the constraint is created: each
older row's PartitionedAssetKeyLog rows are moved onto the latest (highest id) pending
row for the same key, so its Dag run can still be created, and the older row is deleted.
Offline mode cannot read the rows to merge them, so it only marks the latest pending row
per key.

Revision ID: ee86eed19e24
Revises: 9cffe90fa0c0
Create Date: 2026-08-04 00:00:00.000000

"""

from __future__ import annotations

from collections import defaultdict
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op

from airflow.migrations.db_types import StringID
from airflow.migrations.utils import disable_sqlite_fkeys

revision = "ee86eed19e24"
down_revision = "9cffe90fa0c0"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_TABLE = "asset_partition_dag_run"
_LOG_TABLE = "partitioned_asset_key_log"
_UQ_NAME = "apdr_target_dag_id_pending_partition_key_uq"


def _merge_duplicate_pending_apdrs(conn) -> None:
    """
    Merge duplicate pending rows for one (target_dag_id, partition_key) into the latest one.

    The older rows' PartitionedAssetKeyLog rows move to the latest row, so the asset events
    they recorded still count towards its Dag run, and the older rows are deleted. When the
    latest row carries no partition_date and the older rows agree on one, it takes that date.
    """
    pending_by_key = defaultdict(list)
    for row in conn.execute(
        sa.text(
            f"SELECT id, target_dag_id, partition_key, partition_date FROM {_TABLE} "
            "WHERE created_dag_run_id IS NULL ORDER BY id"
        )
    ):
        pending_by_key[(row.target_dag_id, row.partition_key)].append(row)

    for rows in pending_by_key.values():
        if len(rows) < 2:
            continue
        *older, latest = rows
        older_ids = [row.id for row in older]
        conn.execute(
            sa.text(
                f"UPDATE {_LOG_TABLE} SET asset_partition_dag_run_id = :latest_id "
                "WHERE asset_partition_dag_run_id IN :older_ids"
            ).bindparams(sa.bindparam("older_ids", expanding=True)),
            {"latest_id": latest.id, "older_ids": older_ids},
        )
        older_dates = {row.partition_date for row in older if row.partition_date is not None}
        if latest.partition_date is None and len(older_dates) == 1:
            conn.execute(
                sa.text(f"UPDATE {_TABLE} SET partition_date = :partition_date WHERE id = :latest_id"),
                {"partition_date": older_dates.pop(), "latest_id": latest.id},
            )
        conn.execute(
            sa.text(f"DELETE FROM {_TABLE} WHERE id IN :older_ids").bindparams(
                sa.bindparam("older_ids", expanding=True)
            ),
            {"older_ids": older_ids},
        )


def upgrade():
    """Add pending_partition_key to asset_partition_dag_run and enforce single pending row per key."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.add_column(sa.Column("pending_partition_key", StringID(), nullable=True))

        if context.is_offline_mode():
            print(
                dedent("""
                ------------
                --  WARNING: duplicate pending asset_partition_dag_run rows cannot be merged in
                --  offline mode; only the latest pending row per partition key keeps receiving
                --  asset events.
                ------------
                """)
            )
        else:
            _merge_duplicate_pending_apdrs(op.get_bind())

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
