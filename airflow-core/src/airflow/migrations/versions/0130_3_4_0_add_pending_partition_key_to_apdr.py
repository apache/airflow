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

Two asset events from different producer assets that resolve to the same downstream
partition key could each create their own AssetPartitionDagRun (APDR), leaving two
pending rows that could never both be satisfied (apache/airflow#71070). This is now
prevented with a unique constraint on (target_dag_id, pending_partition_key).

A partial/filtered unique index on (target_dag_id, partition_key) WHERE
created_dag_run_id IS NULL would be the more direct fix, but MySQL supports neither
partial nor filtered indexes. ``pending_partition_key`` is the portable equivalent: it
mirrors ``partition_key`` only while ``created_dag_run_id`` is null, and is null once
the dag run is created. A unique index treats null as distinct from every other value
on all three supported backends, so completed rows never collide with each other while
pending rows for the same key do.

Pre-existing duplicate pending rows can never both be satisfied -- the asset events
that would complete them are necessarily split across the duplicates -- so before the
constraint is created, all but the latest (highest id) pending row per
(target_dag_id, partition_key) is dropped. Each loser's PartitionedAssetKeyLog rows are
first re-pointed onto the surviving (highest id) row so the asset events they recorded
are not lost -- deleting them outright would leave the survivor's partition condition
permanently unsatisfied for events it never sees. This mirrors the model docstring's
"always work on the latest matching APDR record" fallback.

Revision ID: ee86eed19e24
Revises: 3c525f44bea8
Create Date: 2026-08-04 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from airflow.migrations.db_types import StringID
from airflow.migrations.utils import disable_sqlite_fkeys

revision = "ee86eed19e24"
down_revision = "3c525f44bea8"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_TABLE = "asset_partition_dag_run"
_LOG_TABLE = "partitioned_asset_key_log"
_UQ_NAME = "apdr_target_dag_id_pending_partition_key_uq"


def _heal_stale_duplicate_pending_apdrs(conn) -> None:
    """
    Collapse pre-existing duplicate pending APDR rows down to the latest one per key.

    Losers' PartitionedAssetKeyLog rows are re-pointed onto the winner (highest id) row
    rather than dropped, so the asset events they recorded still count toward the
    winner's partition condition after the duplicates are removed.
    """
    loser_to_winner = {
        row[0]: row[1]
        for row in conn.execute(
            sa.text(
                f"SELECT loser.id, winner.max_id FROM {_TABLE} loser JOIN ("
                f"    SELECT target_dag_id, partition_key, MAX(id) AS max_id FROM {_TABLE} "
                "     WHERE created_dag_run_id IS NULL "
                "     GROUP BY target_dag_id, partition_key"
                ") AS winner "
                "ON loser.target_dag_id = winner.target_dag_id "
                "AND loser.partition_key = winner.partition_key "
                "WHERE loser.created_dag_run_id IS NULL AND loser.id != winner.max_id"
            )
        ).fetchall()
    }
    if not loser_to_winner:
        return
    for loser_id, winner_id in loser_to_winner.items():
        conn.execute(
            sa.text(
                f"UPDATE {_LOG_TABLE} SET asset_partition_dag_run_id = :winner_id "
                "WHERE asset_partition_dag_run_id = :loser_id"
            ),
            {"winner_id": winner_id, "loser_id": loser_id},
        )
    id_list = ", ".join(str(i) for i in loser_to_winner)
    conn.execute(sa.text(f"DELETE FROM {_TABLE} WHERE id IN ({id_list})"))


def upgrade():
    """Add pending_partition_key to asset_partition_dag_run and enforce single pending row per key."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.add_column(sa.Column("pending_partition_key", StringID(), nullable=True))

        conn = op.get_bind()
        # Duplicate resolution requires reading actual data, which offline (SQL-script)
        # mode has no connection for; the constraint below still applies to whatever data
        # is present when the generated script is eventually run against a live database.
        if not context.is_offline_mode():
            _heal_stale_duplicate_pending_apdrs(conn)

        conn.execute(
            sa.text(
                f"UPDATE {_TABLE} SET pending_partition_key = partition_key WHERE created_dag_run_id IS NULL"
            )
        )

        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.create_unique_constraint(_UQ_NAME, ["target_dag_id", "pending_partition_key"])


def downgrade():
    """Drop the pending-partition uniqueness guard and pending_partition_key column."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table(_TABLE, schema=None) as batch_op:
            batch_op.drop_constraint(_UQ_NAME, type_="unique")
            batch_op.drop_column("pending_partition_key")
