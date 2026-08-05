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
Reference the asset event from asset_dag_run_queue (consume-by-reference).

Table ``asset_dag_run_queue`` gets a new column ``asset_event_id``, and the
primary key is made ``(target_dag_id, asset_event_id)``, so the scheduler
consumes queued asset events by reference instead of by a ``created_at`` time
window. ``asset_id`` is kept as a denormalized column.

Existing rows are coalesced (one per ``(asset_id, target_dag_id)``) and carry no
event reference. Rather than dropping them (which would silently skip pending
asset-triggered Dag runs), the pre-migration scheduler's own consumption window is
replayed per dag to rebuild per-event rows:

    triggered_date =
        MAX(asset_dag_run_queue.created_at) (per dag)
    floor = COALESCE(
        MAX(dag_run.run_after) for asset-triggered runs of the dag with
            run_after < triggered_date,                  (per dag)
        dag_schedule_asset_reference.created_at,     (per dag + asset)
    )
    contributing events =
        the queued asset's events with
        floor < asset_event.timestamp <= triggered_date

The expansion is staged in a side table, the queue is cleared, the (now empty)
table is reshaped, and the staged rows are inserted back.

Revision ID: b2f1a9c7d4e0
Revises: 7a98f1b7dbd3
Create Date: 2026-08-03 12:00:00.000000
"""

from __future__ import annotations

from datetime import datetime, timezone
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op

# revision identifiers, used by Alembic.
revision = "b2f1a9c7d4e0"
down_revision = "7a98f1b7dbd3"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"

_STAGING = "_adrq_migration_staging"

_STAGE_SQL = f"""
CREATE TABLE {_STAGING} AS
SELECT DISTINCT
    adrq.target_dag_id AS target_dag_id,
    adrq.asset_id      AS asset_id,
    ae.id              AS asset_event_id
FROM asset_dag_run_queue adrq
JOIN (
    SELECT t.target_dag_id,
           t.triggered_date,
           (
               SELECT MAX(dr.run_after)
               FROM dag_run dr
               WHERE dr.dag_id = t.target_dag_id
                 AND dr.run_type = 'asset_triggered'
                 AND dr.run_after < t.triggered_date
           ) AS floor_date
    FROM (
        SELECT target_dag_id, MAX(created_at) AS triggered_date
        FROM asset_dag_run_queue
        GROUP BY target_dag_id
    ) t
) td ON td.target_dag_id = adrq.target_dag_id
LEFT JOIN dag_schedule_asset_reference dsar
    ON dsar.dag_id = adrq.target_dag_id
   AND dsar.asset_id = adrq.asset_id
JOIN asset_event ae
    ON ae.asset_id = adrq.asset_id
   AND ae.timestamp <= td.triggered_date
   AND ae.timestamp > COALESCE(td.floor_date, dsar.created_at, :floor_min)
"""


def upgrade():
    """Directly reference the asset event from asset_dag_run_queue."""
    # 1. Add the reference column, nullable for now; it is made NOT NULL after the rebuild.
    op.add_column("asset_dag_run_queue", sa.Column("asset_event_id", sa.Integer(), nullable=True))

    # 2. Stage the faithful per-event expansion, then clear the coalesced rows.
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: asset_dag_run_queue cannot be rebuilt in offline mode;
            --  any pending (unprocessed) queued asset events will be dropped.
            ------------
            """)
        )
        op.execute("DELETE FROM asset_dag_run_queue")
    else:
        conn = op.get_bind()
        floor_min = datetime(1970, 1, 1, tzinfo=timezone.utc)
        conn.execute(sa.text(_STAGE_SQL), {"floor_min": floor_min})
        conn.execute(sa.text("DELETE FROM asset_dag_run_queue"))

    # 3. Make the reference NOT NULL, move the PK, adjust FKs and indexes.
    with op.batch_alter_table("asset_dag_run_queue") as batch_op:
        batch_op.alter_column("asset_event_id", existing_type=sa.Integer(), nullable=False)
        batch_op.drop_constraint("adrq_asset_fkey", type_="foreignkey")
        batch_op.drop_constraint("assetdagrunqueue_pkey", type_="primary")
        batch_op.create_primary_key("assetdagrunqueue_pkey", ["target_dag_id", "asset_event_id"])
        batch_op.create_index("idx_adrq_asset_id", ["asset_id"])
        batch_op.create_foreign_key("adrq_asset_fkey", "asset", ["asset_id"], ["id"], ondelete="CASCADE")
        batch_op.create_foreign_key(
            "adrq_asset_event_fkey", "asset_event", ["asset_event_id"], ["id"], ondelete="CASCADE"
        )
        batch_op.drop_index("idx_asset_dag_run_queue_target_dag_id")

    # 4. Repopulate the per-event rows from staging.
    if not context.is_offline_mode():
        conn = op.get_bind()
        now = datetime.now(timezone.utc)
        conn.execute(
            sa.text(
                f"""
                INSERT INTO asset_dag_run_queue (asset_id, target_dag_id, asset_event_id, created_at)
                SELECT asset_id, target_dag_id, asset_event_id, :now
                FROM {_STAGING}
                """
            ),
            {"now": now},
        )
        op.drop_table(_STAGING)


def downgrade():
    """Revert reference to asset_id from asset_dag_run_queue."""
    # 1. Rebuild the coalesced rows: collapse per-event rows to one row per
    #    (asset_id, target_dag_id) (created_at = max referenced event timestamp), then clear.
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: asset_dag_run_queue cannot be rebuilt in offline mode;
            --  any pending (unprocessed) queued asset events will be dropped.
            ------------
            """)
        )
        op.execute("DELETE FROM asset_dag_run_queue")
    else:
        conn = op.get_bind()
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE {_STAGING} AS
                SELECT adrq.asset_id      AS asset_id,
                    adrq.target_dag_id AS target_dag_id,
                    MAX(ae.timestamp)  AS created_at
                FROM asset_dag_run_queue adrq
                JOIN asset_event ae ON ae.id = adrq.asset_event_id
                GROUP BY adrq.asset_id, adrq.target_dag_id
                """
            )
        )
        conn.execute(sa.text("DELETE FROM asset_dag_run_queue"))

    # 2. Drop the reference + FK, restore the old primary key and indexes.
    with op.batch_alter_table("asset_dag_run_queue") as batch_op:
        batch_op.drop_constraint("adrq_asset_event_fkey", type_="foreignkey")
        batch_op.create_index("idx_asset_dag_run_queue_target_dag_id", ["target_dag_id"])
        batch_op.drop_constraint("assetdagrunqueue_pkey", type_="primary")
        batch_op.create_primary_key("assetdagrunqueue_pkey", ["asset_id", "target_dag_id"])
        batch_op.drop_index("idx_adrq_asset_id")
        batch_op.drop_column("asset_event_id")

    # 3. Repopulate the coalesced rows.
    if not context.is_offline_mode():
        conn = op.get_bind()
        conn.execute(
            sa.text(
                f"""
                INSERT INTO asset_dag_run_queue (asset_id, target_dag_id, created_at)
                SELECT asset_id, target_dag_id, created_at
                FROM {_STAGING}
                """
            )
        )
        op.drop_table(_STAGING)
