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
Regression test for migration 0130 (ee86eed19e24).

Pre-existing duplicate pending AssetPartitionDagRun (APDR) rows for the same
(target_dag_id, partition_key) must have their PartitionedAssetKeyLog rows re-pointed
onto the surviving (highest id) row before the loser rows are dropped. Deleting the
loser's log rows outright (the pre-fix behavior) would leave the survivor's partition
condition permanently unsatisfied for the asset events it never sees (apache/airflow#71070).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0130_3_4_0_add_pending_partition_key_to_apdr.py"
)
_spec = importlib.util.spec_from_file_location("migration_0130", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_DDL = [
    """
    CREATE TABLE asset_partition_dag_run (
        id                  INTEGER PRIMARY KEY,
        target_dag_id       TEXT NOT NULL,
        partition_key       TEXT NOT NULL,
        created_dag_run_id  INTEGER
    )
    """,
    """
    CREATE TABLE partitioned_asset_key_log (
        id                          INTEGER PRIMARY KEY,
        asset_partition_dag_run_id INTEGER NOT NULL,
        asset_event_id              INTEGER NOT NULL
    )
    """,
]


def _make_engine():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        for ddl in _DDL:
            conn.execute(sa.text(ddl))
        conn.commit()
    return engine


def _insert_apdr(conn, apdr_id: int, target_dag_id: str, partition_key: str):
    conn.execute(
        sa.text(
            "INSERT INTO asset_partition_dag_run (id, target_dag_id, partition_key, created_dag_run_id)"
            " VALUES (:id, :target_dag_id, :partition_key, NULL)"
        ),
        {"id": apdr_id, "target_dag_id": target_dag_id, "partition_key": partition_key},
    )


def _insert_log(conn, log_id: int, apdr_id: int, asset_event_id: int):
    conn.execute(
        sa.text(
            "INSERT INTO partitioned_asset_key_log (id, asset_partition_dag_run_id, asset_event_id)"
            " VALUES (:id, :apdr_id, :asset_event_id)"
        ),
        {"id": log_id, "apdr_id": apdr_id, "asset_event_id": asset_event_id},
    )


class TestMigration0130HealStaleDuplicatePendingApdrs:
    def test_loser_log_rows_are_repointed_onto_winner_not_dropped(self):
        engine = _make_engine()
        with engine.begin() as conn:
            # Duplicate pending pair for ("consumer_dag", "k1"); id 2 is the winner.
            _insert_apdr(conn, 1, "consumer_dag", "k1")
            _insert_apdr(conn, 2, "consumer_dag", "k1")
            _insert_log(conn, 100, apdr_id=1, asset_event_id=1001)
            _insert_log(conn, 101, apdr_id=2, asset_event_id=1002)

            # An unrelated, non-duplicated group must be left untouched.
            _insert_apdr(conn, 3, "consumer_dag", "k2")
            _insert_log(conn, 102, apdr_id=3, asset_event_id=1003)

        with engine.begin() as conn:
            _migration._heal_stale_duplicate_pending_apdrs(conn)

        with engine.connect() as conn:
            apdr_ids = {row[0] for row in conn.execute(sa.text("SELECT id FROM asset_partition_dag_run"))}
            log_rows = {
                row[0]: (row[1], row[2])
                for row in conn.execute(
                    sa.text(
                        "SELECT id, asset_partition_dag_run_id, asset_event_id FROM partitioned_asset_key_log"
                    )
                )
            }

        # The loser APDR (id 1) is dropped; the winner (id 2) and the unrelated row (id 3) survive.
        assert apdr_ids == {2, 3}
        # Both log rows that belonged to the loser now point at the winner, and no log row
        # was dropped -- the asset event they recorded still counts toward the survivor.
        assert log_rows == {
            100: (2, 1001),
            101: (2, 1002),
            102: (3, 1003),
        }

    def test_no_duplicates_is_a_no_op(self):
        engine = _make_engine()
        with engine.begin() as conn:
            _insert_apdr(conn, 1, "consumer_dag", "k1")
            _insert_log(conn, 100, apdr_id=1, asset_event_id=1001)

        with engine.begin() as conn:
            _migration._heal_stale_duplicate_pending_apdrs(conn)

        with engine.connect() as conn:
            apdr_ids = {row[0] for row in conn.execute(sa.text("SELECT id FROM asset_partition_dag_run"))}
            log_apdr_ids = {
                row[0]
                for row in conn.execute(
                    sa.text("SELECT asset_partition_dag_run_id FROM partitioned_asset_key_log")
                )
            }

        assert apdr_ids == {1}
        assert log_apdr_ids == {1}
