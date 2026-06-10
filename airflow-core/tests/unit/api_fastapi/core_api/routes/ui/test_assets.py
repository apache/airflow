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
from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import pendulum
import pytest
from sqlalchemy import select

from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    PartitionedAssetKeyLog,
)
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.temporal import StartOfHourMapper
from airflow.partition_mappers.window import HourWindow
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_apdr, clear_db_dags, clear_db_pakl, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_apdr()
    clear_db_pakl()


class TestNextRunAssets:
    def test_should_response_200(self, test_client, dag_maker):
        with dag_maker(
            dag_id="upstream",
            schedule=[Asset(uri="s3://bucket/next-run-asset/1", name="asset1")],
            serialized=True,
        ):
            EmptyOperator(task_id="task1")

        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with assert_queries_count(4):
            response = test_client.get("/next_run_assets/upstream")

        assert response.status_code == 200
        assert response.json() == {
            "asset_expression": {
                "all": [
                    {
                        "asset": {
                            "uri": "s3://bucket/next-run-asset/1",
                            "name": "asset1",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    }
                ]
            },
            "events": [
                {
                    "id": mock.ANY,
                    "uri": "s3://bucket/next-run-asset/1",
                    "name": "asset1",
                    "last_update": None,
                    "received_count": 0,
                    "required_count": 1,
                    "received_keys": [],
                    "required_keys": [],
                    "is_rollup": False,
                    "mapper_error": False,
                    "asset_inactive": False,
                }
            ],
            "pending_partition_count": None,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/next_run_assets/upstream")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/next_run_assets/upstream")
        assert response.status_code == 403

    def test_should_set_last_update_only_for_queued_and_hide_flag(self, test_client, dag_maker, session):
        with dag_maker(
            dag_id="two_assets_equal",
            schedule=[
                Asset(uri="s3://bucket/A", name="A"),
                Asset(uri="s3://bucket/B", name="B"),
            ],
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        assets = {
            a.uri: a
            for a in session.scalars(
                select(AssetModel).where(AssetModel.uri.in_(["s3://bucket/A", "s3://bucket/B"]))
            )
        }
        # Queue and add an event only for A
        session.add(AssetDagRunQueue(asset_id=assets["s3://bucket/A"].id, target_dag_id="two_assets_equal"))
        session.add(
            AssetEvent(asset_id=assets["s3://bucket/A"].id, timestamp=dr.logical_date or pendulum.now())
        )
        session.commit()

        response = test_client.get("/next_run_assets/two_assets_equal")
        assert response.status_code == 200
        assert response.json() == {
            "asset_expression": {
                "all": [
                    {
                        "asset": {
                            "uri": "s3://bucket/A",
                            "name": "A",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    },
                    {
                        "asset": {
                            "uri": "s3://bucket/B",
                            "name": "B",
                            "group": "asset",
                            "id": mock.ANY,
                        }
                    },
                ]
            },
            # events are ordered by uri
            "events": [
                {
                    "id": mock.ANY,
                    "uri": "s3://bucket/A",
                    "name": "A",
                    "last_update": mock.ANY,
                    "received_count": 0,
                    "required_count": 1,
                    "received_keys": [],
                    "required_keys": [],
                    "is_rollup": False,
                    "mapper_error": False,
                    "asset_inactive": False,
                },
                {
                    "id": mock.ANY,
                    "uri": "s3://bucket/B",
                    "name": "B",
                    "last_update": None,
                    "received_count": 0,
                    "required_count": 1,
                    "received_keys": [],
                    "required_keys": [],
                    "is_rollup": False,
                    "mapper_error": False,
                    "asset_inactive": False,
                },
            ],
            "pending_partition_count": None,
        }

    def test_last_update_respects_latest_run_filter(self, test_client, dag_maker, session):
        with dag_maker(
            dag_id="filter_run",
            schedule=[Asset(uri="s3://bucket/F", name="F")],
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset = session.scalars(select(AssetModel).where(AssetModel.uri == "s3://bucket/F")).one()
        session.add(AssetDagRunQueue(asset_id=asset.id, target_dag_id="filter_run"))
        # event before latest_run should be ignored
        ts_base = dr.logical_date or pendulum.now()
        session.add(AssetEvent(asset_id=asset.id, timestamp=ts_base.subtract(minutes=10)))
        # event after latest_run counts
        session.add(AssetEvent(asset_id=asset.id, timestamp=ts_base.add(minutes=10)))
        session.commit()

        resp = test_client.get("/next_run_assets/filter_run")
        assert resp.status_code == 200
        ev = resp.json()["events"][0]
        assert ev["last_update"] is not None
        assert "queued" not in ev

    @pytest.mark.parametrize(
        ("fulfilled", "expect_last_update"),
        [(False, True), (True, False)],
        ids=["pending", "fulfilled"],
    )
    def test_partitioned_dag_last_update(
        self, test_client, dag_maker, session, fulfilled, expect_last_update
    ):
        asset = Asset(uri="s3://bucket/part", name="part")
        with dag_maker(
            dag_id="part_dag",
            schedule=PartitionedAssetTimetable(assets=asset),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset_model = session.scalars(select(AssetModel).where(AssetModel.uri == "s3://bucket/part")).one()
        event = AssetEvent(
            asset_id=asset_model.id, timestamp=(dr.logical_date or pendulum.now()).add(minutes=5)
        )
        session.add(event)
        session.flush()

        pdr = AssetPartitionDagRun(
            target_dag_id="part_dag",
            partition_key="2024-01-01",
            created_dag_run_id=dr.id if fulfilled else None,
        )
        session.add(pdr)
        session.flush()

        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset_model.id,
                asset_event_id=event.id,
                asset_partition_dag_run_id=pdr.id,
                source_partition_key="2024-01-01",
                target_dag_id="part_dag",
                target_partition_key="2024-01-01",
            )
        )
        session.commit()

        resp = test_client.get("/next_run_assets/part_dag")
        assert resp.status_code == 200
        ev = resp.json()["events"][0]
        assert (ev["last_update"] is not None) == expect_last_update

    def test_pending_apdr_uses_fifo_order(self, test_client, dag_maker, session):
        """
        The next-run view must surface the oldest pending APDR so it matches the
        one the scheduler will fire next (``_create_dagruns_for_partitioned_asset_dags``
        is strict FIFO over ``created_at``). When multiple partitions are
        backlogged, showing the newest would confuse the operator about which
        run is actually queued.
        """
        asset = Asset(uri="s3://bucket/fifo", name="fifo")
        with dag_maker(
            dag_id="fifo_dag",
            schedule=PartitionedAssetTimetable(assets=asset),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        base = pendulum.datetime(2024, 1, 1)
        # Insert the newer APDR first so the natural row order disagrees with
        # the intended ``created_at`` order — this catches a regression that
        # would otherwise silently pass because rows happen to come back in
        # insertion order on SQLite.
        for offset_days, key in [(1, "2024-01-02"), (0, "2024-01-01")]:
            apdr = AssetPartitionDagRun(
                target_dag_id="fifo_dag",
                partition_key=key,
            )
            apdr.created_at = base.add(days=offset_days)
            session.add(apdr)
        session.commit()

        resp = test_client.get("/next_run_assets/fifo_dag")
        assert resp.status_code == 200
        body = resp.json()
        ev = body["events"][0]
        assert ev["required_keys"] == ["2024-01-01"]
        assert body["pending_partition_count"] == 2

    def test_rollup_mapper_failure_treats_asset_as_not_satisfied(self, test_client, dag_maker, session):
        """
        When the rollup mapper raises during ``to_upstream`` evaluation the
        route must mirror the scheduler's not-yet-satisfied verdict: zero
        ``received_count``, ``last_update`` cleared, no upstream keys surfaced.
        Without this, an event already logged would make the UI display
        ``1/1 received`` and ``last_update`` set, while the scheduler holds
        the run because its own ``_resolve_asset_partition_status`` raises.
        """
        asset = Asset(uri="s3://bucket/rollup_warn", name="rollup_warn")
        with dag_maker(
            dag_id="rollup_warn_dag",
            schedule=PartitionedAssetTimetable(
                assets=asset,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfHourMapper(),
                    window=HourWindow(),
                ),
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset_model = session.scalar(select(AssetModel).where(AssetModel.uri == "s3://bucket/rollup_warn"))
        apdr = AssetPartitionDagRun(target_dag_id="rollup_warn_dag", partition_key="2024-01-01T00")
        session.add(apdr)
        session.flush()
        # Log a received event so the broken-mapper path would otherwise credit
        # it as 1/1 ready — the test asserts the new behaviour leaves received
        # at 0 regardless of logged events.
        event = AssetEvent(asset_id=asset_model.id, timestamp=pendulum.now())
        session.add(event)
        session.flush()
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset_model.id,
                asset_event_id=event.id,
                asset_partition_dag_run_id=apdr.id,
                source_partition_key="2024-01-01T00",
                target_dag_id="rollup_warn_dag",
                target_partition_key="2024-01-01T00",
            )
        )
        session.commit()

        broken_timetable = MagicMock()
        broken_timetable.get_partition_mapper.side_effect = RuntimeError("mapper exploded")

        with (
            mock.patch(
                "airflow.api_fastapi.core_api.routes.ui.assets.load_partitioned_timetable",
                return_value=broken_timetable,
            ),
            mock.patch("airflow.api_fastapi.core_api.routes.ui.assets.log") as mock_log,
        ):
            resp = test_client.get("/next_run_assets/rollup_warn_dag")

        assert resp.status_code == 200
        ev = resp.json()["events"][0]
        # Scheduler-aligned fallback: not-yet-satisfied, no keys to claim.
        assert ev["received_count"] == 0
        assert ev["required_count"] == 1
        assert ev["received_keys"] == []
        assert ev["required_keys"] == []
        assert ev["last_update"] is None
        assert ev["mapper_error"] is True
        assert mock_log.warning.mock_calls == [
            mock.call(
                "Failed to evaluate rollup mapper; treating asset as not-yet-satisfied",
                dag_id="rollup_warn_dag",
                asset_name="rollup_warn",
                asset_uri="s3://bucket/rollup_warn",
                partition_key="2024-01-01T00",
                exc_info=True,
            )
        ]

    def test_pending_partition_count_includes_inactive_asset_apdrs(self, test_client, dag_maker, session):
        """
        ``pending_partition_count`` must include APDRs even when the upstream
        asset is inactive (orphaned / no declaring Dag). The freeze is surfaced
        via ``asset_inactive`` on each event instead of hiding the count.
        """
        asset = Asset(uri="s3://bucket/count_active", name="count_active")
        with dag_maker(
            dag_id="count_active_dag",
            schedule=PartitionedAssetTimetable(assets=asset),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        # One pending APDR while asset is active.
        session.add(AssetPartitionDagRun(target_dag_id="count_active_dag", partition_key="2024-01-01"))
        session.commit()

        resp_active = test_client.get("/next_run_assets/count_active_dag")
        assert resp_active.status_code == 200
        assert resp_active.json()["pending_partition_count"] == 1

        # Deactivate the asset (simulate orphan / no declaring Dag).
        asset_active_row = session.scalar(
            select(AssetActive).where(
                AssetActive.name == "count_active",
                AssetActive.uri == "s3://bucket/count_active",
            )
        )
        assert asset_active_row is not None
        session.delete(asset_active_row)
        session.commit()

        resp_inactive = test_client.get("/next_run_assets/count_active_dag")
        assert resp_inactive.status_code == 200
        # pending_partition_count still 1 — the APDR is pending regardless of asset active state
        assert resp_inactive.json()["pending_partition_count"] == 1
        # asset_inactive flag signals the frozen state to the UI
        events = resp_inactive.json()["events"]
        assert len(events) == 1
        assert events[0]["asset_inactive"] is True

    def test_asset_inactive_false_for_active_asset(self, test_client, dag_maker, session):
        """``asset_inactive`` must be False when the upstream asset is still active."""
        asset = Asset(uri="s3://bucket/count_active2", name="count_active2")
        with dag_maker(
            dag_id="count_active_dag2",
            schedule=PartitionedAssetTimetable(assets=asset),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        session.add(AssetPartitionDagRun(target_dag_id="count_active_dag2", partition_key="2024-01-01"))
        session.commit()

        resp = test_client.get("/next_run_assets/count_active_dag2")
        assert resp.status_code == 200
        events = resp.json()["events"]
        assert len(events) == 1
        assert events[0]["asset_inactive"] is False

    def test_non_partitioned_asset_inactive_true_when_deactivated(self, test_client, dag_maker, session):
        """Non-partitioned Dag also surfaces asset_inactive when upstream is deactivated."""
        asset = Asset(name="np_inactive", uri="s3://bucket/np_inactive")
        with dag_maker(dag_id="np_inactive_dag", schedule=[asset], serialized=True):
            EmptyOperator(task_id="op")
        dag_maker.sync_dagbag_to_db()

        asset_active_row = session.scalar(
            select(AssetActive).where(
                AssetActive.name == "np_inactive",
                AssetActive.uri == "s3://bucket/np_inactive",
            )
        )
        assert asset_active_row is not None
        session.delete(asset_active_row)
        session.commit()

        response = test_client.get("/next_run_assets/np_inactive_dag")
        assert response.status_code == 200
        body = response.json()
        assert len(body["events"]) == 1
        assert body["events"][0]["asset_inactive"] is True
