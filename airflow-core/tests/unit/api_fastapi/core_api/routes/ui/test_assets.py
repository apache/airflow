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

import json
from unittest import mock
from unittest.mock import MagicMock

import pendulum
import pytest
from sqlalchemy import select

from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    AssetWatcherModel,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
    TaskInletAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.temporal import StartOfHourMapper
from airflow.partition_mappers.window import HourWindow
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_assets,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_pakl,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_teams,
)

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def cleanup():
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_apdr()
    clear_db_pakl()
    clear_db_teams()
    clear_db_dag_bundles()


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

        # 4 queries for the endpoint, 1 to resolve the assets the caller may read and
        # 1 for the caller-independent scheduling asset count.
        with assert_queries_count(6):
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
                            "hidden": False,
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
            "scheduling_asset_count": 1,
            "pending_partition_count": None,
        }

    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_assets",
        autospec=True,
    )
    def test_asset_expression_hides_assets_the_caller_may_not_read(
        self, mock_get_authorized_assets, test_client, dag_maker, session
    ):
        with dag_maker(
            dag_id="hidden_upstream",
            schedule=[
                Asset(uri="s3://bucket/visible", name="visible_asset"),
                Asset(uri="s3://bucket/hidden", name="hidden_asset"),
            ],
            serialized=True,
        ):
            EmptyOperator(task_id="task1")
        dag_maker.sync_dagbag_to_db()
        visible_id = session.scalar(select(AssetModel.id).where(AssetModel.name == "visible_asset"))
        mock_get_authorized_assets.return_value = {visible_id}

        response = test_client.get("/next_run_assets/hidden_upstream")

        assert response.status_code == 200
        assert response.json()["asset_expression"] == {
            "all": [
                {
                    "asset": {
                        "uri": "s3://bucket/visible",
                        "name": "visible_asset",
                        "group": "asset",
                        "id": visible_id,
                        "hidden": False,
                    }
                },
                {"asset": {"uri": None, "name": None, "group": "asset", "id": None, "hidden": True}},
            ]
        }
        redacted = json.dumps(response.json()["asset_expression"])
        assert "hidden_asset" not in redacted
        assert "s3://bucket/hidden" not in redacted

    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_assets",
        autospec=True,
    )
    def test_should_return_only_assets_the_caller_may_read(
        self, mock_get_authorized_assets, test_client, dag_maker, session
    ):
        with dag_maker(
            dag_id="hidden_upstream",
            schedule=[
                Asset(uri="s3://bucket/visible", name="visible_asset"),
                Asset(uri="s3://bucket/hidden", name="hidden_asset"),
            ],
            serialized=True,
        ):
            EmptyOperator(task_id="task1")
        dag_maker.sync_dagbag_to_db()
        visible_id = session.scalar(select(AssetModel.id).where(AssetModel.name == "visible_asset"))
        mock_get_authorized_assets.return_value = {visible_id}

        response = test_client.get("/next_run_assets/hidden_upstream")

        assert response.status_code == 200
        assert [event["name"] for event in response.json()["events"]] == ["visible_asset"]
        assert response.json()["scheduling_asset_count"] == 2

    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_assets",
        autospec=True,
    )
    def test_scheduling_asset_count_is_unaffected_when_no_asset_is_readable(
        self, mock_get_authorized_assets, test_client, dag_maker
    ):
        with dag_maker(
            dag_id="all_hidden_upstream",
            schedule=[
                Asset(uri="s3://bucket/hidden1", name="hidden_asset1"),
                Asset(uri="s3://bucket/hidden2", name="hidden_asset2"),
            ],
            serialized=True,
        ):
            EmptyOperator(task_id="task1")
        dag_maker.sync_dagbag_to_db()
        mock_get_authorized_assets.return_value = set()

        response = test_client.get("/next_run_assets/all_hidden_upstream")

        assert response.status_code == 200
        assert response.json()["events"] == []
        assert response.json()["scheduling_asset_count"] == 2

    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_assets",
        autospec=True,
    )
    def test_partitioned_dag_should_hide_keys_of_assets_the_caller_may_not_read(
        self, mock_get_authorized_assets, test_client, dag_maker, session
    ):
        visible = Asset(uri="s3://bucket/part_visible", name="part_visible")
        hidden = Asset(uri="s3://bucket/part_hidden", name="part_hidden")
        with dag_maker(
            dag_id="part_hidden_dag",
            schedule=PartitionedAssetTimetable(assets=visible & hidden),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset_models = {
            am.name: am
            for am in session.scalars(
                select(AssetModel).where(AssetModel.name.in_(["part_visible", "part_hidden"]))
            )
        }
        pdr = AssetPartitionDagRun(
            target_dag_id="part_hidden_dag", partition_key="2024-01-01", created_dag_run_id=None
        )
        session.add(pdr)
        session.flush()
        for am in asset_models.values():
            event = AssetEvent(asset_id=am.id, timestamp=(dr.logical_date or pendulum.now()).add(minutes=5))
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=am.id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-01-01",
                    target_dag_id="part_hidden_dag",
                    target_partition_key="2024-01-01",
                )
            )
        session.commit()
        mock_get_authorized_assets.return_value = {asset_models["part_visible"].id}

        response = test_client.get("/next_run_assets/part_hidden_dag")

        assert response.status_code == 200
        events = response.json()["events"]
        assert [event["name"] for event in events] == ["part_visible"]
        assert events[0]["received_keys"] == ["2024-01-01"]
        assert events[0]["required_keys"] == ["2024-01-01"]
        assert response.json()["scheduling_asset_count"] == 2

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
        event = AssetEvent(asset_id=assets["s3://bucket/A"].id, timestamp=dr.logical_date or pendulum.now())
        session.add(event)
        session.flush()
        session.add(
            AssetDagRunQueue(
                asset_id=assets["s3://bucket/A"].id,
                target_dag_id="two_assets_equal",
                asset_event_id=event.id,
            )
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
                            "hidden": False,
                        }
                    },
                    {
                        "asset": {
                            "uri": "s3://bucket/B",
                            "name": "B",
                            "group": "asset",
                            "id": mock.ANY,
                            "hidden": False,
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
            "scheduling_asset_count": 2,
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
        ts_base = dr.logical_date or pendulum.now()
        # event before latest_run should be ignored
        event_before = AssetEvent(asset_id=asset.id, timestamp=ts_base.subtract(minutes=10))
        # event after latest_run counts
        event_after = AssetEvent(asset_id=asset.id, timestamp=ts_base.add(minutes=10))
        session.add_all([event_before, event_after])
        session.flush()
        session.add(
            AssetDagRunQueue(asset_id=asset.id, target_dag_id="filter_run", asset_event_id=event_after.id)
        )
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
        The next-run view must surface the pending APDR that has waited longest.

        When multiple partitions are backlogged, showing the newest would hide the one
        that has been queued longest. It is not necessarily the next to fire: the
        scheduler fires whichever pending APDR is satisfied, whatever its age.
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


class TestGetAssetsUi:
    @pytest.fixture(autouse=True)
    def cleanup_assets(self):
        clear_db_assets()

        yield

        clear_db_assets()

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets")
        assert response.status_code == 403

    def test_should_respond_200(self, test_client, session):
        asset = AssetModel(name="ui_asset", uri="s3://bucket/ui_asset", group="asset")
        session.add(asset)
        session.add(AssetActive.for_asset(asset))
        session.commit()

        response = test_client.get("/assets")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        assert body["assets"][0]["name"] == "ui_asset"

    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.get_authorized_assets"
    )
    def test_should_return_only_assets_the_caller_may_read(
        self, mock_get_authorized_assets, test_client, session
    ):
        assets = [AssetModel(name=f"asset{i}", uri=f"s3://bucket/asset{i}", group="asset") for i in range(3)]
        session.add_all(assets)
        session.add_all(AssetActive.for_asset(asset) for asset in assets)
        session.commit()
        mock_get_authorized_assets.return_value = {assets[1].id}

        response = test_client.get("/assets")

        mock_get_authorized_assets.assert_called_once_with(user=mock.ANY, method="GET")
        assert response.status_code == 200
        body = response.json()
        assert [asset["name"] for asset in body["assets"]] == ["asset1"]
        # The count must be scoped too, so the existence of hidden assets does not leak.
        assert body["total_entries"] == 1

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_should_return_only_references_of_readable_dags(
        self, mock_get_authorized_dag_ids, test_client, session
    ):
        bundle = DagBundleModel(name="testing")
        session.add(bundle)
        asset = AssetModel(name="shared", uri="s3://bucket/shared", group="asset")
        session.add(asset)
        session.add(AssetActive.for_asset(asset))
        session.flush()
        for dag_id in ("dag_a", "dag_b"):
            session.add_all(
                [
                    DagModel(dag_id=dag_id, bundle_name="testing"),
                    DagScheduleAssetReference(dag_id=dag_id, asset=asset),
                    TaskOutletAssetReference(dag_id=dag_id, task_id=f"{dag_id}_producer", asset=asset),
                    TaskInletAssetReference(dag_id=dag_id, task_id=f"{dag_id}_consumer", asset=asset),
                ]
            )
        session.commit()
        mock_get_authorized_dag_ids.return_value = {"dag_a"}

        response = test_client.get("/assets")

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        result = body["assets"][0]
        assert [ref["dag_id"] for ref in result["scheduled_dags"]] == ["dag_a"]
        assert [(ref["dag_id"], ref["task_id"]) for ref in result["producing_tasks"]] == [
            ("dag_a", "dag_a_producer")
        ]
        assert [(ref["dag_id"], ref["task_id"]) for ref in result["consuming_tasks"]] == [
            ("dag_a", "dag_a_consumer")
        ]

    def test_sort_by_last_asset_event_timestamp(self, test_client, session):
        older = AssetModel(name="older", uri="s3://bucket/older", group="asset")
        newer = AssetModel(name="newer", uri="s3://bucket/newer", group="asset")
        session.add_all([older, newer])
        session.add(AssetActive.for_asset(older))
        session.add(AssetActive.for_asset(newer))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        session.add(AssetEvent(asset_id=older.id, timestamp=base))
        session.add(AssetEvent(asset_id=newer.id, timestamp=base.add(days=1)))
        session.commit()

        response = test_client.get("/assets?order_by=last_asset_event_timestamp")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["older", "newer"]

        response = test_client.get("/assets?order_by=-last_asset_event_timestamp")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["newer", "older"]

    def test_default_sort_is_last_asset_event_timestamp_desc(self, test_client, session):
        older = AssetModel(name="older", uri="s3://bucket/older_default", group="asset")
        newer = AssetModel(name="newer", uri="s3://bucket/newer_default", group="asset")
        session.add_all([older, newer])
        session.add(AssetActive.for_asset(older))
        session.add(AssetActive.for_asset(newer))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        session.add(AssetEvent(asset_id=older.id, timestamp=base))
        session.add(AssetEvent(asset_id=newer.id, timestamp=base.add(days=1)))
        session.commit()

        response = test_client.get("/assets")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["newer", "older"]

    def test_last_asset_event_uses_latest_timestamp_not_highest_id(self, test_client, session):
        asset = AssetModel(name="out_of_order", uri="s3://bucket/out_of_order", group="asset")
        session.add(asset)
        session.add(AssetActive.for_asset(asset))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        latest_event = AssetEvent(asset_id=asset.id, timestamp=base.add(days=1))
        highest_id_event = AssetEvent(asset_id=asset.id, timestamp=base)
        session.add_all([latest_event, highest_id_event])
        session.flush()
        latest_event_id = latest_event.id
        highest_id_event_id = highest_id_event.id
        session.commit()

        assert highest_id_event_id > latest_event_id

        response = test_client.get("/assets")
        assert response.status_code == 200
        last_asset_event = response.json()["assets"][0]["last_asset_event"]
        assert last_asset_event["id"] == latest_event_id
        assert last_asset_event["timestamp"] == "2024-01-02T00:00:00Z"

    def test_last_asset_event_breaks_timestamp_tie_by_highest_id(self, test_client, session):
        asset = AssetModel(name="tied_timestamp", uri="s3://bucket/tied_timestamp", group="asset")
        session.add(asset)
        session.add(AssetActive.for_asset(asset))
        session.flush()

        tied_timestamp = pendulum.datetime(2024, 1, 1)
        lower_id_event = AssetEvent(asset_id=asset.id, timestamp=tied_timestamp)
        higher_id_event = AssetEvent(asset_id=asset.id, timestamp=tied_timestamp)
        session.add_all([lower_id_event, higher_id_event])
        session.flush()
        higher_id_event_id = higher_id_event.id
        session.commit()

        assert higher_id_event_id > lower_id_event.id

        response = test_client.get("/assets")
        assert response.status_code == 200
        last_asset_event = response.json()["assets"][0]["last_asset_event"]
        assert last_asset_event["id"] == higher_id_event_id

    def test_sort_by_group(self, test_client, session):
        billing = AssetModel(name="billing_asset", uri="s3://bucket/billing_sort", group="billing")
        marketing = AssetModel(name="marketing_asset", uri="s3://bucket/marketing_sort", group="marketing")
        session.add_all([billing, marketing])
        session.add(AssetActive.for_asset(billing))
        session.add(AssetActive.for_asset(marketing))
        session.commit()

        response = test_client.get("/assets?order_by=group")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["billing_asset", "marketing_asset"]

        response = test_client.get("/assets?order_by=-group")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["marketing_asset", "billing_asset"]

    def test_filter_by_group_pattern(self, test_client, session):
        billing = AssetModel(name="billing_asset", uri="s3://bucket/billing", group="billing")
        marketing = AssetModel(name="marketing_asset", uri="s3://bucket/marketing", group="marketing")
        session.add_all([billing, marketing])
        session.add(AssetActive.for_asset(billing))
        session.add(AssetActive.for_asset(marketing))
        session.commit()

        response = test_client.get("/assets?group_pattern=bill")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["billing_asset"]

    def test_filter_by_group_prefix_pattern(self, test_client, session):
        billing = AssetModel(name="billing_asset", uri="s3://bucket/billing_prefix", group="billing")
        rebilling = AssetModel(name="rebilling_asset", uri="s3://bucket/rebilling", group="rebilling")
        session.add_all([billing, rebilling])
        session.add(AssetActive.for_asset(billing))
        session.add(AssetActive.for_asset(rebilling))
        session.commit()

        # Prefix match anchors at the start, so "bill" excludes "rebilling" (substring would not).
        response = test_client.get("/assets?group_prefix_pattern=bill")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["billing_asset"]

    def test_filter_by_last_asset_event_timestamp_range(self, test_client, session):
        older = AssetModel(name="older", uri="s3://bucket/older_range", group="asset")
        newer = AssetModel(name="newer", uri="s3://bucket/newer_range", group="asset")
        session.add_all([older, newer])
        session.add(AssetActive.for_asset(older))
        session.add(AssetActive.for_asset(newer))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        session.add(AssetEvent(asset_id=older.id, timestamp=base))
        session.add(AssetEvent(asset_id=newer.id, timestamp=base.add(days=10)))
        session.commit()

        response = test_client.get(
            "/assets", params={"last_asset_event_timestamp_gte": base.add(days=5).isoformat()}
        )
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["newer"]

    @pytest.mark.parametrize(
        ("has_events", "expected_names"),
        [
            pytest.param(None, ["evented", "never"], id="unset"),
            pytest.param(True, ["evented"], id="has-events"),
            pytest.param(False, ["never"], id="no-events"),
        ],
    )
    def test_filter_by_has_events(self, test_client, session, has_events, expected_names):
        evented = AssetModel(name="evented", uri="s3://bucket/evented", group="asset")
        never = AssetModel(name="never", uri="s3://bucket/never", group="asset")
        session.add_all([evented, never])
        session.add_all([AssetActive.for_asset(evented), AssetActive.for_asset(never)])
        session.flush()
        session.add(AssetEvent(asset_id=evented.id, timestamp=pendulum.datetime(2024, 1, 1)))
        session.commit()

        params = {} if has_events is None else {"has_events": has_events}
        response = test_client.get("/assets", params=params)

        assert response.status_code == 200
        assert sorted(asset["name"] for asset in response.json()["assets"]) == expected_names

    def test_aliases_present_for_asset_via_alias(self, test_client, session):
        """
        Regression test for https://github.com/apache/airflow/issues/58058:
        aliases must be visible via the Assets endpoints, not just fetchable
        through the CLI/DB.
        """
        asset = AssetModel(name="alias_target", uri="s3://bucket/alias_target", group="asset")
        alias = AssetAliasModel(name="my-alias", group="")
        session.add_all([asset, alias])
        session.flush()
        session.add(AssetActive.for_asset(asset))
        asset.aliases.append(alias)
        session.commit()

        response = test_client.get("/assets")
        assert response.status_code == 200
        body = response.json()
        assert len(body["assets"]) == 1
        assert body["assets"][0]["aliases"] == [{"id": alias.id, "name": "my-alias", "group": ""}]

    def test_total_entries_counts_assets_not_events(self, test_client, session):
        """The outer join to AssetEvent must not inflate total_entries for assets with many events."""
        multi = AssetModel(name="multi", uri="s3://bucket/multi", group="reporting")
        solo = AssetModel(name="solo", uri="s3://bucket/solo", group="reporting")
        other = AssetModel(name="other", uri="s3://bucket/other", group="ops")
        session.add_all([multi, solo, other])
        for asset in (multi, solo, other):
            session.add(AssetActive.for_asset(asset))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        for offset in range(3):
            session.add(AssetEvent(asset_id=multi.id, timestamp=base.add(hours=offset)))
        session.add(AssetEvent(asset_id=solo.id, timestamp=base))
        session.commit()

        response = test_client.get("/assets")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        assert sorted(a["name"] for a in body["assets"]) == ["multi", "other", "solo"]

        response = test_client.get("/assets?group_pattern=report")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert sorted(a["name"] for a in body["assets"]) == ["multi", "solo"]

    def test_pagination_covers_all_assets(self, test_client, session):
        """Paging returns every asset exactly once with a stable total, mixing evented and never-evented."""
        evented_new = AssetModel(name="evented_new", uri="s3://bucket/en", group="asset")
        evented_old = AssetModel(name="evented_old", uri="s3://bucket/eo", group="asset")
        never_a = AssetModel(name="never_a", uri="s3://bucket/na", group="asset")
        never_b = AssetModel(name="never_b", uri="s3://bucket/nb", group="asset")
        session.add_all([evented_new, evented_old, never_a, never_b])
        for asset in (evented_new, evented_old, never_a, never_b):
            session.add(AssetActive.for_asset(asset))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        session.add(AssetEvent(asset_id=evented_old.id, timestamp=base))
        session.add(AssetEvent(asset_id=evented_new.id, timestamp=base.add(days=1)))
        session.commit()

        page_one = test_client.get("/assets?limit=2&offset=0")
        page_two = test_client.get("/assets?limit=2&offset=2")
        assert page_one.status_code == 200
        assert page_two.status_code == 200
        assert page_one.json()["total_entries"] == 4
        assert page_two.json()["total_entries"] == 4

        names = [a["name"] for a in page_one.json()["assets"] + page_two.json()["assets"]]
        assert sorted(names) == ["evented_new", "evented_old", "never_a", "never_b"]

    def test_timestamp_range_filter_excludes_never_evented_assets(self, test_client, session):
        """A timestamp range filter excludes assets with no event (NULL fails the range predicate)."""
        evented = AssetModel(name="evented", uri="s3://bucket/re", group="asset")
        never = AssetModel(name="never", uri="s3://bucket/rn", group="asset")
        session.add_all([evented, never])
        session.add(AssetActive.for_asset(evented))
        session.add(AssetActive.for_asset(never))
        session.flush()

        base = pendulum.datetime(2024, 1, 1)
        session.add(AssetEvent(asset_id=evented.id, timestamp=base))
        session.commit()

        response = test_client.get(
            "/assets", params={"last_asset_event_timestamp_gte": base.subtract(days=1).isoformat()}
        )
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["evented"]

    def test_filter_by_only_active(self, test_client, session):
        active = AssetModel(name="active", uri="s3://bucket/active", group="asset")
        inactive = AssetModel(name="inactive", uri="s3://bucket/inactive", group="asset")
        session.add_all([active, inactive])
        session.add(AssetActive.for_asset(active))
        session.commit()

        response = test_client.get("/assets")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["active"]

        response = test_client.get("/assets?only_active=false")
        assert response.status_code == 200
        assert sorted(a["name"] for a in response.json()["assets"]) == ["active", "inactive"]

    def test_filter_by_uri_pattern(self, test_client, session):
        s3 = AssetModel(name="s3_asset", uri="s3://bucket/key", group="asset")
        gcs = AssetModel(name="gcs_asset", uri="gcs://bucket/key", group="asset")
        session.add_all([s3, gcs])
        session.add(AssetActive.for_asset(s3))
        session.add(AssetActive.for_asset(gcs))
        session.commit()

        response = test_client.get("/assets?uri_pattern=s3")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["s3_asset"]

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_filter_by_dag_ids(self, test_client, session):
        referenced = AssetModel(name="referenced", uri="s3://bucket/referenced", group="asset")
        unreferenced = AssetModel(name="unreferenced", uri="s3://bucket/unreferenced", group="asset")
        session.add_all([referenced, unreferenced])
        session.add(AssetActive.for_asset(referenced))
        session.add(AssetActive.for_asset(unreferenced))
        session.add(DagModel(dag_id="consumer_dag", bundle_name="testing"))
        session.add(DagScheduleAssetReference(dag_id="consumer_dag", asset=referenced))
        session.commit()

        response = test_client.get("/assets?dag_ids=consumer_dag")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["referenced"]

    def test_query_count(self, test_client, session):
        """The asset relationships are eager-loaded, so the query count stays fixed regardless of
        how many assets are returned (a lazy-loading regression would issue queries per asset).

        At least two assets carry a watcher (with a trigger) and an alias so the count guards the
        ``selectinload`` of every relationship, including the nested ``watchers -> trigger`` join.
        """
        assets = [AssetModel(name=f"asset{i}", uri=f"s3://bucket/asset{i}", group="asset") for i in range(5)]
        session.add_all(assets)
        session.add_all(AssetActive.for_asset(asset) for asset in assets)
        session.flush()

        for i in range(2):
            trigger = Trigger(classpath=f"airflow.triggers.testing.TestTrigger{i}", kwargs={})
            session.add(trigger)
            session.flush()
            assets[i].watchers.append(AssetWatcherModel(name=f"watcher{i}", trigger_id=trigger.id))
            assets[i].aliases.append(AssetAliasModel(name=f"alias{i}", group=""))
        session.commit()

        # Two of these queries resolve the caller's readable assets and readable Dags, so the list
        # and its nested Dag and task references can be scoped to them.
        with assert_queries_count(10):
            assert test_client.get("/assets").status_code == 200

    @conf_vars({("core", "multi_team"): "True"})
    def test_query_count_with_multi_team(self, test_client, session):
        """Resolving reference ``team_name`` must not add a query per referencing Dag.

        A missing loader option does not raise: :attr:`DagModel.team_name` falls back to the
        cached ``get_team_name`` resolver instead of tripping ``lazy="raise"``, so only a pinned
        count catches the regression.
        """
        bundle = DagBundleModel(name="team-bundle")
        bundle.teams.append(Team(name="owning-team"))
        session.add(bundle)
        session.flush()

        assets = [AssetModel(name=f"asset{i}", uri=f"s3://bucket/asset{i}", group="asset") for i in range(5)]
        session.add_all(assets)
        session.add_all(AssetActive.for_asset(asset) for asset in assets)
        session.flush()

        for i, asset in enumerate(assets):
            session.add(DagModel(dag_id=f"scheduled_dag{i}", bundle_name="team-bundle"))
            session.add(DagModel(dag_id=f"producing_dag{i}", bundle_name="team-bundle"))
            session.add(DagScheduleAssetReference(dag_id=f"scheduled_dag{i}", asset=asset))
            session.add(TaskOutletAssetReference(dag_id=f"producing_dag{i}", task_id="task", asset=asset))
        session.commit()

        with assert_queries_count(13):
            response = test_client.get("/assets")

        assert response.status_code == 200
        assets_by_name = {asset["name"]: asset for asset in response.json()["assets"]}
        assert assets_by_name["asset0"]["scheduled_dags"][0]["team_name"] == "owning-team"
        assert assets_by_name["asset0"]["producing_tasks"][0]["team_name"] == "owning-team"


RUN_START = pendulum.datetime(2026, 1, 2, 10, tz="UTC")
BEFORE_RUN_START = RUN_START.subtract(hours=1)
AFTER_RUN_START = RUN_START.add(minutes=30)


class TestGetAssetPartitions:
    @pytest.fixture(autouse=True)
    def cleanup_assets(self):
        clear_db_assets()
        clear_db_runs()

        yield

        clear_db_assets()
        clear_db_runs()

    @pytest.fixture
    def asset(self, session):
        asset = AssetModel(name="partitioned", uri="s3://bucket/partitioned", group="asset")
        session.add_all([asset, DagBundleModel(name="testing")])
        session.flush()
        return asset

    @staticmethod
    def _add_producer(session, asset, dag_id="producer"):
        session.add_all(
            [
                DagModel(dag_id=dag_id, bundle_name="testing"),
                TaskOutletAssetReference(dag_id=dag_id, task_id="produce", asset_id=asset.id),
            ]
        )

    @staticmethod
    def _add_run(session, key, state, start_date=RUN_START, dag_id="producer", queued_at=None):
        run = DagRun(
            dag_id=dag_id,
            run_id=f"run_{key}_{state}",
            run_type=DagRunType.SCHEDULED,
            state=state,
            start_date=start_date,
            partition_key=key,
            queued_at=queued_at,
            run_after=RUN_START,
        )
        session.add(run)
        session.flush()
        return run

    @staticmethod
    def _add_event(session, asset, key, timestamp=BEFORE_RUN_START, source_run=None):
        event = AssetEvent(
            asset_id=asset.id,
            partition_key=key,
            timestamp=timestamp,
            source_dag_id=source_run and source_run.dag_id,
            source_run_id=source_run and source_run.run_id,
        )
        session.add(event)
        session.flush()
        return event

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets/1/partitions")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets/1/partitions")
        assert response.status_code == 403

    def test_should_respond_404(self, test_client):
        response = test_client.get("/assets/9999/partitions")
        assert response.status_code == 404

    @pytest.mark.parametrize(
        ("run_state", "run_start_date", "event_timestamp", "expected_status"),
        [
            pytest.param(None, None, BEFORE_RUN_START, "materialized", id="event-without-run"),
            pytest.param(DagRunState.RUNNING, RUN_START, None, "in_progress", id="running-without-event"),
            pytest.param(DagRunState.QUEUED, None, BEFORE_RUN_START, "in_progress", id="cleared-run"),
            pytest.param(DagRunState.FAILED, RUN_START, None, "failed", id="failed-without-event"),
            pytest.param(DagRunState.FAILED, None, BEFORE_RUN_START, "failed", id="failed-before-start"),
            pytest.param(
                DagRunState.FAILED, RUN_START, BEFORE_RUN_START, "failed", id="old-event-then-failed-run"
            ),
            pytest.param(
                DagRunState.FAILED, RUN_START, AFTER_RUN_START, "materialized", id="event-before-run-fails"
            ),
            pytest.param(
                DagRunState.RUNNING, RUN_START, AFTER_RUN_START, "materialized", id="event-of-running-run"
            ),
            pytest.param(
                DagRunState.SUCCESS, RUN_START, BEFORE_RUN_START, "materialized", id="old-event-then-success"
            ),
            pytest.param(DagRunState.SUCCESS, RUN_START, None, None, id="success-without-event"),
        ],
    )
    def test_status_of_a_key(
        self, test_client, session, asset, run_state, run_start_date, event_timestamp, expected_status
    ):
        self._add_producer(session, asset)
        if run_state is not None:
            self._add_run(session, "2026-01-01", run_state, run_start_date)
        if event_timestamp is not None:
            self._add_event(session, asset, "2026-01-01", event_timestamp)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        assert response.status_code == 200
        statuses = [partition["status"] for partition in response.json()["partitions"]]
        assert statuses == ([expected_status] if expected_status else [])

    def test_newest_run_across_producers_decides(self, test_client, session, asset):
        self._add_producer(session, asset, "producer_a")
        self._add_producer(session, asset, "producer_b")
        self._add_run(session, "2026-01-01", DagRunState.FAILED, dag_id="producer_a")
        newest = self._add_run(session, "2026-01-01", DagRunState.RUNNING, dag_id="producer_b")
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        assert response.json()["partitions"] == [
            {
                "partition_key": "2026-01-01",
                "status": "in_progress",
                "last_event_id": None,
                "last_event_timestamp": None,
                "last_producer_run": {
                    "dag_id": "producer_b",
                    "run_id": newest.run_id,
                    "state": "running",
                    "start_date": "2026-01-02T10:00:00Z",
                    "end_date": None,
                },
                "consumer_runs": [],
            }
        ]

    def test_cleared_older_run_is_the_newest_attempt(self, test_client, session, asset):
        self._add_producer(session, asset)
        cleared = self._add_run(session, "2026-01-01", DagRunState.QUEUED, None, queued_at=AFTER_RUN_START)
        newer_row = self._add_run(session, "2026-01-01", DagRunState.SUCCESS, queued_at=BEFORE_RUN_START)
        self._add_event(session, asset, "2026-01-01", RUN_START, source_run=newer_row)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        [partition] = response.json()["partitions"]
        assert partition["status"] == "in_progress"
        assert partition["last_producer_run"]["run_id"] == cleared.run_id

    def test_run_that_emitted_under_another_key_is_not_tied_to_its_own_key(self, test_client, session, asset):
        self._add_producer(session, asset)
        run = self._add_run(session, "batch-42", DagRunState.FAILED)
        self._add_event(session, asset, "emea", AFTER_RUN_START, source_run=run)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        assert [(p["partition_key"], p["status"]) for p in response.json()["partitions"]] == [
            ("emea", "materialized")
        ]

    def test_run_that_also_emitted_under_its_own_key_stays_tied_to_it(self, test_client, session, asset):
        self._add_producer(session, asset, "producer_a")
        self._add_producer(session, asset, "producer_b")
        self._add_run(
            session, "2026-01-01", DagRunState.QUEUED, None, dag_id="producer_a", queued_at=BEFORE_RUN_START
        )
        newest = self._add_run(session, "2026-01-01", DagRunState.FAILED, dag_id="producer_b")
        self._add_event(session, asset, "2026-01-01", AFTER_RUN_START, source_run=newest)
        self._add_event(session, asset, "emea", AFTER_RUN_START, source_run=newest)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions", params={"partition_key_pattern": "2026"})

        [partition] = response.json()["partitions"]
        assert partition["status"] == "materialized"
        assert partition["last_producer_run"]["dag_id"] == "producer_b"

    def test_clearing_a_task_of_a_running_run_makes_its_event_stale(self, test_client, session, asset):
        self._add_producer(session, asset)
        # Clearing resets queued_at; a run that is still running keeps its start_date.
        run = self._add_run(
            session, "2026-01-01", DagRunState.RUNNING, BEFORE_RUN_START, queued_at=AFTER_RUN_START
        )
        self._add_event(session, asset, "2026-01-01", RUN_START, source_run=run)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        [partition] = response.json()["partitions"]
        assert partition["status"] == "in_progress"

    def test_ignores_runs_of_dags_that_do_not_produce_the_asset(self, test_client, session, asset):
        session.add(DagModel(dag_id="unrelated", bundle_name="testing"))
        self._add_run(session, "2026-01-01", DagRunState.FAILED, dag_id="unrelated")
        self._add_producer(session, asset)
        self._add_run(session, None, DagRunState.FAILED)
        self._add_event(session, asset, None)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        assert response.json() == {
            "partitions": [],
            "total_entries": 0,
            "status_counts": {"materialized": 0, "in_progress": 0, "failed": 0},
        }

    def test_latest_event_is_the_one_with_the_latest_timestamp(self, test_client, session, asset):
        self._add_producer(session, asset)
        self._add_run(session, "2026-01-01", DagRunState.FAILED)
        latest = self._add_event(session, asset, "2026-01-01", AFTER_RUN_START)
        # Written later, but for an earlier moment: a higher id is not a later event.
        self._add_event(session, asset, "2026-01-01", BEFORE_RUN_START)
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        [partition] = response.json()["partitions"]
        assert partition["status"] == "materialized"
        assert partition["last_event_id"] == latest.id
        assert partition["last_event_timestamp"] == "2026-01-02T10:30:00Z"

    @pytest.fixture
    def three_statuses(self, session, asset):
        self._add_producer(session, asset)
        self._add_event(session, asset, "2026-01-01")
        self._add_event(session, asset, "2026-01-02")
        self._add_run(session, "2026-01-03", DagRunState.FAILED)
        self._add_run(session, "2026-01-04", DagRunState.RUNNING)
        self._add_event(session, asset, "100%")
        session.commit()

    @pytest.mark.parametrize(
        ("params", "expected_keys", "expected_total", "expected_counts"),
        [
            pytest.param(
                {},
                ["2026-01-04", "2026-01-03", "2026-01-02", "2026-01-01", "100%"],
                5,
                {"materialized": 3, "in_progress": 1, "failed": 1},
                id="all-newest-key-first",
            ),
            pytest.param(
                {"status": ["failed", "in_progress"]},
                ["2026-01-04", "2026-01-03"],
                2,
                {"materialized": 3, "in_progress": 1, "failed": 1},
                id="status-filter-keeps-counts",
            ),
            pytest.param(
                {"partition_key_pattern": "01-0"},
                ["2026-01-04", "2026-01-03", "2026-01-02", "2026-01-01"],
                4,
                {"materialized": 2, "in_progress": 1, "failed": 1},
                id="pattern-narrows-counts",
            ),
            pytest.param(
                {"partition_key_pattern": "%"},
                ["100%"],
                1,
                {"materialized": 1, "in_progress": 0, "failed": 0},
                id="pattern-is-literal",
            ),
            pytest.param(
                {"status": ["in_progress"], "partition_key_pattern": "2026-01-0"},
                ["2026-01-04"],
                1,
                {"materialized": 2, "in_progress": 1, "failed": 1},
                id="single-status",
            ),
            pytest.param(
                {"status": ["failed"], "partition_key_pattern": "01-01"},
                [],
                0,
                {"materialized": 1, "in_progress": 0, "failed": 0},
                id="selected-status-without-keys-keeps-counts",
            ),
            pytest.param(
                {"limit": 2, "offset": 10},
                [],
                5,
                {"materialized": 3, "in_progress": 1, "failed": 1},
                id="offset-past-the-end-keeps-counts",
            ),
            pytest.param(
                {"limit": 2, "offset": 1},
                ["2026-01-03", "2026-01-02"],
                5,
                {"materialized": 3, "in_progress": 1, "failed": 1},
                id="pagination",
            ),
        ],
    )
    @pytest.mark.usefixtures("three_statuses")
    def test_filters_counts_and_pagination(
        self, test_client, asset, params, expected_keys, expected_total, expected_counts
    ):
        response = test_client.get(f"/assets/{asset.id}/partitions", params=params)

        assert response.status_code == 200
        body = response.json()
        assert [partition["partition_key"] for partition in body["partitions"]] == expected_keys
        assert body["total_entries"] == expected_total
        assert body["status_counts"] == expected_counts

    @pytest.mark.usefixtures("three_statuses")
    def test_should_respond_422_for_unknown_status(self, test_client, asset):
        response = test_client.get(f"/assets/{asset.id}/partitions", params={"status": "missing"})
        assert response.status_code == 422

    @pytest.mark.parametrize(
        ("fixture_name", "expected_queries"),
        [
            pytest.param("three_statuses", 6, id="no-consumer-runs"),
            pytest.param("consumed_events", 7, id="consumer-runs"),
        ],
    )
    def test_number_of_queries_does_not_grow_with_keys(
        self, test_client, asset, request, fixture_name, expected_queries
    ):
        # The asset lookup, counts with the page, the last events and their consumer runs, plus
        # two for the access check and the caller's readable Dags. A further query ranks the events
        # a consumer run consumed, but only when there is one. Nothing runs once per key or per run.
        request.getfixturevalue(fixture_name)
        with assert_queries_count(expected_queries):
            test_client.get(f"/assets/{asset.id}/partitions")

    @pytest.fixture
    def consumed_events(self, session, asset):
        """An old and a latest event of one key, each consumed by runs of two consumer Dags."""
        self._add_producer(session, asset)
        old = self._add_event(session, asset, "2026-01-01", BEFORE_RUN_START)
        latest = self._add_event(session, asset, "2026-01-01", AFTER_RUN_START)
        session.add_all(
            DagModel(dag_id=dag_id, bundle_name="testing") for dag_id in ("consumer_a", "consumer_b")
        )
        consumptions = [
            (event, key, dag_id)
            for event, key in ((old, "old"), (latest, "latest"))
            for dag_id in ("consumer_a", "consumer_b")
        ]
        for event, key, dag_id in [*consumptions, (old, "again", "consumer_a")]:
            run = self._add_run(session, key, DagRunState.SUCCESS, dag_id=dag_id)
            run.consumed_asset_events.append(event)
        session.commit()

    @pytest.mark.usefixtures("consumed_events")
    def test_consumer_runs_are_those_of_the_latest_event(self, test_client, asset):
        response = test_client.get(f"/assets/{asset.id}/partitions")

        [partition] = response.json()["partitions"]
        assert sorted((run["dag_id"], run["run_id"]) for run in partition["consumer_runs"]) == [
            ("consumer_a", "run_latest_success"),
            ("consumer_b", "run_latest_success"),
        ]

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    @pytest.mark.usefixtures("consumed_events")
    def test_consumer_runs_leave_out_dags_the_caller_may_not_read(
        self, mock_get_authorized_dag_ids, test_client, asset
    ):
        mock_get_authorized_dag_ids.return_value = {"consumer_b"}

        response = test_client.get(f"/assets/{asset.id}/partitions")

        [partition] = response.json()["partitions"]
        assert [run["dag_id"] for run in partition["consumer_runs"]] == ["consumer_b"]

    def test_only_the_newest_consumed_event_triggers_a_consumer_run(self, test_client, session, asset):
        self._add_producer(session, asset)
        session.add(DagModel(dag_id="consumer", bundle_name="testing"))
        older = self._add_event(session, asset, "2026-01-01", BEFORE_RUN_START)
        newer = self._add_event(session, asset, "2026-01-02", AFTER_RUN_START)
        run = self._add_run(session, "consumed", DagRunState.SUCCESS, dag_id="consumer")
        run.consumed_asset_events.extend([older, newer])
        session.commit()

        response = test_client.get(f"/assets/{asset.id}/partitions")

        triggering_by_key = {
            partition["partition_key"]: [r["triggering"] for r in partition["consumer_runs"]]
            for partition in response.json()["partitions"]
        }
        assert triggering_by_key == {"2026-01-02": [True], "2026-01-01": [False]}

    @pytest.fixture
    def sortable_keys(self, session, asset):
        """Keys b and d tie on the event time; c has no event, d has no producer run."""
        self._add_producer(session, asset)
        self._add_run(session, "a", DagRunState.FAILED, start_date=RUN_START.add(hours=2))
        self._add_run(session, "b", DagRunState.RUNNING, start_date=RUN_START.add(hours=1))
        self._add_run(session, "c", DagRunState.RUNNING, start_date=RUN_START)
        self._add_event(session, asset, "a", BEFORE_RUN_START)
        self._add_event(session, asset, "b", AFTER_RUN_START.add(hours=3))
        self._add_event(session, asset, "d", AFTER_RUN_START.add(hours=3))
        session.commit()

    @pytest.mark.parametrize(
        ("order_by", "expected_keys"),
        [
            pytest.param("partition_key", ["a", "b", "c", "d"], id="partition_key-asc"),
            pytest.param("-partition_key", ["d", "c", "b", "a"], id="partition_key-desc"),
            pytest.param("status", ["a", "c", "b", "d"], id="status-asc"),
            pytest.param("-status", ["b", "d", "c", "a"], id="status-desc"),
            pytest.param("last_event_timestamp", ["a", "b", "d", "c"], id="last_event_timestamp-asc"),
            pytest.param("-last_event_timestamp", ["b", "d", "a", "c"], id="last_event_timestamp-desc"),
            pytest.param("last_producer_run", ["c", "b", "a", "d"], id="last_producer_run-asc"),
            pytest.param("-last_producer_run", ["a", "b", "c", "d"], id="last_producer_run-desc"),
        ],
    )
    @pytest.mark.usefixtures("sortable_keys")
    def test_order_by(self, test_client, asset, order_by, expected_keys):
        response = test_client.get(f"/assets/{asset.id}/partitions", params={"order_by": order_by})

        assert response.status_code == 200
        assert [p["partition_key"] for p in response.json()["partitions"]] == expected_keys

    @pytest.mark.parametrize(("offset", "expected_key"), [(0, "b"), (1, "d")])
    @pytest.mark.usefixtures("sortable_keys")
    def test_order_by_splits_a_tie_across_pages_of_a_status(self, test_client, asset, offset, expected_key):
        response = test_client.get(
            f"/assets/{asset.id}/partitions",
            params={
                "order_by": "-last_event_timestamp",
                "status": "materialized",
                "limit": 1,
                "offset": offset,
            },
        )

        body = response.json()
        assert [p["partition_key"] for p in body["partitions"]] == [expected_key]
        assert body["total_entries"] == 2
        assert body["status_counts"] == {"materialized": 2, "in_progress": 1, "failed": 1}

    @pytest.mark.parametrize("order_by", ["missing", "-missing", "consumer_runs", ""])
    @pytest.mark.usefixtures("sortable_keys")
    def test_should_respond_400_for_unknown_order_by(self, test_client, asset, order_by):
        response = test_client.get(f"/assets/{asset.id}/partitions", params={"order_by": order_by})

        assert response.status_code == 400

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_leaves_out_runs_and_events_of_dags_the_caller_may_not_read(
        self, mock_get_authorized_dag_ids, test_client, session, asset
    ):
        self._add_producer(session, asset, "hidden")
        self._add_producer(session, asset, "readable")
        self._add_run(session, "hidden-run", DagRunState.FAILED, dag_id="hidden")
        hidden_source = self._add_run(session, "hidden-event", DagRunState.SUCCESS, dag_id="hidden")
        self._add_event(session, asset, "hidden-event", AFTER_RUN_START, source_run=hidden_source)
        self._add_run(session, "readable-run", DagRunState.FAILED, dag_id="readable")
        self._add_event(session, asset, "without-source")
        session.commit()
        mock_get_authorized_dag_ids.return_value = {"readable"}

        response = test_client.get(f"/assets/{asset.id}/partitions")

        body = response.json()
        assert [(p["partition_key"], p["status"]) for p in body["partitions"]] == [
            ("without-source", "materialized"),
            ("readable-run", "failed"),
        ]
        assert body["total_entries"] == 2
        assert body["status_counts"] == {"materialized": 1, "in_progress": 0, "failed": 1}
