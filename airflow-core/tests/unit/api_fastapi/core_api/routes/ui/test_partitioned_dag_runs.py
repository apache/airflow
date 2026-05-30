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

import pendulum
import pytest
from sqlalchemy import select

from airflow.models.asset import (
    AssetActive,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    PartitionedAssetKeyLog,
)
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.temporal import StartOfWeekMapper
from airflow.partition_mappers.window import WeekWindow
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


class TestGetPartitionedDagRuns:
    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/partitioned_dag_runs?dag_id=any")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/partitioned_dag_runs?dag_id=any")
        assert response.status_code == 403

    def test_should_response_404(self, test_client):
        assert test_client.get("/partitioned_dag_runs?dag_id=no_such_dag").status_code == 404

    def test_should_response_200_non_partitioned_dag_returns_empty(self, test_client, dag_maker):
        with dag_maker(dag_id="normal", schedule=[Asset(uri="s3://bucket/a", name="a")], serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with assert_queries_count(3):
            resp = test_client.get("/partitioned_dag_runs?dag_id=normal&has_created_dag_run_id=false")
        assert resp.status_code == 200
        assert resp.json() == {"partitioned_dag_runs": [], "total": 0, "asset_expressions": None}

    @pytest.mark.parametrize(
        (
            "num_assets",
            "received_count",
            "fulfilled",
            "has_created_dag_run_id",
            "expected_total",
            "expected_state",
        ),
        [
            (1, 1, False, False, 1, "pending"),
            (1, 1, True, False, 0, None),
            (1, 1, True, True, 1, "running"),
            (3, 0, False, False, 1, "pending"),
            (3, 1, False, False, 1, "pending"),
            (3, 2, False, False, 1, "pending"),
            (3, 3, False, False, 1, "pending"),
        ],
        ids=[
            "filter-pending-included",
            "filter-fulfilled-excluded",
            "filter-fulfilled-included",
            "received-0/3",
            "received-1/3",
            "received-2/3",
            "received-3/3",
        ],
    )
    def test_should_response_200(
        self,
        test_client,
        dag_maker,
        session,
        num_assets,
        received_count,
        fulfilled,
        has_created_dag_run_id,
        expected_total,
        expected_state,
    ):
        uris = [f"s3://bucket/lr{i}" for i in range(num_assets)]
        asset_defs = [Asset(uri=uri, name=f"lr{i}") for i, uri in enumerate(uris)]
        schedule = asset_defs[0]
        for a in asset_defs[1:]:
            schedule = schedule & a

        with dag_maker(
            dag_id="list_dag",
            schedule=PartitionedAssetTimetable(assets=schedule),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        assets = {a.uri: a for a in session.scalars(select(AssetModel).where(AssetModel.uri.in_(uris)))}

        pdr = AssetPartitionDagRun(
            target_dag_id="list_dag",
            partition_key="2024-06-01",
            created_dag_run_id=dr.id if fulfilled else None,
        )
        session.add(pdr)
        session.flush()

        for uri in uris[:received_count]:
            event = AssetEvent(asset_id=assets[uri].id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=assets[uri].id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-06-01",
                    target_dag_id="list_dag",
                    target_partition_key="2024-06-01",
                )
            )
        session.commit()

        # Five batch queries, each independent of len(rows) / num_assets:
        # 1. outer SELECT of AssetPartitionDagRun rows
        # 2. SELECT DagModel WHERE dag_id IN (unique_dag_ids)
        # 3. _fetch_active_assets_per_dag — joined SELECT on AssetModel + DagScheduleAssetReference
        # 4. SELECT PartitionedAssetKeyLog WHERE asset_partition_dag_run_id IN (apdr_ids)
        # 5. SELECT timetable for asset_expressions (cached per response)
        # The count does not scale with num_assets or len(rows), so the bump
        # from main's baseline of 3 (3 → 4 → 5 across this branch) is constant
        # per request, not per row.
        with assert_queries_count(5):
            resp = test_client.get(
                f"/partitioned_dag_runs?dag_id=list_dag"
                f"&has_created_dag_run_id={str(has_created_dag_run_id).lower()}"
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == expected_total
        if expected_total > 0:
            pdr_resp = body["partitioned_dag_runs"][0]
            assert pdr_resp["state"] == expected_state
            assert pdr_resp["total_received"] == received_count
            assert pdr_resp["total_required"] == num_assets

    @pytest.mark.parametrize(
        ("num_target_assets", "num_other_assets", "received_count"),
        [(1, 1, 1), (1, 2, 0), (2, 1, 1)],
    )
    def test_received_count_excludes_other_dags_assets(
        self, test_client, dag_maker, session, num_target_assets, num_other_assets, received_count
    ):

        def _make_schedule(prefix, count):
            assets = [Asset(uri=f"s3://bucket/{prefix}{i}", name=f"{prefix}{i}") for i in range(count)]
            schedule = assets[0]
            for a in assets[1:]:
                schedule = schedule & a
            return [a.uri for a in assets], schedule

        target_uris, target_schedule = _make_schedule("t", num_target_assets)
        other_uris, other_schedule = _make_schedule("o", num_other_assets)

        for dag_id, schedule in [("target", target_schedule), ("other", other_schedule)]:
            with dag_maker(
                dag_id=dag_id, schedule=PartitionedAssetTimetable(assets=schedule), serialized=True
            ):
                EmptyOperator(task_id="t")
            dag_maker.create_dagrun()
            dag_maker.sync_dagbag_to_db()

        all_uris = target_uris + other_uris
        assets = {a.uri: a for a in session.scalars(select(AssetModel).where(AssetModel.uri.in_(all_uris)))}

        # Both Dags need APDRs so an uncorrelated subquery would cross-join and inflate counts.
        for dag_id in ("target", "other"):
            session.add(AssetPartitionDagRun(target_dag_id=dag_id, partition_key="2024-06-01"))
        session.flush()

        pdr = session.scalar(
            select(AssetPartitionDagRun).where(AssetPartitionDagRun.target_dag_id == "target")
        )
        # Log target assets (up to received_count) and all other-Dag assets on the same APDR.
        for uri in target_uris[:received_count] + other_uris:
            event = AssetEvent(asset_id=assets[uri].id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=assets[uri].id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-06-01",
                    target_dag_id="target",
                    target_partition_key="2024-06-01",
                )
            )
        session.commit()

        resp = test_client.get("/partitioned_dag_runs?dag_id=target&has_created_dag_run_id=false")
        assert resp.status_code == 200
        pdr_resp = resp.json()["partitioned_dag_runs"][0]
        assert pdr_resp["total_required"] == num_target_assets
        assert pdr_resp["total_received"] == received_count

    @mock.patch(
        "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids",
        return_value={"other_dag"},
    )
    def test_partitioned_dag_runs_filters_unreadable_dags(self, _, test_client, dag_maker, session):
        schedule = PartitionedAssetTimetable(assets=Asset(uri="s3://bucket/a", name="a"))
        with dag_maker(dag_id="restricted_dag", schedule=schedule, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.sync_dagbag_to_db()
        session.add(AssetPartitionDagRun(target_dag_id="restricted_dag", partition_key="2024-06-01"))
        session.commit()

        resp = test_client.get("/partitioned_dag_runs?has_created_dag_run_id=false")
        assert resp.status_code == 200
        body = resp.json()
        dag_ids = {r["dag_id"] for r in body["partitioned_dag_runs"]}
        assert "restricted_dag" not in dag_ids

    def test_duplicate_events_count_as_one(self, test_client, dag_maker, session):
        """Multiple log entries for the same asset count as 1 received, not N."""
        asset_def = Asset(uri="s3://bucket/dup0", name="dup0")
        with dag_maker(
            dag_id="dup_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset = session.scalar(select(AssetModel).where(AssetModel.uri == "s3://bucket/dup0"))
        pdr = AssetPartitionDagRun(target_dag_id="dup_dag", partition_key="2024-06-01")
        session.add(pdr)
        session.flush()

        # Log 3 events for the same asset — all should collapse to total_received = 1.
        for _ in range(3):
            event = AssetEvent(asset_id=asset.id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=asset.id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-06-01",
                    target_dag_id="dup_dag",
                    target_partition_key="2024-06-01",
                )
            )
        session.commit()

        resp = test_client.get("/partitioned_dag_runs?dag_id=dup_dag&has_created_dag_run_id=false")
        assert resp.status_code == 200
        pdr_resp = resp.json()["partitioned_dag_runs"][0]
        assert pdr_resp["total_required"] == 1
        assert pdr_resp["total_received"] == 1

    def test_non_rollup_any_event_counts_as_one(self, test_client, dag_maker, session):
        """For non-rollup, an event with a different source_partition_key still counts as 1."""
        asset_def = Asset(uri="s3://bucket/nr0", name="nr0")
        with dag_maker(
            dag_id="nr_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset = session.scalar(select(AssetModel).where(AssetModel.uri == "s3://bucket/nr0"))
        pdr = AssetPartitionDagRun(target_dag_id="nr_dag", partition_key="2024-06-01")
        session.add(pdr)
        session.flush()

        # Log an event whose source_partition_key differs from the APDR partition_key.
        event = AssetEvent(asset_id=asset.id, timestamp=pendulum.now())
        session.add(event)
        session.flush()
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset.id,
                asset_event_id=event.id,
                asset_partition_dag_run_id=pdr.id,
                source_partition_key="different-key",
                target_dag_id="nr_dag",
                target_partition_key="2024-06-01",
            )
        )
        session.commit()

        resp = test_client.get("/partitioned_dag_runs?dag_id=nr_dag&has_created_dag_run_id=false")
        assert resp.status_code == 200
        pdr_resp = resp.json()["partitioned_dag_runs"][0]
        assert pdr_resp["total_required"] == 1
        assert pdr_resp["total_received"] == 1

    def test_rollup_mapper_counts_received_upstream_keys(self, test_client, dag_maker, session):
        """For a rollup mapper, only upstream keys in to_upstream() are counted."""
        asset_def = Asset(uri="s3://bucket/daily", name="daily")
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
            window=WeekWindow(),
        )
        with dag_maker(
            dag_id="rollup_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset = session.scalar(select(AssetModel).where(AssetModel.uri == "s3://bucket/daily"))
        # Week starting 2024-06-03 (Monday) needs 7 daily keys.
        pdr = AssetPartitionDagRun(target_dag_id="rollup_dag", partition_key="2024-06-03")
        session.add(pdr)
        session.flush()

        # Receive 2 of the 7 required upstream daily keys.
        for day in ("2024-06-03", "2024-06-04"):
            event = AssetEvent(asset_id=asset.id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=asset.id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key=day,
                    target_dag_id="rollup_dag",
                    target_partition_key="2024-06-03",
                )
            )
        # Also log a key outside the required week — it must not inflate the count.
        stray = AssetEvent(asset_id=asset.id, timestamp=pendulum.now())
        session.add(stray)
        session.flush()
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset.id,
                asset_event_id=stray.id,
                asset_partition_dag_run_id=pdr.id,
                source_partition_key="2024-05-27",  # previous week
                target_dag_id="rollup_dag",
                target_partition_key="2024-06-03",
            )
        )
        session.commit()

        resp = test_client.get("/partitioned_dag_runs?dag_id=rollup_dag&has_created_dag_run_id=false")
        assert resp.status_code == 200
        pdr_resp = resp.json()["partitioned_dag_runs"][0]
        assert pdr_resp["total_required"] == 7
        assert pdr_resp["total_received"] == 2  # only the 2 in-week keys count

    def test_rollup_mapper_failure_treats_asset_as_not_satisfied(self, test_client, dag_maker, session):
        """
        When the rollup mapper raises during ``to_upstream`` evaluation the
        list route must mirror the scheduler's not-yet-satisfied verdict:
        ``total_received`` stays 0 even when a log row exists, and
        ``total_required`` counts the broken asset as 1 so the totals do not
        falsely march toward "ready".
        """
        from unittest.mock import MagicMock

        asset_def = Asset(uri="s3://bucket/rollup_warn_list", name="rollup_warn_list")
        with dag_maker(
            dag_id="rollup_warn_list_dag",
            schedule=PartitionedAssetTimetable(
                assets=asset_def,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
                    window=WeekWindow(),
                ),
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset_model = session.scalar(
            select(AssetModel).where(AssetModel.uri == "s3://bucket/rollup_warn_list")
        )
        pdr = AssetPartitionDagRun(target_dag_id="rollup_warn_list_dag", partition_key="2024-06-03")
        session.add(pdr)
        session.flush()
        event = AssetEvent(asset_id=asset_model.id, timestamp=pendulum.now())
        session.add(event)
        session.flush()
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset_model.id,
                asset_event_id=event.id,
                asset_partition_dag_run_id=pdr.id,
                source_partition_key="2024-06-03",
                target_dag_id="rollup_warn_list_dag",
                target_partition_key="2024-06-03",
            )
        )
        session.commit()

        broken_timetable = MagicMock()
        broken_timetable.get_partition_mapper.side_effect = RuntimeError("mapper exploded")

        with (
            mock.patch(
                "airflow.api_fastapi.core_api.routes.ui.partitioned_dag_runs.load_partitioned_timetables",
                return_value={"rollup_warn_list_dag": broken_timetable},
            ),
            mock.patch("airflow.api_fastapi.core_api.routes.ui.partitioned_dag_runs.log") as mock_log,
        ):
            resp = test_client.get(
                "/partitioned_dag_runs?dag_id=rollup_warn_list_dag&has_created_dag_run_id=false"
            )

        assert resp.status_code == 200
        pdr_resp = resp.json()["partitioned_dag_runs"][0]
        assert pdr_resp["total_required"] == 1
        assert pdr_resp["total_received"] == 0
        assert any(
            call.args[0] == "Failed to evaluate rollup mapper; treating asset as not-yet-satisfied"
            for call in mock_log.warning.mock_calls
        )

    def test_list_route_resolves_each_asset_once_per_row(self, test_client, dag_maker, session):
        """
        ``total_required`` and ``total_received`` share a per-row resolution cache
        so each ``(asset, partition_key)`` resolves once per row instead of twice.
        For 2 rows × 1 rollup asset that means 2 calls, not 4.
        """
        asset_def = Asset(uri="s3://bucket/cache_check", name="cache_check")
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
            window=WeekWindow(),
        )
        with dag_maker(
            dag_id="cache_check_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        for week_start in ("2024-06-03", "2024-06-10"):
            session.add(AssetPartitionDagRun(target_dag_id="cache_check_dag", partition_key=week_start))
        session.commit()

        from airflow.api_fastapi.core_api.routes.ui import partitioned_dag_runs as route_module

        with mock.patch.object(
            route_module, "_resolve_rollup_status", wraps=route_module._resolve_rollup_status
        ) as wrapped:
            resp = test_client.get(
                "/partitioned_dag_runs?dag_id=cache_check_dag&has_created_dag_run_id=false"
            )

        assert resp.status_code == 200
        assert wrapped.call_count == 2

    def test_list_route_total_required_includes_inactive_asset(self, test_client, dag_maker, session):
        """list route's total_required must include inactive assets to stay symmetric with detail route."""
        asset = Asset(uri="s3://bucket/lr_inactive", name="lr_inactive")
        with dag_maker(
            dag_id="lr_inactive_dag",
            schedule=PartitionedAssetTimetable(assets=asset),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        session.add(AssetPartitionDagRun(target_dag_id="lr_inactive_dag", partition_key="2024-06-01"))
        session.commit()

        # Verify total_required == 1 while asset is still active.
        resp_active = test_client.get(
            "/partitioned_dag_runs?dag_id=lr_inactive_dag&has_created_dag_run_id=false"
        )
        assert resp_active.status_code == 200
        assert resp_active.json()["partitioned_dag_runs"][0]["total_required"] == 1

        # Deactivate the asset.
        asset_active_row = session.scalar(
            select(AssetActive).where(
                AssetActive.name == "lr_inactive",
                AssetActive.uri == "s3://bucket/lr_inactive",
            )
        )
        assert asset_active_row is not None
        session.delete(asset_active_row)
        session.commit()

        # total_required must still be 1 — inactive asset is still declared; count must
        # match the detail route which includes inactive assets in its sum.
        resp_inactive = test_client.get(
            "/partitioned_dag_runs?dag_id=lr_inactive_dag&has_created_dag_run_id=false"
        )
        assert resp_inactive.status_code == 200
        assert resp_inactive.json()["partitioned_dag_runs"][0]["total_required"] == 1


class TestGetPendingPartitionedDagRun:
    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/pending_partitioned_dag_run/any_dag/any_key")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/pending_partitioned_dag_run/any_dag/any_key")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("dag_id", "partition_key", "fulfilled"),
        [
            ("no_dag", "no_key", False),
            ("fulfilled_dag", "2024-07-01", True),
        ],
        ids=[
            "not-found",
            "fulfilled-excluded",
        ],
    )
    def test_should_response_404(self, test_client, dag_maker, session, dag_id, partition_key, fulfilled):
        if fulfilled:
            with dag_maker(
                dag_id="fulfilled_dag",
                schedule=PartitionedAssetTimetable(assets=Asset(uri="s3://bucket/ful0", name="ful0")),
                serialized=True,
            ):
                EmptyOperator(task_id="t")

            dr = dag_maker.create_dagrun()
            dag_maker.sync_dagbag_to_db()

            session.add(
                AssetPartitionDagRun(
                    target_dag_id="fulfilled_dag",
                    partition_key="2024-07-01",
                    created_dag_run_id=dr.id,
                )
            )
            session.commit()

        resp = test_client.get(f"/pending_partitioned_dag_run/{dag_id}/{partition_key}")
        assert resp.status_code == 404

    @pytest.mark.parametrize(
        ("num_assets", "received_count"),
        [
            (1, 1),
            (1, 0),
            (2, 1),
            (2, 2),
            (2, 0),
        ],
        ids=[
            "1-asset-received-pending",
            "1-asset-none-received-pending",
            "2-assets-partial-pending",
            "2-assets-all-received-pending",
            "2-assets-none-received-pending",
        ],
    )
    def test_should_response_200(self, test_client, dag_maker, session, num_assets, received_count):
        uris = [f"s3://bucket/dt{i}" for i in range(num_assets)]
        asset_defs = [Asset(uri=uri, name=f"dt{i}") for i, uri in enumerate(uris)]
        schedule = asset_defs[0] if num_assets == 1 else asset_defs[0] & asset_defs[1]

        with dag_maker(
            dag_id="detail_dag",
            schedule=PartitionedAssetTimetable(assets=schedule),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        assets = {a.uri: a for a in session.scalars(select(AssetModel).where(AssetModel.uri.in_(uris)))}

        pdr = AssetPartitionDagRun(
            target_dag_id="detail_dag",
            partition_key="2024-07-01",
            created_dag_run_id=None,
        )
        session.add(pdr)
        session.flush()

        for uri in uris[:received_count]:
            event = AssetEvent(asset_id=assets[uri].id, timestamp=pendulum.now())
            session.add(event)
            session.flush()
            session.add(
                PartitionedAssetKeyLog(
                    asset_id=assets[uri].id,
                    asset_event_id=event.id,
                    asset_partition_dag_run_id=pdr.id,
                    source_partition_key="2024-07-01",
                    target_dag_id="detail_dag",
                    target_partition_key="2024-07-01",
                )
            )
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/detail_dag/2024-07-01")
        assert resp.status_code == 200
        body = resp.json()
        assert body["dag_id"] == "detail_dag"
        assert body["partition_key"] == "2024-07-01"
        assert body["total_required"] == num_assets
        assert body["total_received"] == received_count
        assert len(body["assets"]) == num_assets
        assert body["asset_expression"] is not None
        assert body["created_dag_run_id"] is None

        received_uris = {a["asset_uri"] for a in body["assets"] if a["received"]}
        assert received_uris == set(uris[:received_count])

    def test_is_rollup_false_for_non_rollup_asset(self, test_client, dag_maker, session):
        """is_rollup is False for every asset when the schedule uses identity (non-rollup) mappers."""
        asset_defs = [
            Asset(uri="s3://bucket/nr1", name="nr1"),
            Asset(uri="s3://bucket/nr2", name="nr2"),
        ]
        schedule = asset_defs[0] & asset_defs[1]
        with dag_maker(
            dag_id="nr_detail_dag",
            schedule=PartitionedAssetTimetable(assets=schedule),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        session.add(AssetPartitionDagRun(target_dag_id="nr_detail_dag", partition_key="2024-07-01"))
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/nr_detail_dag/2024-07-01")
        assert resp.status_code == 200
        assets = resp.json()["assets"]
        assert len(assets) == 2
        assert [a["is_rollup"] for a in assets] == [False, False]

    def test_is_rollup_true_for_default_rollup_mapper(self, test_client, dag_maker, session):
        """
        End-to-end coverage for the primary documented rollup pattern: a Dag
        configured with ``default_partition_mapper=RollupMapper(...)`` and no
        ``partition_mapper_config``. Every asset covered by the default
        must report ``is_rollup=True`` and the rollup window's required keys.
        """
        asset_defs = [
            Asset(uri="s3://bucket/weekly_default1", name="weekly_default1"),
            Asset(uri="s3://bucket/weekly_default2", name="weekly_default2"),
        ]
        schedule = asset_defs[0] & asset_defs[1]
        with dag_maker(
            dag_id="rollup_default_dag",
            schedule=PartitionedAssetTimetable(
                assets=schedule,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
                    window=WeekWindow(),
                ),
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        session.add(AssetPartitionDagRun(target_dag_id="rollup_default_dag", partition_key="2024-06-03"))
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/rollup_default_dag/2024-06-03")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_required"] == 14
        assert body["total_received"] == 0
        assets = body["assets"]
        assert len(assets) == 2
        assert [a["is_rollup"] for a in assets] == [True, True]
        assert [a["required_count"] for a in assets] == [7, 7]
        assert [a["received_count"] for a in assets] == [0, 0]

    def test_is_rollup_true_for_rollup_asset(self, test_client, dag_maker, session):
        """is_rollup is True for assets that use a RollupMapper, and keys are populated."""
        asset_def = Asset(uri="s3://bucket/weekly", name="weekly")
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
            window=WeekWindow(),
        )
        with dag_maker(
            dag_id="rollup_detail_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset = session.scalar(select(AssetModel).where(AssetModel.uri == "s3://bucket/weekly"))
        pdr = AssetPartitionDagRun(target_dag_id="rollup_detail_dag", partition_key="2024-06-03")
        session.add(pdr)
        session.flush()

        # Receive one upstream daily key.
        event = AssetEvent(asset_id=asset.id, timestamp=pendulum.now())
        session.add(event)
        session.flush()
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset.id,
                asset_event_id=event.id,
                asset_partition_dag_run_id=pdr.id,
                source_partition_key="2024-06-03",
                target_dag_id="rollup_detail_dag",
                target_partition_key="2024-06-03",
            )
        )
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/rollup_detail_dag/2024-06-03")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_required"] == 7
        assert body["total_received"] == 1
        assets = body["assets"]
        assert len(assets) == 1
        a = assets[0]
        assert a["is_rollup"] is True
        assert a["required_count"] == 7
        assert a["received_count"] == 1
        assert len(a["required_keys"]) == 7
        assert "2024-06-03" in a["required_keys"]
        assert a["received_keys"] == ["2024-06-03"]

    def test_rollup_mapper_failure_treats_asset_as_not_satisfied(self, test_client, dag_maker, session):
        """
        When the rollup mapper raises during ``to_upstream`` evaluation the
        detail route must mirror the scheduler's not-yet-satisfied verdict:
        the asset row reports zero received with no claimed upstream keys,
        and ``received`` is False even when a log row exists.
        """
        from unittest.mock import MagicMock

        asset_def = Asset(uri="s3://bucket/rollup_warn_detail", name="rollup_warn_detail")
        with dag_maker(
            dag_id="rollup_warn_detail_dag",
            schedule=PartitionedAssetTimetable(
                assets=asset_def,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
                    window=WeekWindow(),
                ),
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        asset_model = session.scalar(
            select(AssetModel).where(AssetModel.uri == "s3://bucket/rollup_warn_detail")
        )
        pdr = AssetPartitionDagRun(target_dag_id="rollup_warn_detail_dag", partition_key="2024-06-03")
        session.add(pdr)
        session.flush()
        event = AssetEvent(asset_id=asset_model.id, timestamp=pendulum.now())
        session.add(event)
        session.flush()
        session.add(
            PartitionedAssetKeyLog(
                asset_id=asset_model.id,
                asset_event_id=event.id,
                asset_partition_dag_run_id=pdr.id,
                source_partition_key="2024-06-03",
                target_dag_id="rollup_warn_detail_dag",
                target_partition_key="2024-06-03",
            )
        )
        session.commit()

        broken_timetable = MagicMock()
        broken_timetable.get_partition_mapper.side_effect = RuntimeError("mapper exploded")

        with (
            mock.patch(
                "airflow.api_fastapi.core_api.routes.ui.partitioned_dag_runs.load_partitioned_timetable",
                return_value=broken_timetable,
            ),
            mock.patch("airflow.api_fastapi.core_api.routes.ui.partitioned_dag_runs.log") as mock_log,
        ):
            resp = test_client.get("/pending_partitioned_dag_run/rollup_warn_detail_dag/2024-06-03")

        assert resp.status_code == 200
        a = resp.json()["assets"][0]
        assert a["received_count"] == 0
        assert a["required_count"] == 1
        assert a["received_keys"] == []
        assert a["required_keys"] == []
        assert a["received"] is False
        assert a["mapper_error"] is True
        assert any(
            call.args[0] == "Failed to evaluate rollup mapper; treating asset as not-yet-satisfied"
            for call in mock_log.warning.mock_calls
        )

    def test_partitioned_dag_runs_asset_inactive_true_when_deactivated(self, test_client, dag_maker, session):
        """``asset_inactive`` is True in the detail response when the upstream asset is deactivated."""
        asset_def = Asset(uri="s3://bucket/inactive_detail", name="inactive_detail")
        with dag_maker(
            dag_id="inactive_detail_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        session.add(AssetPartitionDagRun(target_dag_id="inactive_detail_dag", partition_key="2024-07-01"))
        session.commit()

        # Deactivate the asset.
        asset_active_row = session.scalar(
            select(AssetActive).where(
                AssetActive.name == "inactive_detail",
                AssetActive.uri == "s3://bucket/inactive_detail",
            )
        )
        assert asset_active_row is not None
        session.delete(asset_active_row)
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/inactive_detail_dag/2024-07-01")
        assert resp.status_code == 200
        assets = resp.json()["assets"]
        assert len(assets) == 1
        assert assets[0]["asset_inactive"] is True

    def test_partitioned_dag_runs_asset_inactive_false_for_active_asset(
        self, test_client, dag_maker, session
    ):
        """``asset_inactive`` is False in the detail response when the upstream asset is active."""
        asset_def = Asset(uri="s3://bucket/active_detail", name="active_detail")
        with dag_maker(
            dag_id="active_detail_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        session.add(AssetPartitionDagRun(target_dag_id="active_detail_dag", partition_key="2024-07-01"))
        session.commit()

        resp = test_client.get("/pending_partitioned_dag_run/active_detail_dag/2024-07-01")
        assert resp.status_code == 200
        assets = resp.json()["assets"]
        assert len(assets) == 1
        assert assets[0]["asset_inactive"] is False
