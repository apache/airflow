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

from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
)
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.temporal import StartOfHourMapper
from airflow.partition_mappers.window import HourWindow
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

from tests_common.test_utils.asserts import assert_queries_count, count_queries
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_assets,
    clear_db_dags,
    clear_db_pakl,
    clear_db_serialized_dags,
)

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

    def test_sort_by_last_asset_event_timestamp_puts_assets_without_events_last(self, test_client, session):
        """
        Regression test: assets that never had an event (NULL last_asset_event_timestamp)
        must sort after assets with a real timestamp in both directions. Without an explicit
        nulls_last(), Postgres's default (nulls sort as the largest value) would put these
        assets *first* when sorting descending -- the opposite of what the UI needs.
        """
        never_updated = AssetModel(name="never_updated", uri="s3://bucket/never_updated", group="asset")
        updated = AssetModel(name="updated", uri="s3://bucket/updated", group="asset")
        session.add_all([never_updated, updated])
        session.add(AssetActive.for_asset(never_updated))
        session.add(AssetActive.for_asset(updated))
        session.flush()

        session.add(AssetEvent(asset_id=updated.id, timestamp=pendulum.datetime(2024, 1, 1)))
        session.commit()

        response = test_client.get("/assets?order_by=-last_asset_event_timestamp")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["updated", "never_updated"]

        response = test_client.get("/assets?order_by=last_asset_event_timestamp")
        assert response.status_code == 200
        assert [a["name"] for a in response.json()["assets"]] == ["updated", "never_updated"]

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

    def test_pagination_with_never_evented_assets(self, test_client, session):
        """Under the default desc sort, paging is stable and never-evented assets land last."""
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
        assert page_one.status_code == 200
        assert page_one.json()["total_entries"] == 4
        assert [a["name"] for a in page_one.json()["assets"]] == ["evented_new", "evented_old"]

        page_two = test_client.get("/assets?limit=2&offset=2")
        assert page_two.status_code == 200
        assert page_two.json()["total_entries"] == 4
        assert {a["name"] for a in page_two.json()["assets"]} == {"never_a", "never_b"}

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

    def test_query_count_does_not_scale_with_asset_count(self, test_client, session):
        """
        The five asset relationships are eager-loaded via subqueryload in a fixed number of
        queries. A regression to lazy loading would issue queries per asset (N+1), so the count
        must not grow between one asset and many.
        """

        def add_active_assets(prefix: str, count: int) -> None:
            for i in range(count):
                asset = AssetModel(name=f"{prefix}{i}", uri=f"s3://bucket/{prefix}{i}", group="asset")
                session.add(asset)
                session.add(AssetActive.for_asset(asset))
            session.commit()

        add_active_assets("one", 1)
        with count_queries() as single:
            assert test_client.get("/assets").status_code == 200

        add_active_assets("many", 4)
        with count_queries() as multiple:
            assert test_client.get("/assets").status_code == 200

        assert sum(single.values()) == sum(multiple.values())
