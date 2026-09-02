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

from collections.abc import Generator
from datetime import timedelta
from unittest import mock

import pytest
import time_machine
from sqlalchemy import delete, func, select, update

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.core_api.security import PermittedAssetEventFilter
from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetWatcherModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.base import ID_LEN
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from airflow.timetables.simple import PartitionedAtRuntime
from airflow.timetables.trigger import CronPartitionTimetable
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_teams,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms
from tests_common.test_utils.logs import check_last_log

DEFAULT_DATE = timezone.datetime(2020, 6, 11, 18, 0, 0)

pytestmark = pytest.mark.db_test


def _create_assets(session, num: int = 2) -> list[AssetModel]:
    # Event fixtures in this module attribute their events to these Dag ids. /assets/events
    # scopes results to the Dags the caller may read, so the Dags have to exist for the
    # fixtures to represent a real deployment.
    _ensure_dags(session, "source_dag_id", "d", "d1", "d2")
    assets = [
        AssetModel(
            id=i,
            name=f"simple{i}",
            uri=f"s3://bucket/key/{i}",
            group="asset",
            extra={"foo": "bar"},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets)
    session.add_all(AssetActive.for_asset(a) for a in assets)
    session.commit()
    return assets


def _create_assets_with_watchers(session, num: int = 2) -> list[AssetModel]:
    """Create assets with watchers for testing."""
    assets = [
        AssetModel(
            id=i,
            name=f"watched{i}",
            uri=f"s3://watched/bucket/key/{i}",
            group="asset",
            extra={"foo": "bar"},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]

    # Create triggers for the watchers
    triggers = [
        Trigger(
            classpath=f"airflow.triggers.testing.TestTrigger{i}",
            kwargs={"timeout": 60 * i},
            created_date=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]

    session.add_all(assets)
    session.add_all(triggers)
    session.flush()  # Flush to get IDs

    # Create watchers that link assets to triggers
    watchers = [
        AssetWatcherModel(
            name=f"watcher_{i}",
            asset_id=assets[i - 1].id,
            trigger_id=triggers[i - 1].id,
        )
        for i in range(1, 1 + num)
    ]

    session.add_all(watchers)
    session.add_all(AssetActive.for_asset(a) for a in assets)
    session.commit()
    return assets


def _create_assets_with_team_references(session, num: int = 2, refs_per_asset: int = 1) -> list[AssetModel]:
    """Create ``num`` assets, each scheduling and produced by ``refs_per_asset`` team-owned Dags."""
    bundle = DagBundleModel(name="team-bundle-assets")
    bundle.teams.append(Team(name="team-assets"))
    session.add(bundle)
    session.flush()
    assets = [AssetModel(name=f"asset{i}", uri=f"s3://bucket/asset{i}", group="asset") for i in range(num)]
    session.add_all(assets)
    session.add_all(AssetActive.for_asset(asset) for asset in assets)
    session.flush()
    for i, asset in enumerate(assets):
        for j in range(refs_per_asset):
            session.add_all(
                [
                    DagModel(dag_id=f"scheduled_dag{i}_{j}", bundle_name="team-bundle-assets"),
                    DagModel(dag_id=f"producing_dag{i}_{j}", bundle_name="team-bundle-assets"),
                    DagScheduleAssetReference(dag_id=f"scheduled_dag{i}_{j}", asset=asset),
                    TaskOutletAssetReference(dag_id=f"producing_dag{i}_{j}", task_id="task1", asset=asset),
                ]
            )
    session.commit()
    return assets


def _create_assets_with_sensitive_extra(session, num: int = 2) -> None:
    assets = [
        AssetModel(
            id=i,
            name=f"sensitive{i}",
            uri=f"s3://bucket/key/{i}",
            group="asset",
            extra={"password": "bar"},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets)
    session.add_all(AssetActive.for_asset(a) for a in assets)
    session.commit()


def _create_provided_asset(session, asset: AssetModel) -> None:
    session.add(asset)
    session.add(AssetActive.for_asset(asset))
    session.commit()


def _create_asset_aliases(session, num: int = 2) -> None:
    asset_aliases = [
        AssetAliasModel(
            id=i,
            name=f"simple{i}",
            group="alias",
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(asset_aliases)
    session.commit()


def _create_provided_asset_alias(session, asset_alias: AssetAliasModel) -> None:
    session.add(asset_alias)
    session.commit()


def _ensure_dags(session, *dag_ids: str) -> None:
    """Register the Dags that asset-event fixtures attribute their events to.

    ``/assets/events`` scopes events to the Dags the caller may read, and that scoping
    resolves against ``DagModel``. Fixtures that create events with a ``source_dag_id``
    therefore need the corresponding Dag to exist, as it would in a real deployment.
    """
    from airflow.models.dagbundle import DagBundleModel

    session.merge(DagBundleModel(name="testing"))
    session.flush()
    for dag_id in dag_ids:
        if session.get(DagModel, dag_id) is None:
            session.add(DagModel(dag_id=dag_id, bundle_name="testing"))
    session.commit()


def _ensure_source_dag(session) -> None:
    _ensure_dags(session, "source_dag_id")


def _create_assets_events(session, num: int = 2, varying_timestamps=False) -> None:
    _ensure_source_dag(session)
    assets_events = [
        AssetEvent(
            id=i,
            asset_id=i,
            extra={"foo": "bar"},
            source_task_id="source_task_id",
            source_dag_id="source_dag_id",
            source_run_id=f"source_run_id_{i}",
            timestamp=DEFAULT_DATE + timedelta(days=i - 1) if varying_timestamps else DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets_events)
    session.commit()


def _create_assets_events_with_sensitive_extra(session, num: int = 2) -> None:
    _ensure_source_dag(session)
    assets_events = [
        AssetEvent(
            id=i,
            asset_id=i,
            extra={"password": "bar"},
            source_task_id="source_task_id",
            source_dag_id="source_dag_id",
            source_run_id=f"source_run_id_{i}",
            timestamp=DEFAULT_DATE,
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets_events)
    session.commit()


def _create_provided_asset_event(session, asset_event: AssetEvent) -> None:
    _ensure_source_dag(session)
    session.add(asset_event)
    session.commit()


def _create_dag_run(session, num: int = 2):
    _ensure_source_dag(session)
    dag_runs = [
        DagRun(
            dag_id="source_dag_id",
            run_id=f"source_run_id_{i}",
            run_type=DagRunType.MANUAL,
            logical_date=DEFAULT_DATE + timedelta(days=i - 1),
            start_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=DagRunState.SUCCESS,
        )
        for i in range(1, 1 + num)
    ]
    for dag_run in dag_runs:
        dag_run.end_date = DEFAULT_DATE
    session.add_all(dag_runs)
    session.commit()


def _create_asset_dag_run(session, num: int = 2):
    for i in range(1, 1 + num):
        dag_run = session.scalar(select(DagRun).where(DagRun.run_id == f"source_run_id_{i}"))
        asset_event = session.scalar(select(AssetEvent).where(AssetEvent.id == i))
        if dag_run and asset_event:
            dag_run.consumed_asset_events.append(asset_event)
    session.commit()


class TestAssets:
    @pytest.fixture
    def time_freezer(self) -> Generator:
        freezer = time_machine.travel(DEFAULT_DATE, tick=False)
        freezer.start()

        yield

        freezer.stop()

    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_assets()
        clear_db_runs()
        clear_db_dags()
        clear_db_logs()

        yield

        clear_db_assets()
        clear_db_runs()
        clear_db_dags()
        clear_db_teams()
        clear_db_dag_bundles()
        clear_db_logs()

    @provide_session
    def create_assets(self, num: int = 2, *, session) -> list[AssetModel]:
        return _create_assets(session=session, num=num)

    @provide_session
    def create_assets_with_watchers(self, num: int = 2, *, session) -> list[AssetModel]:
        return _create_assets_with_watchers(session=session, num=num)

    @provide_session
    def create_assets_with_sensitive_extra(self, num: int = 2, *, session):
        _create_assets_with_sensitive_extra(session=session, num=num)

    @provide_session
    def create_provided_asset(self, asset: AssetModel, *, session):
        _create_provided_asset(session=session, asset=asset)

    @provide_session
    def create_assets_events(self, num: int = 2, varying_timestamps: bool = False, *, session):
        _create_assets_events(session=session, num=num, varying_timestamps=varying_timestamps)

    @provide_session
    def create_assets_events_with_sensitive_extra(self, num: int = 2, *, session):
        _create_assets_events_with_sensitive_extra(session=session, num=num)

    @provide_session
    def create_provided_asset_event(self, asset_event: AssetEvent, *, session):
        _create_provided_asset_event(session=session, asset_event=asset_event)

    @provide_session
    def create_dag_run(self, num: int = 2, *, session):
        _create_dag_run(num=num, session=session)

    @provide_session
    def create_asset_dag_run(self, num: int = 2, *, session):
        _create_asset_dag_run(num=num, session=session)


class TestGetAssets(TestAssets):
    def test_should_respond_200(self, test_client, session):
        assets1, asset2 = self.create_assets(session=session)
        session.add(AssetModel("inactive", "inactive"))
        session.commit()

        assert len(session.scalars(select(AssetModel)).all()) == 3
        assert len(session.scalars(select(AssetActive)).all()) == 2

        with assert_queries_count(7):
            response = test_client.get("/assets")

        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        assert response_data == {
            "assets": [
                {
                    "id": assets1.id,
                    "name": "simple1",
                    "uri": "s3://bucket/key/1",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [],
                    # No AssetEvent, so no data!
                    "last_asset_event": {"id": None, "timestamp": None},
                },
                {
                    "id": asset2.id,
                    "name": "simple2",
                    "uri": "s3://bucket/key/2",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [],
                    "last_asset_event": {"id": None, "timestamp": None},
                },
            ],
            "total_entries": 2,
        }

    def test_should_respond_200_with_watchers(self, test_client, session):
        """Test that assets with watchers return the watcher information in the API response."""
        asset1, asset2 = self.create_assets_with_watchers(session=session, num=2)

        response = test_client.get("/assets")
        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)

        assert response_data == {
            "assets": [
                {
                    "id": asset1.id,
                    "name": "watched1",
                    "uri": "s3://watched/bucket/key/1",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [
                        {
                            "name": "watcher_1",
                            "trigger_id": asset1.watchers[0].trigger_id,
                            "created_date": tz_datetime_format,
                        }
                    ],
                    "last_asset_event": {"id": None, "timestamp": None},
                },
                {
                    "id": asset2.id,
                    "name": "watched2",
                    "uri": "s3://watched/bucket/key/2",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [
                        {
                            "name": "watcher_2",
                            "trigger_id": asset2.watchers[0].trigger_id,
                            "created_date": tz_datetime_format,
                        }
                    ],
                    "last_asset_event": {"id": None, "timestamp": None},
                },
            ],
            "total_entries": 2,
        }

    def test_should_show_inactive(self, test_client, session):
        asset1, asset2 = self.create_assets(session=session)
        session.add(
            asset3 := AssetModel(
                name="simple3",
                uri="s3://bucket/key/3",
                group="asset",
                extra={"foo": "bar"},
                created_at=DEFAULT_DATE,
                updated_at=DEFAULT_DATE,
            )
        )
        session.commit()

        assert len(session.scalars(select(AssetModel)).all()) == 3
        assert len(session.scalars(select(AssetActive)).all()) == 2

        response = test_client.get("/assets?only_active=0")
        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        assert response_data == {
            "assets": [
                {
                    "id": asset1.id,
                    "name": "simple1",
                    "uri": "s3://bucket/key/1",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [],
                    "last_asset_event": {"id": None, "timestamp": None},
                },
                {
                    "id": asset2.id,
                    "name": "simple2",
                    "uri": "s3://bucket/key/2",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [],
                    "last_asset_event": {"id": None, "timestamp": None},
                },
                {
                    "id": asset3.id,
                    "name": "simple3",
                    "uri": "s3://bucket/key/3",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "scheduled_dags": [],
                    "producing_tasks": [],
                    "consuming_tasks": [],
                    "aliases": [],
                    "watchers": [],
                    "last_asset_event": {"id": None, "timestamp": None},
                },
            ],
            "total_entries": 3,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets")
        assert response.status_code == 403

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/assets?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    def test_assets_references_team_name_none_without_multi_team(self, test_client, session):
        """Without multi-team enabled, references keep ``team_name`` of ``None`` and no lookup happens."""
        _create_assets_with_team_references(session)

        response = test_client.get("/assets")
        assert response.status_code == 200
        assets = {asset["name"]: asset for asset in response.json()["assets"]}
        assert assets["asset0"]["scheduled_dags"][0]["team_name"] is None
        assert assets["asset0"]["producing_tasks"][0]["team_name"] is None

    @conf_vars({("core", "multi_team"): "True"})
    def test_assets_references_include_team_name(self, test_client, session):
        """With multi-team enabled, the owning team is attached to scheduled Dags and producing tasks."""
        _create_assets_with_team_references(session)

        response = test_client.get("/assets")
        assert response.status_code == 200
        assets = {asset["name"]: asset for asset in response.json()["assets"]}
        assert assets["asset0"]["scheduled_dags"][0]["team_name"] == "team-assets"
        assert assets["asset0"]["producing_tasks"][0]["team_name"] == "team-assets"

    @conf_vars({("core", "multi_team"): "True"})
    def test_query_count_with_multi_team(self, test_client, session):
        """Resolving reference ``team_name`` must not add a query per referencing Dag.

        A missing loader option does not raise: :attr:`DagModel.team_name` falls back to the
        cached ``get_team_name`` resolver instead of tripping ``lazy="raise"``, so only a pinned
        count catches the regression.
        """
        _create_assets_with_team_references(session, num=5)

        with assert_queries_count(9):
            response = test_client.get("/assets")

        assert response.status_code == 200
        assets = {asset["name"]: asset for asset in response.json()["assets"]}
        assert assets["asset4"]["scheduled_dags"][0]["team_name"] == "team-assets"
        assert assets["asset4"]["producing_tasks"][0]["team_name"] == "team-assets"

    @pytest.mark.parametrize(
        ("params", "expected_assets"),
        [
            ({"name_pattern": "s3"}, {"s3://folder/key"}),
            ({"name_pattern": "bucket"}, {"gcp://bucket/key", "wasb://some_asset_bucket_/key"}),
            (
                {"name_pattern": "asset"},
                {"somescheme://asset/key", "wasb://some_asset_bucket_/key"},
            ),
            (
                {"name_pattern": ""},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
            ({"name_prefix_pattern": "s3"}, {"s3://folder/key"}),
            ({"name_prefix_pattern": "gcp"}, {"gcp://bucket/key"}),
            ({"name_prefix_pattern": "some"}, {"somescheme://asset/key"}),
            ({"name_prefix_pattern": "wasb"}, {"wasb://some_asset_bucket_/key"}),
            (
                {"name_prefix_pattern": "~"},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
        ],
    )
    @provide_session
    def test_filter_assets_by_name_pattern_works(self, test_client, params, expected_assets, *, session):
        asset1 = AssetModel("s3-folder-key", "s3://folder/key")
        asset2 = AssetModel("gcp-bucket-key", "gcp://bucket/key")
        asset3 = AssetModel("some-asset-key", "somescheme://asset/key")
        asset4 = AssetModel("wasb-some_asset_bucket_-key", "wasb://some_asset_bucket_/key")

        assets = [asset1, asset2, asset3, asset4]
        for a in assets:
            self.create_provided_asset(asset=a)

        response = test_client.get("/assets", params=params)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls

    @pytest.mark.parametrize(
        ("params", "expected_assets"),
        [
            ({"uri_pattern": "s3"}, {"s3://folder/key"}),
            ({"uri_pattern": "bucket"}, {"gcp://bucket/key", "wasb://some_asset_bucket_/key"}),
            (
                {"uri_pattern": "asset"},
                {"somescheme://asset/key", "wasb://some_asset_bucket_/key"},
            ),
            (
                {"uri_pattern": ""},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
            ({"uri_prefix_pattern": "s3://"}, {"s3://folder/key"}),
            ({"uri_prefix_pattern": "gcp://"}, {"gcp://bucket/key"}),
            ({"uri_prefix_pattern": "somescheme"}, {"somescheme://asset/key"}),
            ({"uri_prefix_pattern": "wasb://"}, {"wasb://some_asset_bucket_/key"}),
            (
                {"uri_prefix_pattern": "~"},
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://asset/key",
                    "wasb://some_asset_bucket_/key",
                },
            ),
            # Exact-match ``uri`` filter: only the asset whose full URI matches is returned.
            ({"uri": "s3://folder/key"}, {"s3://folder/key"}),
            ({"uri": "gcp://bucket/key"}, {"gcp://bucket/key"}),
            # Repeated ``uri`` params match any of the given URIs.
            ({"uri": ["s3://folder/key", "gcp://bucket/key"]}, {"s3://folder/key", "gcp://bucket/key"}),
            # A substring of an existing URI must NOT match (unlike uri_pattern).
            ({"uri": "s3://folder"}, set()),
            ({"uri": "does-not-exist://key"}, set()),
        ],
    )
    @provide_session
    def test_filter_assets_by_uri_pattern_works(self, test_client, params, expected_assets, *, session):
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        asset4 = AssetModel("wasb://some_asset_bucket_/key")

        assets = [asset1, asset2, asset3, asset4]
        for a in assets:
            self.create_provided_asset(asset=a)

        response = test_client.get("/assets", params=params)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls

    @pytest.mark.parametrize(("dag_ids", "expected_num"), [("dag1,dag2", 2), ("dag3", 1), ("dag2,dag3", 2)])
    @provide_session
    def test_filter_assets_by_dag_ids_works(
        self, test_client, dag_ids, expected_num, testing_dag_bundle, *, session
    ):
        session.execute(delete(DagModel))
        session.commit()
        bundle_name = "testing"

        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        session.add_all(
            [
                asset1,
                asset2,
                asset3,
                AssetActive.for_asset(asset1),
                AssetActive.for_asset(asset2),
                AssetActive.for_asset(asset3),
                DagModel(dag_id="dag1", bundle_name=bundle_name),
                DagModel(dag_id="dag2", bundle_name=bundle_name),
                DagModel(dag_id="dag3", bundle_name=bundle_name),
                DagScheduleAssetReference(dag_id="dag1", asset=asset1),
                DagScheduleAssetReference(dag_id="dag2", asset=asset2),
                TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3),
            ],
        )
        session.commit()
        response = test_client.get(
            f"/assets?dag_ids={dag_ids}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num

    @pytest.mark.parametrize(
        ("dag_ids", "uri_pattern", "expected_num"),
        [("dag1,dag2", "folder", 1), ("dag3", "nothing", 0), ("dag2,dag3", "key", 2)],
    )
    @provide_session
    def test_filter_assets_by_dag_ids_and_uri_pattern_works(
        self, test_client, dag_ids, uri_pattern, expected_num, testing_dag_bundle, *, session
    ):
        session.execute(delete(DagModel))
        session.commit()
        bundle_name = "testing"

        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        session.add_all(
            [
                asset1,
                asset2,
                asset3,
                AssetActive.for_asset(asset1),
                AssetActive.for_asset(asset2),
                AssetActive.for_asset(asset3),
                DagModel(dag_id="dag1", bundle_name=bundle_name),
                DagModel(dag_id="dag2", bundle_name=bundle_name),
                DagModel(dag_id="dag3", bundle_name=bundle_name),
                DagScheduleAssetReference(dag_id="dag1", asset=asset1),
                DagScheduleAssetReference(dag_id="dag2", asset=asset2),
                TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3),
            ]
        )
        session.commit()
        response = test_client.get(
            f"/assets?dag_ids={dag_ids}&uri_pattern={uri_pattern}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num


class TestGetAssetsEndpointPagination(TestAssets):
    @pytest.mark.parametrize(
        ("url", "expected_asset_uris"),
        [
            # Limit test data
            ("/assets?limit=1", ["s3://bucket/key/1"]),
            ("/assets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/assets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 52)]),
            ("/assets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 54)]),
            # Limit and offset test data
            ("/assets?offset=50&limit=50", [f"s3://bucket/key/{i}" for i in range(51, 101)]),
            ("/assets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ],
    )
    def test_limit_and_offset(self, test_client, url, expected_asset_uris):
        self.create_assets(num=110)

        response = test_client.get(url)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json()["assets"]]
        assert asset_uris == expected_asset_uris

    def test_should_respect_page_size_limit_default(self, test_client):
        self.create_assets(num=110)

        response = test_client.get("/assets")

        assert response.status_code == 200
        assert len(response.json()["assets"]) == 50


class TestAssetAliases:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_assets()
        clear_db_runs()
        clear_db_dags()

    def teardown_method(self) -> None:
        clear_db_assets()
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()

    @provide_session
    def create_asset_aliases(self, num: int = 2, *, session):
        _create_asset_aliases(num=num, session=session)

    @provide_session
    def create_provided_asset_alias(self, asset_alias: AssetAliasModel, *, session):
        _create_provided_asset_alias(session=session, asset_alias=asset_alias)


class TestGetAssetAliases(TestAssetAliases):
    def test_should_respond_200(self, test_client, *, session):
        self.create_asset_aliases()
        asset_aliases = session.scalars(select(AssetAliasModel)).all()
        assert len(asset_aliases) == 2

        with assert_queries_count(2):
            response = test_client.get("/assets/aliases")

        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_aliases": [
                {"id": 1, "name": "simple1", "group": "alias"},
                {"id": 2, "name": "simple2", "group": "alias"},
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/assets/aliases?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        ("params", "expected_asset_aliases"),
        [
            ({"name_pattern": "foo"}, {"foo1"}),
            ({"name_pattern": "1"}, {"foo1", "bar12"}),
            ({"uri_pattern": ""}, {"foo1", "bar12", "bar2", "bar3", "rex23"}),
            ({"name_prefix_pattern": "foo"}, {"foo1"}),
            ({"name_prefix_pattern": "bar"}, {"bar12", "bar2", "bar3"}),
            ({"name_prefix_pattern": "~"}, {"foo1", "bar12", "bar2", "bar3", "rex23"}),
        ],
    )
    @provide_session
    def test_filter_assets_by_name_pattern_works(
        self, test_client, params, expected_asset_aliases, *, session
    ):
        asset_alias1 = AssetAliasModel(name="foo1")
        asset_alias2 = AssetAliasModel(name="bar12")
        asset_alias3 = AssetAliasModel(name="bar2")
        asset_alias4 = AssetAliasModel(name="bar3")
        asset_alias5 = AssetAliasModel(name="rex23")

        asset_aliases = [asset_alias1, asset_alias2, asset_alias3, asset_alias4, asset_alias5]
        for a in asset_aliases:
            self.create_provided_asset_alias(a)

        response = test_client.get("/assets/aliases", params=params)
        assert response.status_code == 200
        alias_names = {asset_alias["name"] for asset_alias in response.json()["asset_aliases"]}
        assert expected_asset_aliases == alias_names


class TestGetAssetAliasesEndpointPagination(TestAssetAliases):
    @pytest.mark.parametrize(
        ("url", "expected_asset_aliases"),
        [
            # Limit test data
            ("/assets/aliases?limit=1", ["simple1"]),
            ("/assets/aliases?limit=100", [f"simple{i}" for i in range(1, 101)]),
            # Offset test data
            ("/assets/aliases?offset=1", [f"simple{i}" for i in range(2, 52)]),
            ("/assets/aliases?offset=3", [f"simple{i}" for i in range(4, 54)]),
            # Limit and offset test data
            ("/assets/aliases?offset=3&limit=3", ["simple4", "simple5", "simple6"]),
        ],
    )
    def test_limit_and_offset(self, test_client, url, expected_asset_aliases):
        self.create_asset_aliases(num=110)

        response = test_client.get(url)

        assert response.status_code == 200
        alias_names = [asset["name"] for asset in response.json()["asset_aliases"]]
        assert alias_names == expected_asset_aliases

    def test_should_respect_page_size_limit_default(self, test_client):
        self.create_asset_aliases(num=110)
        response = test_client.get("/assets/aliases")
        assert response.status_code == 200
        assert len(response.json()["asset_aliases"]) == 50


class TestGetAssetEventsPerDagScoping(TestAssets):
    """``/assets/events`` returns only events the caller is entitled to see.

    An event produced by a Dag's task is scoped to that Dag's readability. An event with no
    source Dag — created through the API, or emitted by a watcher — carries no per-Dag key to
    authorize on and stays visible.
    """

    def test_filter_scopes_to_source_dag_and_keeps_dagless_events(self):
        """The clause admits the readable Dags and rows with no source Dag."""
        rendered = str(PermittedAssetEventFilter({"readable_dag"}).to_orm(select(AssetEvent)))

        assert "source_dag_id IN" in rendered
        assert "source_dag_id IS NULL" in rendered

    def test_filter_with_no_readable_dags_still_admits_dagless_events(self, session):
        """A caller who may read no Dag at all still sees events that belong to no Dag."""
        self.create_assets(session=session, num=1)
        session.add(AssetEvent(id=1, asset_id=1, extra={}, timestamp=DEFAULT_DATE))
        session.add(
            AssetEvent(id=2, asset_id=1, extra={}, source_dag_id="source_dag_id", timestamp=DEFAULT_DATE)
        )
        session.commit()

        statement = PermittedAssetEventFilter(set()).to_orm(select(AssetEvent))
        visible = session.scalars(statement).all()

        assert [event.id for event in visible] == [1]

    def test_filter_admits_only_events_from_readable_dags(self, session):
        """An event produced by a Dag the caller cannot read is not returned."""
        self.create_assets(session=session, num=1)
        session.add(
            AssetEvent(id=1, asset_id=1, extra={}, source_dag_id="source_dag_id", timestamp=DEFAULT_DATE)
        )
        session.add(AssetEvent(id=2, asset_id=1, extra={}, source_dag_id="other_dag", timestamp=DEFAULT_DATE))
        session.commit()

        statement = PermittedAssetEventFilter({"source_dag_id"}).to_orm(select(AssetEvent))
        visible = session.scalars(statement).all()

        assert [event.id for event in visible] == [1]

    @pytest.mark.parametrize(
        ("readable_dags", "expected_ids"),
        [
            pytest.param(["source_dag_id"], [1, 3], id="one-readable-dag-plus-dagless"),
            pytest.param(["source_dag_id", "other_dag"], [1, 2, 3], id="both-dags-readable"),
            pytest.param([], [3], id="no-readable-dags-still-sees-dagless"),
        ],
    )
    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_endpoint_returns_only_events_the_caller_may_read(
        self, mock_get_authorized_dag_ids, test_client, session, readable_dags, expected_ids
    ):
        """End-to-end: the route itself scopes the response, not just the filter class."""
        mock_get_authorized_dag_ids.return_value = set(readable_dags)

        self.create_assets(session=session, num=1)
        session.add_all(
            [
                AssetEvent(id=1, asset_id=1, extra={}, source_dag_id="source_dag_id", timestamp=DEFAULT_DATE),
                AssetEvent(id=2, asset_id=1, extra={}, source_dag_id="other_dag", timestamp=DEFAULT_DATE),
                # No source Dag: created through the API or emitted by a watcher.
                AssetEvent(id=3, asset_id=1, extra={}, timestamp=DEFAULT_DATE),
            ]
        )
        session.commit()

        response = test_client.get("/assets/events")

        assert response.status_code == 200
        body = response.json()
        assert sorted(event["id"] for event in body["asset_events"]) == expected_ids
        # The count must be scoped too, so the existence of hidden events does not leak.
        assert body["total_entries"] == len(expected_ids)


class TestGetAssetEvents(TestAssets):
    def test_should_respond_200(self, test_client, session):
        asset1, asset2 = self.create_assets(session=session)
        self.create_assets_events(session=session)
        self.create_dag_run(session=session)
        self.create_asset_dag_run(session=session)
        assets = session.scalars(select(AssetEvent)).all()
        session.commit()
        assert len(assets) == 2

        # 5 rather than 4: resolving the caller's readable Dags, so events can be scoped
        # to them, costs one additional query — the same cost the queued-events routes pay.
        with assert_queries_count(5):
            response = test_client.get("/assets/events")

        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_events": [
                {
                    "id": 1,
                    "asset_id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "group": "asset",
                    "name": "simple1",
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_1",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_1",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "partition_key": None,
                            "triggering": True,
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "partition_key": None,
                },
                {
                    "id": 2,
                    "asset_id": 2,
                    "uri": "s3://bucket/key/2",
                    "group": "asset",
                    "name": "simple2",
                    "extra": {"foo": "bar"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_2",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_2",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(
                                DEFAULT_DATE + timedelta(days=1),
                            ),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "partition_key": None,
                            "triggering": True,
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "partition_key": None,
                },
            ],
            "total_entries": 2,
        }

    def test_only_most_recent_consumed_event_is_flagged_as_triggering(self, test_client, session):
        """A run consuming several events marks only its most recent consumed event as triggering it."""
        self.create_assets(num=1)
        older_event = AssetEvent(id=1, asset_id=1, extra={}, timestamp=DEFAULT_DATE)
        newer_event = AssetEvent(id=2, asset_id=1, extra={}, timestamp=DEFAULT_DATE + timedelta(days=1))
        session.add_all([older_event, newer_event])
        dag_run = DagRun(
            dag_id="source_dag_id",
            run_id="run_1",
            run_type=DagRunType.MANUAL,
            logical_date=DEFAULT_DATE + timedelta(days=1),
            start_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=DagRunState.SUCCESS,
        )
        dag_run.end_date = DEFAULT_DATE
        session.add(dag_run)
        session.flush()
        dag_run.consumed_asset_events.extend([older_event, newer_event])
        session.commit()

        response = test_client.get("/assets/events")

        assert response.status_code == 200
        events = {event["id"]: event for event in response.json()["asset_events"]}
        assert events[1]["created_dagruns"][0]["triggering"] is False
        assert events[2]["created_dagruns"][0]["triggering"] is True

    def test_should_return_created_dag_run_without_start_date(self, test_client, session):
        self.create_assets(num=1, session=session)
        _ensure_dags(session, "producer_dag")
        asset_event = AssetEvent(
            asset_id=1,
            source_dag_id="producer_dag",
            source_run_id="producer_run",
            timestamp=DEFAULT_DATE,
        )
        dag_run = DagRun(
            dag_id="consumer_dag",
            run_id="asset-triggered-run",
            run_type=DagRunType.ASSET_TRIGGERED,
            logical_date=DEFAULT_DATE,
            start_date=None,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=DagRunState.QUEUED,
        )
        dag_run.consumed_asset_events.append(asset_event)
        session.add(dag_run)
        session.commit()

        response = test_client.get("/assets/events")

        assert response.status_code == 200
        assert response.json()["asset_events"][0]["created_dagruns"][0]["start_date"] is None

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets/events")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets/events")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("params", "total_entries"),
        [
            ({"asset_id": "2"}, 1),
            ({"source_dag_id": "source_dag_id"}, 2),
            ({"source_task_id": "source_task_id"}, 2),
            ({"source_run_id": "source_run_id_1"}, 1),
            ({"source_map_index": "-1"}, 2),
            ({"name_pattern": "simple1"}, 1),
            ({"name_pattern": "simple%"}, 2),
            ({"name_pattern": "nonexistent"}, 0),
            ({"name_prefix_pattern": "simple1"}, 1),
            ({"name_prefix_pattern": "simple"}, 2),
            ({"name_prefix_pattern": "nonexistent"}, 0),
        ],
    )
    @provide_session
    def test_filtering(self, test_client, params, total_entries, *, session):
        self.create_assets()
        self.create_assets_events()
        self.create_dag_run()
        self.create_asset_dag_run()
        response = test_client.get("/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == total_entries

    @pytest.mark.parametrize(
        ("params", "expected_ids"),
        [
            # Test Case 1: Filtering with both timestamp_gte and timestamp_lte set to the same date
            (
                {
                    "timestamp_gte": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "timestamp_lte": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
                [1],  # expected_ids for events exactly on DEFAULT_DATE
            ),
            # Test Case 2: Filtering events greater than or equal to a certain timestamp and less than or equal to another
            (
                {
                    "timestamp_gte": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "timestamp_lte": from_datetime_to_zulu_without_ms(DEFAULT_DATE + timedelta(days=1)),
                },
                [1, 2],  # expected_ids for events within the date range
            ),
            # Test Case 3: timestamp_gte later than timestamp_lte with no events in range
            (
                {
                    "timestamp_gte": from_datetime_to_zulu_without_ms(DEFAULT_DATE + timedelta(days=1)),
                    "timestamp_lte": from_datetime_to_zulu_without_ms(DEFAULT_DATE - timedelta(days=1)),
                },
                [],  # expected_ids for events outside the range
            ),
            # Test Case 4: timestamp_gte earlier than timestamp_lte, allowing events within the range
            (
                {
                    "timestamp_gte": from_datetime_to_zulu_without_ms(DEFAULT_DATE + timedelta(days=1)),
                    "timestamp_lte": from_datetime_to_zulu_without_ms(DEFAULT_DATE + timedelta(days=2)),
                },
                [2, 3],  # expected_ids for events within the date range
            ),
        ],
    )
    def test_filter_by_timestamp_gte_and_lte(self, test_client, params, expected_ids, session):
        # Create sample assets and asset events with specified timestamps
        self.create_assets()
        self.create_assets_events(num=3, varying_timestamps=True)
        self.create_dag_run()
        self.create_asset_dag_run()

        # Test with both timestamp_gte and timestamp_lte filters
        response = test_client.get("/assets/events", params=params)

        assert response.status_code == 200
        asset_event_ids = [asset_event["id"] for asset_event in response.json()["asset_events"]]

        assert asset_event_ids == expected_ids

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/assets/events?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        ("params", "expected_asset_ids"),
        [
            # Limit test data
            ({"limit": "1"}, [1]),
            ({"limit": "100"}, list(range(1, 101))),
            # Offset test data
            ({"offset": "1"}, list(range(2, 52))),
            ({"offset": "3"}, list(range(4, 54))),
        ],
    )
    def test_limit_and_offset(self, test_client, params, expected_asset_ids):
        self.create_assets(num=110)
        self.create_assets_events(num=110)
        self.create_dag_run(num=110)
        self.create_asset_dag_run(num=110)

        response = test_client.get("/assets/events", params=params)

        assert response.status_code == 200
        asset_ids = [asset["id"] for asset in response.json()["asset_events"]]
        assert asset_ids == expected_asset_ids

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        self.create_assets_with_sensitive_extra()
        self.create_assets_events_with_sensitive_extra()
        self.create_dag_run()
        self.create_asset_dag_run()
        response = test_client.get("/assets/events")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_events": [
                {
                    "id": 1,
                    "asset_id": 1,
                    "uri": "s3://bucket/key/1",
                    "group": "asset",
                    "name": "sensitive1",
                    "extra": {"password": "***"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_1",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_1",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "partition_key": None,
                            "triggering": True,
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "partition_key": None,
                },
                {
                    "id": 2,
                    "asset_id": 2,
                    "uri": "s3://bucket/key/2",
                    "group": "asset",
                    "name": "sensitive2",
                    "extra": {"password": "***"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_2",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_2",
                            "dag_id": "source_dag_id",
                            "logical_date": from_datetime_to_zulu_without_ms(
                                DEFAULT_DATE + timedelta(days=1),
                            ),
                            "start_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "end_date": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "state": "success",
                            "data_interval_start": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "data_interval_end": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                            "partition_key": None,
                            "triggering": True,
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                    "partition_key": None,
                },
            ],
            "total_entries": 2,
        }


class TestGetAssetEventsPartitionKeyRegex(TestAssets):
    """Tests for partition_key_regexp_pattern regex filter on GET /assets/events.

    Patterns are written to work consistently across PostgreSQL (~),
    MySQL (REGEXP), and SQLite (re.match), including both anchored and
    unanchored expressions where appropriate.
    """

    @pytest.fixture(autouse=True)
    def _enable_regexp_query_filters(self):
        with conf_vars({("api", "regexp_query_timeout"): "30"}):
            yield

    @pytest.fixture(autouse=True)
    def _create_partition_key_test_data(self, setup, session):
        _create_assets(session=session)
        events = [
            AssetEvent(
                asset_id=1,
                extra={},
                source_task_id="t",
                source_dag_id="d",
                source_run_id="r1",
                partition_key="2024-01-01",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=2,
                extra={},
                source_task_id="t",
                source_dag_id="d",
                source_run_id="r2",
                partition_key="2024-01-02",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={},
                source_task_id="t",
                source_dag_id="d",
                source_run_id="r3",
                partition_key="us|2024-01-01",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=2,
                extra={},
                source_task_id="t",
                source_dag_id="d",
                source_run_id="r4",
                partition_key="eu|2024-01-01",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={},
                source_task_id="t",
                source_dag_id="d",
                source_run_id="r5",
                partition_key="apac|2024-03-20",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={},
                source_task_id="t",
                source_dag_id="d",
                source_run_id="r6",
                partition_key=None,
                timestamp=DEFAULT_DATE,
            ),
        ]
        session.add_all(events)
        session.commit()

    @pytest.mark.parametrize(
        ("partition_key_regexp_pattern", "expected_count"),
        [
            ("^2024-01-01$", 1),
            ("^2024-01-", 2),
            ("^us\\|", 1),
            (".*\\|2024-01-01$", 2),
            ("^(us|eu)\\|", 2),
            ("^nonexistent", 0),
        ],
    )
    def test_partition_key_regexp_pattern_filtering(
        self, test_client, partition_key_regexp_pattern, expected_count
    ):
        response = test_client.get(
            "/assets/events", params={"partition_key_regexp_pattern": partition_key_regexp_pattern}
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_count

    @pytest.mark.parametrize(
        ("params", "expected_count"),
        [
            ({"partition_key_regexp_pattern": "^us\\|", "asset_id": "1"}, 1),
            ({"partition_key_regexp_pattern": "^us\\|", "asset_id": "2"}, 0),
            ({"partition_key_regexp_pattern": ".*\\|2024-01-01$", "source_dag_id": "d"}, 2),
            ({"partition_key_regexp_pattern": ".*\\|2024-01-01$", "source_dag_id": "other"}, 0),
        ],
    )
    def test_partition_key_regexp_pattern_combined_filters(self, test_client, params, expected_count):
        response = test_client.get("/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_count

    def test_partition_key_regexp_pattern_invalid_regex_returns_400(self, test_client):
        response = test_client.get(
            "/assets/events", params={"partition_key_regexp_pattern": "[invalid(regex"}
        )
        assert response.status_code == 400
        assert "Invalid regular expression" in response.json()["detail"]

    def test_partition_key_regexp_pattern_disabled_returns_400(self, test_client):
        with conf_vars({("api", "regexp_query_timeout"): "0"}):
            response = test_client.get("/assets/events", params={"partition_key_regexp_pattern": "^2024-"})
        assert response.status_code == 400
        assert "disabled" in response.json()["detail"]

    def test_exact_match_works_when_regex_disabled(self, test_client):
        with conf_vars({("api", "regexp_query_timeout"): "0"}):
            response = test_client.get("/assets/events", params={"partition_key": "2024-01-01"})
        assert response.status_code == 200
        assert response.json()["total_entries"] == 1

    def test_partition_key_exact_match_via_regex(self, test_client):
        response = test_client.get("/assets/events", params={"partition_key_regexp_pattern": "^2024-01-01$"})
        assert response.status_code == 200
        assert response.json()["total_entries"] == 1

    @pytest.mark.parametrize(
        ("partition_key", "expected_count"),
        [
            ("2024-01-01", 1),
            ("us|2024-01-01", 1),
            ("nonexistent", 0),
        ],
    )
    def test_partition_key_exact_match(self, test_client, partition_key, expected_count):
        response = test_client.get("/assets/events", params={"partition_key": partition_key})
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_count

    def test_partition_key_and_pattern_combined(self, test_client):
        # Both filters are allowed and combine with AND: a disjoint pair yields no results.
        response = test_client.get(
            "/assets/events",
            params={"partition_key": "2024-01-01", "partition_key_regexp_pattern": "^2025-"},
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == 0


class TestGetAssetEventsExtraFilter(TestAssets):
    @pytest.fixture
    def _setup(self, session):
        self.create_assets(num=2, session=session)
        events = [
            AssetEvent(
                asset_id=1,
                extra={"region": "us", "env": "prod"},
                source_task_id="t1",
                source_dag_id="d1",
                source_run_id="r1",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={"region": "eu", "env": "prod"},
                source_task_id="t1",
                source_dag_id="d1",
                source_run_id="r2",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=2,
                extra={"region": "us", "env": "staging"},
                source_task_id="t2",
                source_dag_id="d2",
                source_run_id="r3",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={},
                source_task_id="t1",
                source_dag_id="d1",
                source_run_id="r4",
                timestamp=DEFAULT_DATE,
            ),
        ]
        session.add_all(events)
        session.commit()

    @pytest.mark.usefixtures("_setup")
    @pytest.mark.parametrize(
        ("params", "expected_count"),
        [
            ({"extra": "region=us"}, 2),
            ({"extra": "region=eu"}, 1),
            ({"extra": "env=prod"}, 2),
            ({"extra": "env=staging"}, 1),
            ({"extra": "region=ap"}, 0),
            ({"extra": "nonexistent=us"}, 0),
            ({}, 4),
        ],
    )
    def test_extra_filter(self, test_client, params, expected_count):
        response = test_client.get("/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_count

    @pytest.mark.usefixtures("_setup")
    def test_extra_filter_combined_with_asset_id(self, test_client):
        response = test_client.get("/assets/events", params={"extra": "region=us", "asset_id": "1"})
        assert response.status_code == 200
        assert response.json()["total_entries"] == 1

    @pytest.mark.usefixtures("_setup")
    @pytest.mark.parametrize(
        ("params", "expected_count"),
        [
            ([("extra", "region=us"), ("extra", "env=prod")], 1),
            ([("extra", "region=eu"), ("extra", "env=prod")], 1),
            ([("extra", "region=us"), ("extra", "env=staging")], 1),
            ([("extra", "region=eu"), ("extra", "env=staging")], 0),
        ],
    )
    def test_extra_filter_multiple_keys(self, test_client, params, expected_count):
        response = test_client.get("/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_count


class TestGetAssetEventsExtraFilterSpecialKeys(TestAssets):
    """
    Keys containing JSON-path metacharacters must be matched literally on every backend.

    PostgreSQL (``@>``) and MySQL (``JSON_CONTAINS``) compare keys literally by containment.
    The SQLite fallback builds a ``json_extract`` path from the key, where an unquoted ``.``
    or ``[`` is interpreted as path navigation instead — silently missing literal dotted keys
    and wrongly matching nested objects.
    """

    @pytest.fixture
    def _setup(self, session):
        self.create_assets(num=1, session=session)
        events = [
            AssetEvent(
                asset_id=1,
                extra={"spark.executor.memory": "4g"},
                source_task_id="t1",
                source_dag_id="d1",
                source_run_id="r1",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={"spark": {"executor": {"memory": "4g"}}},
                source_task_id="t1",
                source_dag_id="d1",
                source_run_id="r2",
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                asset_id=1,
                extra={"partitions[0]": "2024-01-01"},
                source_task_id="t1",
                source_dag_id="d1",
                source_run_id="r3",
                timestamp=DEFAULT_DATE,
            ),
        ]
        session.add_all(events)
        session.commit()

    @pytest.mark.usefixtures("_setup")
    @pytest.mark.parametrize(
        ("params", "expected_count"),
        [
            # Matches only the event whose extra has the literal dotted key,
            # not the one nesting the same path as objects.
            ({"extra": "spark.executor.memory=4g"}, 1),
            ({"extra": "partitions[0]=2024-01-01"}, 1),
            ({"extra": "spark.executor.memory=8g"}, 0),
        ],
    )
    def test_extra_filter_metacharacter_keys_match_literally(self, test_client, params, expected_count):
        response = test_client.get("/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_count

    @pytest.mark.usefixtures("_setup")
    def test_extra_filter_dotted_key_matches_the_literal_key_event(self, test_client):
        response = test_client.get("/assets/events", params={"extra": "spark.executor.memory=4g"})
        assert response.status_code == 200
        assert [e["source_run_id"] for e in response.json()["asset_events"]] == ["r1"]


class TestGetAssetEndpoint(TestAssets):
    @provide_session
    def test_should_respond_200(self, test_client, *, session):
        self.create_assets(num=1)
        assert session.scalars(select(func.count(AssetModel.id))).one() == 1
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        with assert_queries_count(6):
            response = test_client.get("/assets/1")
        assert response.status_code == 200
        assert response.json() == {
            "id": 1,
            "name": "simple1",
            "uri": "s3://bucket/key/1",
            "group": "asset",
            "extra": {"foo": "bar"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "scheduled_dags": [],
            "producing_tasks": [],
            "consuming_tasks": [],
            "aliases": [],
            "watchers": [],
            "last_asset_event": {"id": None, "timestamp": None},
        }

    @provide_session
    def test_should_respond_200_with_watchers(self, test_client, *, session):
        """Test that single asset endpoint returns watcher information."""
        assets = self.create_assets_with_watchers(num=1, session=session)
        asset = assets[0]

        response = test_client.get(f"/assets/{asset.id}")
        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)

        assert response_data == {
            "id": asset.id,
            "name": "watched1",
            "uri": "s3://watched/bucket/key/1",
            "group": "asset",
            "extra": {"foo": "bar"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "scheduled_dags": [],
            "producing_tasks": [],
            "consuming_tasks": [],
            "aliases": [],
            "watchers": [
                {
                    "name": "watcher_1",
                    "trigger_id": asset.watchers[0].trigger_id,
                    "created_date": tz_datetime_format,
                }
            ],
            "last_asset_event": {"id": None, "timestamp": None},
        }

    @conf_vars({("core", "multi_team"): "True"})
    def test_query_count_with_multi_team(self, test_client, session):
        """Resolving reference ``team_name`` must not add a query per referencing Dag.

        A missing loader option does not raise: :attr:`DagModel.team_name` falls back to the
        cached ``get_team_name`` resolver instead of tripping ``lazy="raise"``, so only a pinned
        count catches the regression.
        """
        asset = _create_assets_with_team_references(session, num=1, refs_per_asset=5)[0]

        with assert_queries_count(8):
            response = test_client.get(f"/assets/{asset.id}")

        assert response.status_code == 200
        body = response.json()
        assert {ref["team_name"] for ref in body["scheduled_dags"]} == {"team-assets"}
        assert {ref["team_name"] for ref in body["producing_tasks"]} == {"team-assets"}

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets/1")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets/1")
        assert response.status_code == 403

    def test_should_respond_404(self, test_client):
        response = test_client.get("/assets/1")
        assert response.status_code == 404
        assert response.json()["detail"] == "The Asset with ID: `1` was not found"

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        self.create_assets_with_sensitive_extra()
        tz_datetime_format = from_datetime_to_zulu_without_ms(DEFAULT_DATE)
        response = test_client.get("/assets/1")
        assert response.status_code == 200
        assert response.json() == {
            "id": 1,
            "name": "sensitive1",
            "uri": "s3://bucket/key/1",
            "group": "asset",
            "extra": {"password": "***"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "scheduled_dags": [],
            "producing_tasks": [],
            "consuming_tasks": [],
            "aliases": [],
            "watchers": [],
            "last_asset_event": {"id": None, "timestamp": None},
        }


class TestGetAssetAliasEndpoint(TestAssetAliases):
    @provide_session
    def test_should_respond_200(self, test_client, *, session):
        self.create_asset_aliases(num=1)
        assert session.scalars(select(func.count(AssetAliasModel.id))).one() == 1
        with assert_queries_count(6):
            response = test_client.get("/assets/aliases/1")
        assert response.status_code == 200
        assert response.json() == {"id": 1, "name": "simple1", "group": "alias"}

    def test_should_respond_404(self, test_client):
        response = test_client.get("/assets/aliases/1")
        assert response.status_code == 404
        assert response.json()["detail"] == "The Asset Alias with ID: `1` was not found"


class TestQueuedEventEndpoint(TestAssets):
    def _create_asset_dag_run_queues(self, dag_id, asset_id, session):
        session.execute(delete(AssetDagRunQueue))
        session.flush()
        event = AssetEvent(asset_id=asset_id, timestamp=timezone.utcnow())
        session.add(event)
        session.flush()
        adrq = AssetDagRunQueue(target_dag_id=dag_id, asset_id=asset_id, asset_event_id=event.id)
        session.add(adrq)
        session.commit()
        return adrq


class TestGetDagAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)
        self._create_asset_dag_run_queues(dag_id, asset.id, session)

        with assert_queries_count(4):
            response = test_client.get(
                f"/dags/{dag_id}/assets/queuedEvents",
            )

        assert response.status_code == 200
        assert response.json() == {
            "queued_events": [
                {
                    "asset_id": asset.id,
                    "dag_id": "dag",
                    "dag_display_name": "dag",
                    "created_at": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/random/assets/queuedEvents")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/random/assets/queuedEvents")
        assert response.status_code == 403

    def test_should_respond_200_empty(self, test_client):
        dag_id = "not_exists"

        response = test_client.get(
            f"/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 200
        assert response.json() == {"queued_events": [], "total_entries": 0}


class TestQueuedEventsDagAxisAuthorization:
    """The Dag axis of the queued-events routes must match what the route does to the Dag.

    Deleting queued events cancels a Dag's pending asset-triggered scheduling, which is a
    write to that Dag's scheduling state, so those routes require Dag edit. Reading them
    requires only Dag read.
    """

    @staticmethod
    def _dag_axis_methods(route) -> list[str]:
        """Return the ``method`` captured by each ``requires_access_dag`` on a route."""
        from airflow.api_fastapi.core_api.security import requires_access_dag

        module = requires_access_dag.__module__
        methods = []
        for dependency in route.dependant.dependencies:
            call = dependency.call
            # requires_access_dag returns a closure; the ResourceMethod it was built with
            # is captured in one of that closure's cells.
            if getattr(call, "__module__", None) != module or not call.__closure__:
                continue
            if call.__qualname__.split(".")[0] != "requires_access_dag":
                continue
            for cell in call.__closure__:
                value = cell.cell_contents
                if isinstance(value, str) and value in {"GET", "POST", "PUT", "DELETE", "MENU"}:
                    methods.append(value)
        return methods

    @pytest.fixture
    def routes_by_path(self, test_client):
        return {
            (r.path, tuple(sorted(r.methods))): r for r in test_client.app.routes if hasattr(r, "dependant")
        }

    @pytest.mark.parametrize(
        "path",
        [
            "/assets/{asset_id}/queuedEvents",
            "/dags/{dag_id}/assets/queuedEvents",
            "/dags/{dag_id}/assets/{asset_id}/queuedEvents",
        ],
    )
    def test_delete_queued_events_requires_dag_edit(self, routes_by_path, path):
        matches = [
            r for (p, methods), r in routes_by_path.items() if p.endswith(path) and "DELETE" in methods
        ]
        assert matches, f"no DELETE route registered for {path}"
        for route in matches:
            assert self._dag_axis_methods(route) == ["PUT"], (
                f"DELETE {path} must gate the Dag axis on edit, not read"
            )

    @pytest.mark.parametrize(
        "path",
        [
            "/dags/{dag_id}/assets/queuedEvents",
            "/dags/{dag_id}/assets/{asset_id}/queuedEvents",
        ],
    )
    def test_get_queued_events_requires_only_dag_read(self, routes_by_path, path):
        matches = [r for (p, methods), r in routes_by_path.items() if p.endswith(path) and "GET" in methods]
        assert matches, f"no GET route registered for {path}"
        for route in matches:
            assert self._dag_axis_methods(route) == ["GET"]


class TestDeleteDagDatasetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        asset_id = 1
        self._create_asset_dag_run_queues(dag_id, asset_id, session)
        adrqs = session.scalars(select(AssetDagRunQueue)).all()
        assert len(adrqs) == 1

        response = test_client.delete(
            f"/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 204
        adrqs = session.scalars(select(AssetDagRunQueue)).all()
        assert len(adrqs) == 0
        check_last_log(session, dag_id=dag_id, event="delete_dag_asset_queued_events", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete("/dags/random/assets/queuedEvents")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/random/assets/queuedEvents")
        assert response.status_code == 403

    def test_should_respond_404_invalid_dag(self, test_client):
        dag_id = "not_exists"

        response = test_client.delete(
            f"/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `not_exists` was not found"

    def test_should_respond_404_valid_dag_no_adrq(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        adrqs = session.scalars(select(AssetDagRunQueue)).all()
        assert len(adrqs) == 0

        response = test_client.delete(
            f"/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `dag` was not found"


class TestPostAssetEvents(TestAssets):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session):
        (asset,) = self.create_assets(num=1, session=session)
        event_payload = {"asset_id": asset.id, "extra": {"foo": "bar"}}
        response = test_client.post("/assets/events", json=event_payload)
        assert response.status_code == 200
        assert response.json() == {
            "id": mock.ANY,
            "asset_id": asset.id,
            "uri": "s3://bucket/key/1",
            "group": "asset",
            "name": "simple1",
            "extra": {"foo": "bar", "from_rest_api": True},
            "source_task_id": None,
            "source_dag_id": None,
            "source_run_id": None,
            "source_map_index": -1,
            "created_dagruns": [],
            "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
            "partition_key": None,
        }
        check_last_log(session, dag_id=None, event="create_asset_event", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/assets/events", json={"asset_uri": "s3://bucket/key/1"})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/assets/events", json={"asset_uri": "s3://bucket/key/1"})
        assert response.status_code == 403

    def test_invalid_attr_not_allowed(self, test_client, session):
        self.create_assets(session=session)
        event_invalid_payload = {"asset_uri": "s3://bucket/key/1", "extra": {"foo": "bar"}, "fake": {}}
        response = test_client.post("/assets/events", json=event_invalid_payload)

        assert response.status_code == 422

    @pytest.mark.parametrize(
        ("partition_key", "expected_status_code"),
        [
            pytest.param("", 422, id="empty"),
            pytest.param("   ", 422, id="whitespace_only"),
            pytest.param("a" * (ID_LEN + 1), 422, id="too_long"),
            pytest.param("2026-03-23", 200, id="valid"),
            pytest.param(None, 200, id="none"),
        ],
    )
    def test_partition_key_validation(self, test_client, session, partition_key, expected_status_code):
        (asset,) = self.create_assets(num=1, session=session)
        event_payload = {"asset_id": asset.id, "partition_key": partition_key}
        response = test_client.post("/assets/events", json=event_payload)
        assert response.status_code == expected_status_code

    def test_partition_key_preserves_surrounding_whitespace(self, test_client, session):
        (asset,) = self.create_assets(num=1, session=session)
        event_payload = {"asset_id": asset.id, "partition_key": "  2026-03-23  "}
        response = test_client.post("/assets/events", json=event_payload)
        assert response.status_code == 200
        assert response.json()["partition_key"] == "  2026-03-23  "

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        (asset,) = self.create_assets(num=1, session=session)
        event_payload = {"asset_id": asset.id, "extra": {"password": "bar"}}
        response = test_client.post("/assets/events", json=event_payload)
        assert response.status_code == 200
        assert response.json() == {
            "id": mock.ANY,
            "asset_id": asset.id,
            "uri": "s3://bucket/key/1",
            "group": "asset",
            "name": "simple1",
            "extra": {"password": "***", "from_rest_api": True},
            "source_task_id": None,
            "source_dag_id": None,
            "source_run_id": None,
            "source_map_index": -1,
            "created_dagruns": [],
            "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
            "partition_key": None,
        }

    def test_should_update_asset_endpoint(self, test_client, session):
        """Test for a single Asset."""
        (asset,) = self.create_assets(num=1, session=session)
        event_payload = {"asset_id": asset.id, "extra": {"foo": "bar"}}
        asset_event_response = test_client.post("/assets/events", json=event_payload)
        asset_response = test_client.get(f"/assets/{asset.id}")

        assert asset_response.json()["last_asset_event"]["id"] == asset_event_response.json()["id"]
        assert (
            asset_response.json()["last_asset_event"]["timestamp"] == asset_event_response.json()["timestamp"]
        )

    def test_should_update_assets_endpoint(self, test_client, session):
        """Test for multiple Assets."""
        asset1, asset2 = self.create_assets(num=2, session=session)

        # Now, only make a POST to the /assets/events endpoint for one of the Assets
        for _ in range(2):
            event_payload = {"asset_id": asset1.id, "extra": {"foo": "bar"}}
            asset_event_response = test_client.post("/assets/events", json=event_payload)

        assets_response = test_client.get("/assets")

        for asset in assets_response.json()["assets"]:
            # We should expect to see AssetEvents for the first Asset
            if asset["id"] == asset1.id:
                assert asset["last_asset_event"]["id"] == asset_event_response.json()["id"]
                assert asset["last_asset_event"]["timestamp"] == asset_event_response.json()["timestamp"]

            elif asset["id"] == asset2.id:
                assert asset["last_asset_event"]["id"] is None
                assert asset["last_asset_event"]["timestamp"] is None


class TestPostAssetEventsTeamResolution(TestAssets):
    """Tests for team-based filtering in create_asset_event."""

    def _make_mock_event(self, asset):
        m = mock.MagicMock(
            spec=AssetEvent,
            id=1,
            asset_id=asset.id,
            uri=asset.uri,
            group=asset.group,
            extra={"from_rest_api": True},
            source_map_index=-1,
            timestamp=DEFAULT_DATE,
            source_task_id=None,
            source_dag_id=None,
            source_run_id=None,
            partition_key=None,
            created_dagruns=[],
        )
        # MagicMock uses 'name' internally for repr, so it must be set separately.
        m.name = asset.name
        return m

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.parametrize(
        ("multi_team", "expected_teams"),
        [
            pytest.param("True", {"team_a", "team_b"}, id="enabled"),
            pytest.param("False", set(), id="disabled"),
        ],
    )
    @mock.patch("airflow.api_fastapi.core_api.routes.public.assets.asset_manager.register_asset_change")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.assets.get_auth_manager")
    def test_team_resolution(
        self, mock_get_auth_manager, mock_register, test_client, session, multi_team, expected_teams
    ):
        (asset,) = self.create_assets(num=1, session=session)
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = {"team_a", "team_b"}
        mock_register.return_value = self._make_mock_event(asset)

        with conf_vars({("core", "multi_team"): multi_team}):
            response = test_client.post("/assets/events", json={"asset_id": asset.id, "extra": {}})

        assert response.status_code == 200
        call_kwargs = mock_register.call_args.kwargs
        assert call_kwargs["source_is_api"] is True
        assert call_kwargs["api_user_teams"] == expected_teams

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.parametrize(
        ("multi_team", "access_control", "expected_consumer_teams", "expected_allow_global"),
        [
            pytest.param(
                "True",
                {"consumer_teams": ["team_ml", "team_data"], "allow_global": False},
                ["team_ml", "team_data"],
                False,
                id="multi_team_enabled_with_consumer_teams",
            ),
            pytest.param(
                "True",
                None,
                None,
                True,
                id="multi_team_enabled_no_access_control",
            ),
            pytest.param(
                "True",
                {"consumer_teams": []},
                [],
                True,
                id="multi_team_enabled_empty_consumer_teams",
            ),
            pytest.param(
                "False",
                {"consumer_teams": ["team_ml"], "allow_global": False},
                None,
                True,
                id="multi_team_disabled_access_control_ignored",
            ),
        ],
    )
    @mock.patch("airflow.api_fastapi.core_api.routes.public.assets.asset_manager.register_asset_change")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.assets.get_auth_manager")
    def test_access_control_consumer_teams(
        self,
        mock_get_auth_manager,
        mock_register,
        test_client,
        session,
        multi_team,
        access_control,
        expected_consumer_teams,
        expected_allow_global,
    ):
        (asset,) = self.create_assets(num=1, session=session)
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = {"team_a"}
        mock_register.return_value = self._make_mock_event(asset)

        payload = {"asset_id": asset.id, "extra": {}}
        if access_control is not None:
            payload["access_control"] = access_control

        with conf_vars({("core", "multi_team"): multi_team}):
            response = test_client.post("/assets/events", json=payload)

        assert response.status_code == 200
        call_kwargs = mock_register.call_args.kwargs
        assert call_kwargs["api_allow_consumer_teams"] == expected_consumer_teams
        assert call_kwargs["api_allow_global_consumers"] == expected_allow_global


@pytest.mark.need_serialized_dag
class TestPostAssetMaterialize(TestAssets):
    DAG_ASSET1_ID = "test_dag_1"
    DAG_ASSET2_ID_A = "test_dag_2a"
    DAG_ASSET2_ID_B = "test_dag_2b"
    DAG_ASSET_NO = "test_dag_no"

    @pytest.fixture(autouse=True)
    def create_dags(self, setup, dag_maker, session):
        # Depend on 'setup' so it runs first. Otherwise it deletes what we create here.
        assets = {
            i: am.to_serialized() for i, am in enumerate(self.create_assets(session=session, num=3), start=1)
        }
        # DAG_ASSET1_ID is materialized with a partition_key in several tests below, so it must be a
        # partitioned Dag. PartitionedAtRuntime accepts runtime-discovered partition keys without
        # requiring a partitioned timetable.
        with dag_maker(self.DAG_ASSET1_ID, schedule=PartitionedAtRuntime(), session=session):
            EmptyOperator(task_id="task", outlets=assets[1])
        with dag_maker(self.DAG_ASSET2_ID_A, schedule=None, session=session):
            EmptyOperator(task_id="task", outlets=assets[2])
        with dag_maker(self.DAG_ASSET2_ID_B, schedule=None, session=session):
            EmptyOperator(task_id="task", outlets=assets[2])
        with dag_maker(self.DAG_ASSET_NO, schedule=None, session=session):
            EmptyOperator(task_id="task")
        session.commit()

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    @mock.patch(
        "airflow.api_fastapi.auth.managers.simple.user.SimpleAuthManagerUser.get_display_name",
        return_value="Jane Doe",
    )
    def test_materialize_records_triggering_user_display_name(self, mock_display_name, test_client):
        response = test_client.post("/assets/1/materialize")
        assert response.status_code == 200
        assert response.json()["triggering_user_name"] == "Jane Doe"

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200(self, test_client):
        response = test_client.post("/assets/1/materialize")
        assert response.status_code == 200
        assert response.json() == {
            "bundle_version": None,
            "dag_display_name": self.DAG_ASSET1_ID,
            "dag_run_id": mock.ANY,
            "dag_id": self.DAG_ASSET1_ID,
            "dag_versions": mock.ANY,
            "logical_date": None,
            "partition_key": None,
            "partition_date": None,
            "queued_at": mock.ANY,
            "run_after": mock.ANY,
            "start_date": None,
            "end_date": None,
            "duration": None,
            "data_interval_start": None,
            "data_interval_end": None,
            "last_scheduling_decision": None,
            "run_type": "asset_materialization",
            "state": "queued",
            "triggered_by": "rest_api",
            "triggering_user_name": "test",
            "conf": {},
            "note": None,
            "team_name": None,
        }

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_with_partition_key(self, test_client):
        partition_key = "2026-03-23"
        response = test_client.post("/assets/1/materialize", json={"partition_key": partition_key})
        assert response.status_code == 200
        assert response.json()["partition_key"] == partition_key

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_with_trigger_fields(self, test_client):
        payload = {
            "conf": {"foo": "bar"},
            "dag_run_id": "asset_materialization_run_1",
            "data_interval_end": "2026-03-24T00:00:00Z",
            "data_interval_start": "2026-03-23T00:00:00Z",
            "logical_date": "2026-03-23T00:00:00Z",
            "note": "created from asset page",
            "partition_key": "2026-03-23",
        }
        response = test_client.post("/assets/1/materialize", json=payload)

        assert response.status_code == 200
        assert response.json()["conf"] == {"foo": "bar"}
        assert response.json()["dag_run_id"] == "asset_materialization_run_1"
        assert response.json()["data_interval_start"] == "2026-03-23T00:00:00Z"
        assert response.json()["data_interval_end"] == "2026-03-24T00:00:00Z"
        assert response.json()["logical_date"] == "2026-03-23T00:00:00Z"
        assert response.json()["note"] == "created from asset page"
        assert response.json()["partition_key"] == "2026-03-23"
        assert response.json()["run_type"] == "asset_materialization"

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_with_trigger_fields_without_dag_run_id(self, test_client):
        payload = {
            "conf": {"foo": "bar"},
            # "dag_run_id": "asset_materialization_run_1",
            "data_interval_end": "2026-03-24T00:00:00Z",
            "data_interval_start": "2026-03-23T00:00:00Z",
            "logical_date": "2026-03-23T00:00:00Z",
            "note": "created from asset page",
            "partition_key": "2026-03-23",
        }
        response = test_client.post("/assets/1/materialize", json=payload)

        assert response.status_code == 200
        assert response.json()["conf"] == {"foo": "bar"}
        assert response.json()["dag_run_id"].startswith("asset_materialization__")
        assert response.json()["data_interval_start"] == "2026-03-23T00:00:00Z"
        assert response.json()["data_interval_end"] == "2026-03-24T00:00:00Z"
        assert response.json()["logical_date"] == "2026-03-23T00:00:00Z"
        assert response.json()["note"] == "created from asset page"
        assert response.json()["partition_key"] == "2026-03-23"
        assert response.json()["run_type"] == "asset_materialization"

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/assets/2/materialize")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/assets/2/materialize")
        assert response.status_code == 403

    def test_should_respond_409_on_multiple_dags(self, test_client):
        response = test_client.post("/assets/2/materialize")
        assert response.status_code == 409
        assert response.json()["detail"] == "More than one Dag materializes asset with ID: 2"

    def test_should_respond_409_for_draining_dag(self, test_client, session):
        dag_model = session.get(DagModel, self.DAG_ASSET1_ID)
        dag_model.is_draining = True
        session.commit()

        response = test_client.post("/assets/1/materialize")

        assert response.status_code == 409
        assert response.json()["detail"] == (
            f"Dag with dag_id: '{self.DAG_ASSET1_ID}' is draining and does not accept new runs"
        )

    def test_should_respond_404_on_multiple_dags(self, test_client):
        response = test_client.post("/assets/3/materialize")
        assert response.status_code == 404
        assert response.json()["detail"] == "No Dag materializes asset with ID: 3"

    def test_should_respond_400_if_materialization_runs_denied(self, test_client, session):
        sdm = session.scalar(
            select(SerializedDagModel).where(SerializedDagModel.dag_id == self.DAG_ASSET1_ID)
        )
        data = sdm.data
        data["dag"]["allowed_run_types"] = [DagRunType.SCHEDULED.value]
        session.execute(
            update(SerializedDagModel)
            .where(SerializedDagModel.dag_id == self.DAG_ASSET1_ID)
            .values(_data=data)
        )
        session.commit()
        response = test_client.post("/assets/1/materialize")
        assert response.status_code == 400
        assert (
            response.json()["detail"]
            == f"Dag with dag_id: '{self.DAG_ASSET1_ID}' does not allow asset materialization runs"
        )

    def test_materialize_allowed_run_types_from_requested_version(self, test_client, session, dag_maker):
        """Asset materialization allowed_run_types is enforced from the requested bundle version, not latest."""
        bundle_name = "allowed_run_types_bundle"
        asset = session.get(AssetModel, 1).to_serialized()

        with dag_maker(
            self.DAG_ASSET1_ID,
            bundle_name=bundle_name,
            bundle_version="v1",
            schedule=None,
            session=session,
        ):
            EmptyOperator(task_id="task_v1", outlets=asset)

        with dag_maker(
            self.DAG_ASSET1_ID,
            bundle_name=bundle_name,
            bundle_version="v2",
            schedule="@daily",
            allowed_run_types=[DagRunType.SCHEDULED],
            session=session,
        ):
            EmptyOperator(task_id="task_v2", outlets=asset)

        # v1 allows materialization; latest v2 does not. Requesting v1 must succeed.
        response = test_client.post("/assets/1/materialize", json={"bundle_version": "v1"})
        assert response.status_code == 200
        assert response.json()["bundle_version"] == "v1"

        # Without bundle_version the latest (v2) governs and rejects the run.
        response = test_client.post("/assets/1/materialize")
        assert response.status_code == 400
        assert (
            response.json()["detail"]
            == f"Dag with dag_id: '{self.DAG_ASSET1_ID}' does not allow asset materialization runs"
        )

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_403_when_user_cannot_trigger_dag(self, test_client):
        with mock.patch(
            "airflow.api_fastapi.core_api.routes.public.assets.get_auth_manager",
            autospec=True,
        ) as mock_get_auth_manager:
            mock_get_auth_manager.return_value.is_authorized_dag.return_value = False

            response = test_client.post("/assets/1/materialize")

            assert response.status_code == 403
            assert response.json()["detail"] == (
                f"User is not authorized to trigger a run for Dag: {self.DAG_ASSET1_ID} that materializes this asset"
            )
            mock_get_auth_manager.return_value.is_authorized_dag.assert_called_once_with(
                method="POST",
                access_entity=DagAccessEntity.RUN,
                details=DagDetails(id=self.DAG_ASSET1_ID),
                user=mock.ANY,
            )

    def test_should_respond_with_bundle_version(self, test_client, session, dag_maker):
        """Test that asset materialization respects bundle_version parameter."""
        bundle_name = "testing_bundle"
        asset = session.get(AssetModel, 1).to_serialized()

        with dag_maker(
            self.DAG_ASSET1_ID,
            bundle_name=bundle_name,
            bundle_version="v1",
            schedule=None,
            session=session,
        ):
            EmptyOperator(task_id="task_v1", outlets=asset)

        with dag_maker(
            self.DAG_ASSET1_ID,
            bundle_name=bundle_name,
            bundle_version="v2",
            schedule=None,
            session=session,
        ):
            EmptyOperator(task_id="task_v2", outlets=asset)

        response = test_client.post("/assets/1/materialize", json={"bundle_version": "v1"})
        assert response.status_code == 200
        assert response.json()["bundle_version"] == "v1"

        response = test_client.post("/assets/1/materialize", json={"bundle_version": "invalid_version"})
        assert response.status_code == 404
        assert (
            f"DAG with dag_id: '{self.DAG_ASSET1_ID}' does not have a version for bundle_version 'invalid_version'"
            in response.json()["detail"]
        )

        with dag_maker(
            self.DAG_ASSET1_ID,
            bundle_name=bundle_name,
            bundle_version="v3",
            schedule=None,
            session=session,
        ):
            EmptyOperator(task_id="task_v3", outlets=asset)
            dag_maker.dag.disable_bundle_versioning = True

        response = test_client.post("/assets/1/materialize", json={"bundle_version": "v1"})
        assert response.status_code == 400
        assert (
            f"DAG with dag_id: '{self.DAG_ASSET1_ID}' does not support bundle versioning"
            in response.json()["detail"]
        )

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_400_on_invalid_dag_run_id(self, test_client):
        """A dag_run_id containing '..' triggers ValueError in DagRun.validate_run_id.

        It must surface as 400 BAD_REQUEST, not 500 INTERNAL_SERVER_ERROR.
        """
        response = test_client.post(
            "/assets/1/materialize",
            json={"dag_run_id": "bad..id"},
        )
        assert response.status_code == 400
        assert "must not contain '..'" in response.json()["detail"]

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_with_partition_date_for_partitioned_dag(
        self, test_client, dag_maker, session
    ):
        """Materializing a Dag with a real partitioned timetable must populate partition_date.

        Regression guard: before this fix, `partition_date` resolved by `validate_context` was
        dropped when creating the run, unlike the sibling `/dags/{dag_id}/dagRuns` trigger route.
        """
        partitioned_dag_id = "test_materialize_populates_partition_date"
        asset = Asset(name="materialize_partition_date_asset", uri="s3://bucket/materialize-partition-date")
        with dag_maker(
            dag_id=partitioned_dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=DEFAULT_DATE,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task", outlets=[asset])
        session.commit()

        asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset.uri))

        response = test_client.post(
            f"/assets/{asset_id}/materialize",
            json={"partition_key": "2025-06-01T00:00:00"},
        )
        assert response.status_code == 200

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == partitioned_dag_id))
        assert dag_run is not None
        assert dag_run.partition_key == "2025-06-01T00:00:00"
        assert dag_run.partition_date == timezone.datetime(2025, 6, 1)

    @pytest.mark.parametrize("team_name", ["team_b", None])
    def test_authorizes_against_the_dags_team(self, test_client, session, team_name):
        """The Dag is resolved from the asset, so its team must be resolved and passed too — see the
        call site's comment for why an unresolved team asks about the wrong resource."""
        recorded = []

        auth_manager = mock.Mock(spec=BaseAuthManager)
        auth_manager.is_authorized_dag.side_effect = lambda **kw: recorded.append(kw) or True

        with (
            mock.patch(
                "airflow.api_fastapi.core_api.routes.public.assets.get_auth_manager",
                return_value=auth_manager,
            ),
            mock.patch.object(
                DagModel, "get_team_name", return_value=team_name, autospec=True
            ) as mock_get_team_name,
        ):
            test_client.post("/assets/1/materialize")

        assert len(recorded) == 1, "expected exactly one authorization check"
        details = recorded[0]["details"]
        assert details.id == self.DAG_ASSET1_ID
        assert details.team_name == team_name
        # resolved for the Dag the asset led to, not for some other Dag
        mock_get_team_name.assert_called_once_with(self.DAG_ASSET1_ID, session=mock.ANY)


class TestGetAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)
        self._create_asset_dag_run_queues(dag_id, asset.id, session)

        with assert_queries_count(3):
            response = test_client.get(f"/assets/{asset.id}/queuedEvents")

        assert response.status_code == 200
        assert response.json() == {
            "queued_events": [
                {
                    "asset_id": asset.id,
                    "dag_id": "dag",
                    "dag_display_name": "dag",
                    "created_at": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets/1/queuedEvents")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets/1/queuedEvents")
        assert response.status_code == 403

    def test_should_respond_200_empty(self, test_client):
        response = test_client.get("/assets/1/queuedEvents")
        assert response.status_code == 200
        assert response.json() == {"queued_events": [], "total_entries": 0}


class TestDeleteAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)
        self._create_asset_dag_run_queues(dag_id, asset.id, session)

        assert session.scalars(select(AssetDagRunQueue)).all()
        response = test_client.delete(f"/assets/{asset.id}/queuedEvents")
        assert response.status_code == 204
        assert session.scalars(select(AssetDagRunQueue)).all() == []
        check_last_log(session, dag_id=None, event="delete_asset_queued_events", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete("/assets/1/queuedEvents")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete("/assets/1/queuedEvents")
        assert response.status_code == 403

    def test_should_respond_404(self, test_client):
        response = test_client.delete("/assets/1/queuedEvents")
        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with asset_id: `1` was not found"

    def test_delete_does_not_read_back_deleted_row_keys(self, test_client, session, create_dummy_dag):
        from sqlalchemy import event

        import airflow.settings

        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)
        self._create_asset_dag_run_queues(dag_id, asset.id, session)

        executed_statements: list[str] = []

        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_statements.append(" ".join(statement.split()).upper())

        event.listen(airflow.settings.engine, "before_cursor_execute", capture)
        try:
            response = test_client.delete(f"/assets/{asset.id}/queuedEvents")
        finally:
            event.remove(airflow.settings.engine, "before_cursor_execute", capture)

        assert response.status_code == 204
        deletes = [s for s in executed_statements if s.startswith("DELETE")]
        assert deletes, "Expected the endpoint to issue a DELETE statement"
        assert [s for s in deletes if "RETURNING" in s] == [], "DELETE must not read back deleted keys"
        after_first_delete = executed_statements[executed_statements.index(deletes[0]) :]
        assert [s for s in after_first_delete if s.startswith("SELECT")] == [], (
            "No SELECT may precede a DELETE to collect the keys it is about to remove"
        )


class TestDeleteDagAssetQueuedEvent(TestQueuedEventEndpoint):
    def test_delete_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)

        self._create_asset_dag_run_queues(dag_id, asset.id, session)
        adrq = session.scalars(select(AssetDagRunQueue)).all()
        assert len(adrq) == 1

        response = test_client.delete(
            f"/dags/{dag_id}/assets/{asset.id}/queuedEvents",
        )

        assert response.status_code == 204
        adrq = session.scalars(select(AssetDagRunQueue)).all()
        assert len(adrq) == 0
        check_last_log(session, dag_id=dag_id, event="delete_dag_asset_queued_event", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete("/dags/random/assets/random/queuedEvents")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete("/dags/random/assets/random/queuedEvents")
        assert response.status_code == 403

    def test_delete_does_not_read_back_deleted_row_keys(self, test_client, session, create_dummy_dag):
        from sqlalchemy import event

        import airflow.settings

        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)
        self._create_asset_dag_run_queues(dag_id, asset.id, session)

        executed_statements: list[str] = []

        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_statements.append(" ".join(statement.split()).upper())

        event.listen(airflow.settings.engine, "before_cursor_execute", capture)
        try:
            response = test_client.delete(f"/dags/{dag_id}/assets/{asset.id}/queuedEvents")
        finally:
            event.remove(airflow.settings.engine, "before_cursor_execute", capture)

        assert response.status_code == 204
        deletes = [s for s in executed_statements if s.startswith("DELETE")]
        assert deletes, "Expected the endpoint to issue a DELETE statement"
        assert [s for s in deletes if "RETURNING" in s] == [], "DELETE must not read back deleted keys"
        after_first_delete = executed_statements[executed_statements.index(deletes[0]) :]
        assert [s for s in after_first_delete if s.startswith("SELECT")] == [], (
            "No SELECT may precede a DELETE to collect the keys it is about to remove"
        )

    def test_should_respond_404(self, test_client):
        dag_id = "not_exists"
        asset_id = 1

        response = test_client.delete(
            f"/dags/{dag_id}/assets/{asset_id}/queuedEvents",
        )

        assert response.status_code == 404
        assert (
            response.json()["detail"]
            == "Queued event with dag_id: `not_exists` and asset_id: `1` was not found"
        )
