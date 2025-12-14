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
from datetime import datetime, timedelta
from unittest import mock

import pytest
import time_machine
from sqlalchemy import delete, func, select

from airflow._shared.timezones import timezone
from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms
from tests_common.test_utils.logs import check_last_log

DEFAULT_DATE = datetime(2020, 6, 11, 18, 0, 0, tzinfo=timezone.utc)

pytestmark = pytest.mark.db_test


def _create_assets(session, num: int = 2) -> list[AssetModel]:
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


def _create_assets_events(session, num: int = 2, varying_timestamps=False) -> None:
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
    session.add(asset_event)
    session.commit()


def _create_dag_run(session, num: int = 2):
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
        clear_db_dag_bundles()
        clear_db_logs()

        yield

        clear_db_assets()
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_logs()

    @provide_session
    def create_assets(self, session, num: int = 2) -> list[AssetModel]:
        return _create_assets(session=session, num=num)

    @provide_session
    def create_assets_with_sensitive_extra(self, session, num: int = 2):
        _create_assets_with_sensitive_extra(session=session, num=num)

    @provide_session
    def create_provided_asset(self, session, asset: AssetModel):
        _create_provided_asset(session=session, asset=asset)

    @provide_session
    def create_assets_events(self, session, num: int = 2, varying_timestamps: bool = False):
        _create_assets_events(session=session, num=num, varying_timestamps=varying_timestamps)

    @provide_session
    def create_assets_events_with_sensitive_extra(self, session, num: int = 2):
        _create_assets_events_with_sensitive_extra(session=session, num=num)

    @provide_session
    def create_provided_asset_event(self, session, asset_event: AssetEvent):
        _create_provided_asset_event(session=session, asset_event=asset_event)

    @provide_session
    def create_dag_run(self, session, num: int = 2):
        _create_dag_run(num=num, session=session)

    @provide_session
    def create_asset_dag_run(self, session, num: int = 2):
        _create_asset_dag_run(num=num, session=session)


class TestGetAssets(TestAssets):
    def test_should_respond_200(self, test_client, session):
        assets1, asset2 = self.create_assets(session)
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
                    "last_asset_event": {"id": None, "timestamp": None},
                },
            ],
            "total_entries": 2,
        }

    def test_should_show_inactive(self, test_client, session):
        asset1, asset2 = self.create_assets(session)
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

    @pytest.mark.parametrize(
        "params, expected_assets",
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
        ],
    )
    @provide_session
    def test_filter_assets_by_name_pattern_works(self, test_client, params, expected_assets, session):
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
        "params, expected_assets",
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
        ],
    )
    @provide_session
    def test_filter_assets_by_uri_pattern_works(self, test_client, params, expected_assets, session):
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

    @pytest.mark.parametrize("dag_ids, expected_num", [("dag1,dag2", 2), ("dag3", 1), ("dag2,dag3", 2)])
    @provide_session
    def test_filter_assets_by_dag_ids_works(
        self, test_client, dag_ids, expected_num, testing_dag_bundle, session
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
        "dag_ids, uri_pattern,expected_num",
        [("dag1,dag2", "folder", 1), ("dag3", "nothing", 0), ("dag2,dag3", "key", 2)],
    )
    @provide_session
    def test_filter_assets_by_dag_ids_and_uri_pattern_works(
        self, test_client, dag_ids, uri_pattern, expected_num, testing_dag_bundle, session
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
        "url, expected_asset_uris",
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
        clear_db_dag_bundles()

    def teardown_method(self) -> None:
        clear_db_assets()
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()

    @provide_session
    def create_asset_aliases(self, num: int = 2, *, session):
        _create_asset_aliases(num=num, session=session)

    @provide_session
    def create_provided_asset_alias(self, asset_alias: AssetAliasModel, session):
        _create_provided_asset_alias(session=session, asset_alias=asset_alias)


class TestGetAssetAliases(TestAssetAliases):
    def test_should_respond_200(self, test_client, session):
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
        "params, expected_asset_aliases",
        [
            ({"name_pattern": "foo"}, {"foo1"}),
            ({"name_pattern": "1"}, {"foo1", "bar12"}),
            ({"uri_pattern": ""}, {"foo1", "bar12", "bar2", "bar3", "rex23"}),
        ],
    )
    @provide_session
    def test_filter_assets_by_name_pattern_works(self, test_client, params, expected_asset_aliases, session):
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
        "url, expected_asset_aliases",
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


class TestGetAssetEvents(TestAssets):
    def test_should_respond_200(self, test_client, session):
        asset1, asset2 = self.create_assets(session)
        self.create_assets_events(session)
        self.create_dag_run(session)
        self.create_asset_dag_run(session)
        assets = session.scalars(select(AssetEvent)).all()
        session.commit()
        assert len(assets) == 2

        with assert_queries_count(3):
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
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
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
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
            ],
            "total_entries": 2,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/assets/events")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/assets/events")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "params, total_entries",
        [
            ({"asset_id": "2"}, 1),
            ({"source_dag_id": "source_dag_id"}, 2),
            ({"source_task_id": "source_task_id"}, 2),
            ({"source_run_id": "source_run_id_1"}, 1),
            ({"source_map_index": "-1"}, 2),
        ],
    )
    @provide_session
    def test_filtering(self, test_client, params, total_entries, session):
        self.create_assets()
        self.create_assets_events()
        self.create_dag_run()
        self.create_asset_dag_run()
        response = test_client.get("/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == total_entries

    @pytest.mark.parametrize(
        "params, expected_ids",
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
        "params, expected_asset_ids",
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
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
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
                        }
                    ],
                    "timestamp": from_datetime_to_zulu_without_ms(DEFAULT_DATE),
                },
            ],
            "total_entries": 2,
        }


class TestGetAssetEndpoint(TestAssets):
    @provide_session
    def test_should_respond_200(self, test_client, session):
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
            "last_asset_event": {"id": None, "timestamp": None},
        }

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
            "last_asset_event": {"id": None, "timestamp": None},
        }


class TestGetAssetAliasEndpoint(TestAssetAliases):
    @provide_session
    def test_should_respond_200(self, test_client, session):
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
        adrq = AssetDagRunQueue(target_dag_id=dag_id, asset_id=asset_id)
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

    def test_should_respond_404(self, test_client):
        dag_id = "not_exists"

        response = test_client.get(
            f"/dags/{dag_id}/assets/queuedEvents",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `not_exists` was not found"


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
        (asset,) = self.create_assets(session, num=1)
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
        }
        check_last_log(session, dag_id=None, event="create_asset_event", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/assets/events", json={"asset_uri": "s3://bucket/key/1"})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/assets/events", json={"asset_uri": "s3://bucket/key/1"})
        assert response.status_code == 403

    def test_invalid_attr_not_allowed(self, test_client, session):
        self.create_assets(session)
        event_invalid_payload = {"asset_uri": "s3://bucket/key/1", "extra": {"foo": "bar"}, "fake": {}}
        response = test_client.post("/assets/events", json=event_invalid_payload)

        assert response.status_code == 422

    @pytest.mark.usefixtures("time_freezer")
    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra(self, test_client, session):
        (asset,) = self.create_assets(session, num=1)
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
        }

    def test_should_update_asset_endpoint(self, test_client, session):
        """Test for a single Asset."""
        (asset,) = self.create_assets(session, num=1)
        event_payload = {"asset_id": asset.id, "extra": {"foo": "bar"}}
        asset_event_response = test_client.post("/assets/events", json=event_payload)
        asset_response = test_client.get(f"/assets/{asset.id}")

        assert asset_response.json()["last_asset_event"]["id"] == asset_event_response.json()["id"]
        assert (
            asset_response.json()["last_asset_event"]["timestamp"] == asset_event_response.json()["timestamp"]
        )

    def test_should_update_assets_endpoint(self, test_client, session):
        """Test for multiple Assets."""
        asset1, asset2 = self.create_assets(session, num=2)

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
            i: am.to_public() for i, am in enumerate(self.create_assets(session=session, num=3), start=1)
        }
        with dag_maker(self.DAG_ASSET1_ID, schedule=None, session=session):
            EmptyOperator(task_id="task", outlets=assets[1])
        with dag_maker(self.DAG_ASSET2_ID_A, schedule=None, session=session):
            EmptyOperator(task_id="task", outlets=assets[2])
        with dag_maker(self.DAG_ASSET2_ID_B, schedule=None, session=session):
            EmptyOperator(task_id="task", outlets=assets[2])
        with dag_maker(self.DAG_ASSET_NO, schedule=None, session=session):
            EmptyOperator(task_id="task")
        session.commit()

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
            "queued_at": mock.ANY,
            "run_after": mock.ANY,
            "start_date": None,
            "end_date": None,
            "duration": None,
            "data_interval_start": None,
            "data_interval_end": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "state": "queued",
            "triggered_by": "rest_api",
            "triggering_user_name": "test",
            "conf": {},
            "note": None,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/assets/2/materialize")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/assets/2/materialize")
        assert response.status_code == 403

    def test_should_respond_409_on_multiple_dags(self, test_client):
        response = test_client.post("/assets/2/materialize")
        assert response.status_code == 409
        assert response.json()["detail"] == "More than one DAG materializes asset with ID: 2"

    def test_should_respond_404_on_multiple_dags(self, test_client):
        response = test_client.post("/assets/3/materialize")
        assert response.status_code == 404
        assert response.json()["detail"] == "No DAG materializes asset with ID: 3"


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

    def test_should_respond_404(self, test_client):
        response = test_client.get("/assets/1/queuedEvents")
        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with asset_id: `1` was not found"


class TestDeleteAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        (asset,) = self.create_assets(session=session, num=1)
        self._create_asset_dag_run_queues(dag_id, asset.id, session)

        assert session.get(AssetDagRunQueue, (asset.id, dag_id)) is not None
        response = test_client.delete(f"/assets/{asset.id}/queuedEvents")
        assert response.status_code == 204
        assert session.get(AssetDagRunQueue, (asset.id, dag_id)) is None
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
