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

import urllib
from typing import Generator

import pytest
import time_machine

from airflow.models import DagModel
from airflow.models.asset import (
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


def _create_assets(session, num: int = 2) -> None:
    default_time = "2020-06-11T18:00:00+00:00"
    assets = [
        AssetModel(
            id=i,
            uri=f"s3://bucket/key/{i}",
            extra={"foo": "bar"},
            created_at=timezone.parse(default_time),
            updated_at=timezone.parse(default_time),
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets)
    session.commit()


def _create_provided_asset(session, asset: AssetModel) -> None:
    session.add(asset)
    session.commit()


def _create_assets_events(session, num: int = 2) -> None:
    default_time = "2020-06-11T18:00:00+00:00"
    assets_events = [
        AssetEvent(
            id=i,
            asset_id=i,
            extra={"foo": "bar"},
            source_task_id="source_task_id",
            source_dag_id="source_dag_id",
            source_run_id=f"source_run_id_{i}",
            timestamp=timezone.parse(default_time),
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(assets_events)
    session.commit()


def _create_provided_asset_event(session, asset_event: AssetEvent) -> None:
    session.add(asset_event)
    session.commit()


def _create_dag_run(session, num: int = 2):
    default_time = "2020-06-11T18:00:00+00:00"
    dag_runs = [
        DagRun(
            dag_id="source_dag_id",
            run_id=f"source_run_id_{i}",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(default_time),
            start_date=timezone.parse(default_time),
            data_interval=(timezone.parse(default_time), timezone.parse(default_time)),
            external_trigger=True,
            state=DagRunState.SUCCESS,
        )
        for i in range(1, 1 + num)
    ]
    for dag_run in dag_runs:
        dag_run.end_date = timezone.parse(default_time)
    session.add_all(dag_runs)
    session.commit()


def _create_asset_dag_run(session, num: int = 2):
    for i in range(1, 1 + num):
        dag_run = session.query(DagRun).filter_by(run_id=f"source_run_id_{i}").first()
        asset_event = session.query(AssetEvent).filter_by(id=i).first()
        if dag_run and asset_event:
            dag_run.consumed_asset_events.append(asset_event)
    session.commit()


class TestAssets:
    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self) -> None:
        clear_db_assets()
        clear_db_runs()

    @provide_session
    def create_assets(self, session, num: int = 2):
        _create_assets(session=session, num=num)

    @provide_session
    def create_provided_asset(self, session, asset: AssetModel):
        _create_provided_asset(session=session, asset=asset)

    @provide_session
    def create_assets_events(self, session, num: int = 2):
        _create_assets_events(session=session, num=num)

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
        self.create_assets()
        assets = session.query(AssetModel).all()
        assert len(assets) == 2

        response = test_client.get("/public/assets")
        assert response.status_code == 200
        response_data = response.json()
        tz_datetime_format = self.default_time.replace("+00:00", "Z")
        assert response_data == {
            "assets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"foo": "bar"},
                    "created_at": tz_datetime_format,
                    "updated_at": tz_datetime_format,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/public/assets?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

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

        response = test_client.get("/public/assets", params=params)
        assert response.status_code == 200
        asset_urls = {asset["uri"] for asset in response.json()["assets"]}
        assert expected_assets == asset_urls

    @pytest.mark.parametrize("dag_ids, expected_num", [("dag1,dag2", 2), ("dag3", 1), ("dag2,dag3", 2)])
    @provide_session
    def test_filter_assets_by_dag_ids_works(self, test_client, dag_ids, expected_num, session):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", asset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", asset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = test_client.get(
            f"/public/assets?dag_ids={dag_ids}",
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
        self, test_client, dag_ids, uri_pattern, expected_num, session
    ):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://asset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", asset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", asset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", asset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = test_client.get(
            f"/public/assets?dag_ids={dag_ids}&uri_pattern={uri_pattern}",
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data["assets"]) == expected_num


class TestGetAssetsEndpointPagination(TestAssets):
    @pytest.mark.parametrize(
        "url, expected_asset_uris",
        [
            # Limit test data
            ("/public/assets?limit=1", ["s3://bucket/key/1"]),
            ("/public/assets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/public/assets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ("/public/assets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/public/assets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
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

        response = test_client.get("/public/assets")

        assert response.status_code == 200
        assert len(response.json()["assets"]) == 100


class TestGetAssetEvents(TestAssets):
    def test_should_respond_200(self, test_client, session):
        self.create_assets()
        self.create_assets_events()
        self.create_dag_run()
        self.create_asset_dag_run()
        assets = session.query(AssetEvent).all()
        assert len(assets) == 2
        response = test_client.get("/public/assets/events")
        assert response.status_code == 200
        response_data = response.json()
        assert response_data == {
            "asset_events": [
                {
                    "id": 1,
                    "asset_id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_1",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_1",
                            "dag_id": "source_dag_id",
                            "logical_date": "2020-06-11T18:00:00Z",
                            "start_date": "2020-06-11T18:00:00Z",
                            "end_date": "2020-06-11T18:00:00Z",
                            "state": "success",
                            "data_interval_start": "2020-06-11T18:00:00Z",
                            "data_interval_end": "2020-06-11T18:00:00Z",
                        }
                    ],
                    "timestamp": "2020-06-11T18:00:00Z",
                },
                {
                    "id": 2,
                    "asset_id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"foo": "bar"},
                    "source_task_id": "source_task_id",
                    "source_dag_id": "source_dag_id",
                    "source_run_id": "source_run_id_2",
                    "source_map_index": -1,
                    "created_dagruns": [
                        {
                            "run_id": "source_run_id_2",
                            "dag_id": "source_dag_id",
                            "logical_date": "2020-06-11T18:00:00Z",
                            "start_date": "2020-06-11T18:00:00Z",
                            "end_date": "2020-06-11T18:00:00Z",
                            "state": "success",
                            "data_interval_start": "2020-06-11T18:00:00Z",
                            "data_interval_end": "2020-06-11T18:00:00Z",
                        }
                    ],
                    "timestamp": "2020-06-11T18:00:00Z",
                },
            ],
            "total_entries": 2,
        }

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
        response = test_client.get("/public/assets/events", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == total_entries

    def test_order_by_raises_400_for_invalid_attr(self, test_client, session):
        response = test_client.get("/public/assets/events?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json()["detail"] == msg

    @pytest.mark.parametrize(
        "params, expected_asset_uris",
        [
            # Limit test data
            ({"limit": "1"}, ["s3://bucket/key/1"]),
            ({"limit": "100"}, [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ({"offset": "1"}, [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ({"offset": "3"}, [f"s3://bucket/key/{i}" for i in range(4, 104)]),
        ],
    )
    def test_limit_and_offset(self, test_client, params, expected_asset_uris):
        self.create_assets(num=110)
        self.create_assets_events(num=110)
        self.create_dag_run(num=110)
        self.create_asset_dag_run(num=110)

        response = test_client.get("/public/assets/events", params=params)

        assert response.status_code == 200
        asset_uris = [asset["uri"] for asset in response.json()["asset_events"]]
        assert asset_uris == expected_asset_uris


class TestGetAssetEndpoint(TestAssets):
    @pytest.mark.parametrize(
        "url",
        [
            urllib.parse.quote(
                "s3://bucket/key/1", safe=""
            ),  # api should cover raw as well as unquoted case like legacy
            "s3://bucket/key/1",
        ],
    )
    @provide_session
    def test_should_respond_200(self, test_client, url, session):
        self.create_assets(num=1)
        assert session.query(AssetModel).count() == 1
        tz_datetime_format = self.default_time.replace("+00:00", "Z")
        with assert_queries_count(6):
            response = test_client.get(
                f"/public/assets/{url}",
            )
        assert response.status_code == 200
        assert response.json() == {
            "id": 1,
            "uri": "s3://bucket/key/1",
            "extra": {"foo": "bar"},
            "created_at": tz_datetime_format,
            "updated_at": tz_datetime_format,
            "consuming_dags": [],
            "producing_tasks": [],
            "aliases": [],
        }

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            f"/public/assets/{urllib.parse.quote('s3://bucket/key', safe='')}",
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "The Asset with uri: `s3://bucket/key` was not found"


class TestQueuedEventEndpoint(TestAssets):
    @pytest.fixture
    def time_freezer(self) -> Generator:
        freezer = time_machine.travel(self.default_time, tick=False)
        freezer.start()

        yield

        freezer.stop()

    def _create_asset_dag_run_queues(self, dag_id, asset_id, session):
        adrq = AssetDagRunQueue(target_dag_id=dag_id, asset_id=asset_id)
        session.add(adrq)
        session.commit()
        return adrq


class TestGetDagAssetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        self.create_assets(session=session, num=1)
        asset_id = 1
        self._create_asset_dag_run_queues(dag_id, asset_id, session)

        response = test_client.get(
            f"/public/dags/{dag_id}/assets/queuedEvent",
        )

        assert response.status_code == 200
        assert response.json() == {
            "queued_events": [
                {
                    "created_at": self.default_time.replace("+00:00", "Z"),
                    "uri": "s3://bucket/key/1",
                    "dag_id": "dag",
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_404(self, test_client):
        dag_id = "not_exists"

        response = test_client.get(
            f"/public/dags/{dag_id}/assets/queuedEvent",
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Queue event with dag_id: `not_exists` was not found"


class TestDeleteDagAssetQueuedEvent(TestQueuedEventEndpoint):
    def test_delete_should_respond_204(self, test_client, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        asset_uri = "s3://bucket/key/1"
        self.create_assets(session=session, num=1)
        asset_id = 1

        self._create_asset_dag_run_queues(dag_id, asset_id, session)
        adrq = session.query(AssetDagRunQueue).all()
        assert len(adrq) == 1

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvent/{asset_uri}",
        )

        assert response.status_code == 204
        adrq = session.query(AssetDagRunQueue).all()
        assert len(adrq) == 0

    def test_should_respond_404(self, test_client):
        dag_id = "not_exists"
        asset_uri = "not_exists"

        response = test_client.delete(
            f"/public/dags/{dag_id}/assets/queuedEvent/{asset_uri}",
        )

        assert response.status_code == 404
        assert (
            response.json()["detail"]
            == "Queue event with dag_id: `not_exists` and asset uri: `not_exists` was not found"
        )
