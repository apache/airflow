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
from unittest.mock import ANY

import pytest
import time_machine

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
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
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_assets, clear_db_runs
from tests.test_utils.www import _check_last_log

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


class TestDatasetEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self) -> None:
        clear_db_assets()
        clear_db_runs()

    def _create_dataset(self, session):
        asset_model = AssetModel(
            id=1,
            uri="s3://bucket/key",
            extra={"foo": "bar"},
            created_at=timezone.parse(self.default_time),
            updated_at=timezone.parse(self.default_time),
        )
        session.add(asset_model)
        session.commit()
        return asset_model


class TestGetDatasetEndpoint(TestDatasetEndpoint):
    def test_should_respond_200(self, session):
        self._create_dataset(session)
        assert session.query(AssetModel).count() == 1

        with assert_queries_count(6):
            response = self.client.get(
                f"/api/v1/datasets/{urllib.parse.quote('s3://bucket/key', safe='')}",
                environ_overrides={"REMOTE_USER": "test"},
            )
        assert response.status_code == 200
        assert response.json == {
            "id": 1,
            "uri": "s3://bucket/key",
            "extra": {"foo": "bar"},
            "created_at": self.default_time,
            "updated_at": self.default_time,
            "consuming_dags": [],
            "producing_tasks": [],
            "aliases": [],
        }

    def test_should_respond_404(self):
        response = self.client.get(
            f"/api/v1/datasets/{urllib.parse.quote('s3://bucket/key', safe='')}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert {
            "detail": "The Asset with uri: `s3://bucket/key` was not found",
            "status": 404,
            "title": "Asset not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, session):
        self._create_dataset(session)
        response = self.client.get(f"/api/v1/datasets/{urllib.parse.quote('s3://bucket/key', safe='')}")
        assert_401(response)


class TestGetDatasets(TestDatasetEndpoint):
    def test_should_respond_200(self, session):
        assets = [
            AssetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(assets)
        session.commit()
        assert session.query(AssetModel).count() == 2

        with assert_queries_count(10):
            response = self.client.get("/api/v1/datasets", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "extra": {"foo": "bar"},
                    "created_at": self.default_time,
                    "updated_at": self.default_time,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "extra": {"foo": "bar"},
                    "created_at": self.default_time,
                    "updated_at": self.default_time,
                    "consuming_dags": [],
                    "producing_tasks": [],
                    "aliases": [],
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, session):
        assets = [
            AssetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(assets)
        session.commit()
        assert session.query(AssetModel).count() == 2

        response = self.client.get(
            "/api/v1/datasets?order_by=fake", environ_overrides={"REMOTE_USER": "test"}
        )  # missing attr

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_should_raises_401_unauthenticated(self, session):
        assets = [
            AssetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(assets)
        session.commit()
        assert session.query(AssetModel).count() == 2

        response = self.client.get("/api/v1/datasets")

        assert_401(response)

    @pytest.mark.parametrize(
        "url, expected_datasets",
        [
            ("api/v1/datasets?uri_pattern=s3", {"s3://folder/key"}),
            ("api/v1/datasets?uri_pattern=bucket", {"gcp://bucket/key", "wasb://some_dataset_bucket_/key"}),
            (
                "api/v1/datasets?uri_pattern=dataset",
                {"somescheme://dataset/key", "wasb://some_dataset_bucket_/key"},
            ),
            (
                "api/v1/datasets?uri_pattern=",
                {
                    "gcp://bucket/key",
                    "s3://folder/key",
                    "somescheme://dataset/key",
                    "wasb://some_dataset_bucket_/key",
                },
            ),
        ],
    )
    @provide_session
    def test_filter_datasets_by_uri_pattern_works(self, url, expected_datasets, session):
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://dataset/key")
        asset4 = AssetModel("wasb://some_dataset_bucket_/key")
        session.add_all([asset1, asset2, asset3, asset4])
        session.commit()
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        dataset_urls = {dataset["uri"] for dataset in response.json["datasets"]}
        assert expected_datasets == dataset_urls

    @pytest.mark.parametrize("dag_ids, expected_num", [("dag1,dag2", 2), ("dag3", 1), ("dag2,dag3", 2)])
    @provide_session
    def test_filter_datasets_by_dag_ids_works(self, dag_ids, expected_num, session):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        dataset1 = AssetModel("s3://folder/key")
        dataset2 = AssetModel("gcp://bucket/key")
        dataset3 = AssetModel("somescheme://dataset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", dataset=dataset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", dataset=dataset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", dataset=dataset3)
        session.add_all([dataset1, dataset2, dataset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = self.client.get(
            f"/api/v1/datasets?dag_ids={dag_ids}", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        response_data = response.json
        assert len(response_data["datasets"]) == expected_num

    @pytest.mark.parametrize(
        "dag_ids, uri_pattern,expected_num",
        [("dag1,dag2", "folder", 1), ("dag3", "nothing", 0), ("dag2,dag3", "key", 2)],
    )
    def test_filter_datasets_by_dag_ids_and_uri_pattern_works(
        self, dag_ids, uri_pattern, expected_num, session
    ):
        session.query(DagModel).delete()
        session.commit()
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        dag3 = DagModel(dag_id="dag3")
        asset1 = AssetModel("s3://folder/key")
        asset2 = AssetModel("gcp://bucket/key")
        asset3 = AssetModel("somescheme://dataset/key")
        dag_ref1 = DagScheduleAssetReference(dag_id="dag1", dataset=asset1)
        dag_ref2 = DagScheduleAssetReference(dag_id="dag2", dataset=asset2)
        task_ref1 = TaskOutletAssetReference(dag_id="dag3", task_id="task1", dataset=asset3)
        session.add_all([asset1, asset2, asset3, dag1, dag2, dag3, dag_ref1, dag_ref2, task_ref1])
        session.commit()
        response = self.client.get(
            f"/api/v1/datasets?dag_ids={dag_ids}&uri_pattern={uri_pattern}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        response_data = response.json
        assert len(response_data["datasets"]) == expected_num


class TestGetDatasetsEndpointPagination(TestDatasetEndpoint):
    @pytest.mark.parametrize(
        "url, expected_dataset_uris",
        [
            # Limit test data
            ("/api/v1/datasets?limit=1", ["s3://bucket/key/1"]),
            ("/api/v1/datasets?limit=100", [f"s3://bucket/key/{i}" for i in range(1, 101)]),
            # Offset test data
            ("/api/v1/datasets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 102)]),
            ("/api/v1/datasets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 104)]),
            # Limit and offset test data
            ("/api/v1/datasets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ],
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_dataset_uris, session):
        assets = [
            AssetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(assets)
        session.commit()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        dataset_uris = [dataset["uri"] for dataset in response.json["datasets"]]
        assert dataset_uris == expected_dataset_uris

    def test_should_respect_page_size_limit_default(self, session):
        assets = [
            AssetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(assets)
        session.commit()

        response = self.client.get("/api/v1/datasets", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        assert len(response.json["datasets"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        assets = [
            AssetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 200)
        ]
        session.add_all(assets)
        session.commit()

        response = self.client.get("/api/v1/datasets?limit=180", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        assert len(response.json["datasets"]) == 150


class TestGetDatasetEvents(TestDatasetEndpoint):
    def test_should_respond_200(self, session):
        d = self._create_dataset(session)
        common = {
            "dataset_id": 1,
            "extra": {"foo": "bar"},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
            "created_dagruns": [],
        }

        events = [AssetEvent(id=i, timestamp=timezone.parse(self.default_time), **common) for i in [1, 2]]
        session.add_all(events)
        session.commit()
        assert session.query(AssetEvent).count() == 2

        response = self.client.get("/api/v1/datasets/events", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "dataset_events": [
                {
                    "id": 1,
                    "timestamp": self.default_time,
                    **common,
                    "dataset_uri": d.uri,
                },
                {
                    "id": 2,
                    "timestamp": self.default_time,
                    **common,
                    "dataset_uri": d.uri,
                },
            ],
            "total_entries": 2,
        }

    @pytest.mark.parametrize(
        "attr, value",
        [
            ("dataset_id", "2"),
            ("source_dag_id", "dag2"),
            ("source_task_id", "task2"),
            ("source_run_id", "run2"),
            ("source_map_index", "2"),
        ],
    )
    @provide_session
    def test_filtering(self, attr, value, session):
        assets = [
            AssetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2, 3]
        ]
        session.add_all(assets)
        session.commit()
        events = [
            AssetEvent(
                id=i,
                dataset_id=i,
                source_dag_id=f"dag{i}",
                source_task_id=f"task{i}",
                source_run_id=f"run{i}",
                source_map_index=i,
                timestamp=timezone.parse(self.default_time),
            )
            for i in [1, 2, 3]
        ]
        session.add_all(events)
        session.commit()
        assert session.query(AssetEvent).count() == 3

        response = self.client.get(
            f"/api/v1/datasets/events?{attr}={value}", environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "dataset_events": [
                {
                    "id": 2,
                    "dataset_id": 2,
                    "dataset_uri": assets[1].uri,
                    "extra": {},
                    "source_dag_id": "dag2",
                    "source_task_id": "task2",
                    "source_run_id": "run2",
                    "source_map_index": 2,
                    "timestamp": self.default_time,
                    "created_dagruns": [],
                }
            ],
            "total_entries": 1,
        }

    def test_order_by_raises_400_for_invalid_attr(self, session):
        self._create_dataset(session)
        events = [
            AssetEvent(
                dataset_id=1,
                extra="{'foo': 'bar'}",
                source_dag_id="foo",
                source_task_id="bar",
                source_run_id="custom",
                source_map_index=-1,
                timestamp=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(events)
        session.commit()
        assert session.query(AssetEvent).count() == 2

        response = self.client.get(
            "/api/v1/datasets/events?order_by=fake", environ_overrides={"REMOTE_USER": "test"}
        )  # missing attr

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_should_raises_401_unauthenticated(self, session):
        response = self.client.get("/api/v1/datasets/events")
        assert_401(response)

    def test_includes_created_dagrun(self, session):
        self._create_dataset(session)
        event = AssetEvent(
            id=1,
            dataset_id=1,
            timestamp=timezone.parse(self.default_time),
        )
        session.add(event)
        session.commit()

        dagrun = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.DATASET_TRIGGERED,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="success",
        )
        dagrun.end_date = timezone.parse(self.default_time)
        session.add(dagrun)
        session.commit()

        event.created_dagruns.append(dagrun)
        session.commit()

        response = self.client.get("/api/v1/datasets/events", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "dataset_events": [
                {
                    "id": 1,
                    "dataset_id": 1,
                    "dataset_uri": "s3://bucket/key",
                    "extra": {},
                    "source_dag_id": None,
                    "source_task_id": None,
                    "source_run_id": None,
                    "source_map_index": -1,
                    "timestamp": self.default_time,
                    "created_dagruns": [
                        {
                            "dag_id": "TEST_DAG_ID",
                            "dag_run_id": "TEST_DAG_RUN_ID",
                            "data_interval_end": None,
                            "data_interval_start": None,
                            "end_date": self.default_time,
                            "logical_date": self.default_time,
                            "start_date": self.default_time,
                            "state": "success",
                        },
                    ],
                }
            ],
            "total_entries": 1,
        }


class TestPostDatasetEvents(TestDatasetEndpoint):
    @pytest.fixture
    def time_freezer(self) -> Generator:
        freezer = time_machine.travel(self.default_time, tick=False)
        freezer.start()

        yield

        freezer.stop()

    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, session):
        self._create_dataset(session)
        event_payload = {"asset_uri": "s3://bucket/key", "extra": {"foo": "bar"}}
        response = self.client.post(
            "/api/v1/datasets/events", json=event_payload, environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "id": ANY,
            "created_dagruns": [],
            "dataset_uri": event_payload["asset_uri"],
            "dataset_id": ANY,
            "extra": {"foo": "bar", "from_rest_api": True},
            "source_dag_id": None,
            "source_task_id": None,
            "source_run_id": None,
            "source_map_index": -1,
            "timestamp": self.default_time,
        }
        _check_last_log(
            session,
            dag_id=None,
            event="api.create_asset_event",
            execution_date=None,
            expected_extra=event_payload,
        )

    @pytest.mark.enable_redact
    def test_should_mask_sensitive_extra_logs(self, session):
        self._create_dataset(session)
        event_payload = {"asset_uri": "s3://bucket/key", "extra": {"password": "bar"}}
        response = self.client.post(
            "/api/v1/datasets/events", json=event_payload, environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 200
        expected_extra = {**event_payload, "extra": {"password": "***"}}
        _check_last_log(
            session,
            dag_id=None,
            event="api.create_asset_event",
            execution_date=None,
            expected_extra=expected_extra,
        )

    def test_order_by_raises_400_for_invalid_attr(self, session):
        self._create_dataset(session)
        event_invalid_payload = {"asset_uri": "TEST_ASSET_URI", "extra": {"foo": "bar"}, "fake": {}}
        response = self.client.post(
            "/api/v1/datasets/events", json=event_invalid_payload, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400

    def test_should_raises_401_unauthenticated(self, session):
        self._create_dataset(session)
        response = self.client.post("/api/v1/datasets/events", json={"asset_uri": "TEST_ASSET_URI"})
        assert_401(response)


class TestGetDatasetEventsEndpointPagination(TestDatasetEndpoint):
    @pytest.mark.parametrize(
        "url, expected_event_runids",
        [
            # Limit test data
            ("/api/v1/datasets/events?limit=1&order_by=source_run_id", ["run1"]),
            (
                "/api/v1/datasets/events?limit=3&order_by=source_run_id",
                [f"run{i}" for i in range(1, 4)],
            ),
            # Offset test data
            (
                "/api/v1/datasets/events?offset=1&order_by=source_run_id",
                [f"run{i}" for i in range(2, 10)],
            ),
            (
                "/api/v1/datasets/events?offset=3&order_by=source_run_id",
                [f"run{i}" for i in range(4, 10)],
            ),
            # Limit and offset test data
            (
                "/api/v1/datasets/events?offset=3&limit=3&order_by=source_run_id",
                [f"run{i}" for i in [4, 5, 6]],
            ),
        ],
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_event_runids, session):
        self._create_dataset(session)
        events = [
            AssetEvent(
                dataset_id=1,
                source_dag_id="foo",
                source_task_id="bar",
                source_run_id=f"run{i}",
                source_map_index=-1,
                timestamp=timezone.parse(self.default_time),
            )
            for i in range(1, 10)
        ]
        session.add_all(events)
        session.commit()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        event_runids = [event["source_run_id"] for event in response.json["dataset_events"]]
        assert event_runids == expected_event_runids

    def test_should_respect_page_size_limit_default(self, session):
        self._create_dataset(session)
        events = [
            AssetEvent(
                dataset_id=1,
                source_dag_id="foo",
                source_task_id="bar",
                source_run_id=f"run{i}",
                source_map_index=-1,
                timestamp=timezone.parse(self.default_time),
            )
            for i in range(1, 110)
        ]
        session.add_all(events)
        session.commit()

        response = self.client.get("/api/v1/datasets/events", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        assert len(response.json["dataset_events"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        self._create_dataset(session)
        events = [
            AssetEvent(
                dataset_id=1,
                source_dag_id="foo",
                source_task_id="bar",
                source_run_id=f"run{i}",
                source_map_index=-1,
                timestamp=timezone.parse(self.default_time),
            )
            for i in range(1, 200)
        ]
        session.add_all(events)
        session.commit()

        response = self.client.get(
            "/api/v1/datasets/events?limit=180", environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 200
        assert len(response.json["dataset_events"]) == 150


class TestQueuedEventEndpoint(TestDatasetEndpoint):
    @pytest.fixture
    def time_freezer(self) -> Generator:
        freezer = time_machine.travel(self.default_time, tick=False)
        freezer.start()

        yield

        freezer.stop()

    def _create_dataset_dag_run_queues(self, dag_id, dataset_id, session):
        adrq = AssetDagRunQueue(target_dag_id=dag_id, dataset_id=dataset_id)
        session.add(adrq)
        session.commit()
        return adrq


class TestGetDagDatasetQueuedEvent(TestQueuedEventEndpoint):
    def test_should_raises_401_unauthenticated(self, session):
        dag_id = "dummy"
        dataset_uri = "dummy"

        response = self.client.get(f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}")

        assert_401(response)

    def test_should_raise_403_forbidden(self, session):
        dag_id = "dummy"
        dataset_uri = "dummy"

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403


class TestDeleteDagDatasetQueuedEvent(TestDatasetEndpoint):
    def test_should_raises_401_unauthenticated(self, session):
        dag_id = "dummy"
        dataset_uri = "dummy"
        response = self.client.delete(f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}")
        assert_401(response)

    def test_should_raise_403_forbidden(self, session):
        dag_id = "dummy"
        dataset_uri = "dummy"
        response = self.client.delete(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403


class TestGetDagDatasetQueuedEvents(TestQueuedEventEndpoint):
    def test_should_raises_401_unauthenticated(self):
        dag_id = "dummy"

        response = self.client.get(f"/api/v1/dags/{dag_id}/datasets/queuedEvent")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        dag_id = "dummy"

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403


class TestDeleteDagDatasetQueuedEvents(TestDatasetEndpoint):
    def test_should_raises_401_unauthenticated(self):
        dag_id = "dummy"

        response = self.client.delete(f"/api/v1/dags/{dag_id}/datasets/queuedEvent")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        dag_id = "dummy"

        response = self.client.delete(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403


class TestGetDatasetQueuedEvents(TestQueuedEventEndpoint):
    def test_should_raises_401_unauthenticated(self):
        dataset_uri = "not_exists"

        response = self.client.get(f"/api/v1/datasets/queuedEvent/{dataset_uri}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        dataset_uri = "not_exists"

        response = self.client.get(
            f"/api/v1/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403


class TestDeleteAssetQueuedEvents(TestQueuedEventEndpoint):
    def test_should_raises_401_unauthenticated(self):
        dataset_uri = "not_exists"

        response = self.client.delete(f"/api/v1/datasets/queuedEvent/{dataset_uri}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        dataset_uri = "not_exists"

        response = self.client.delete(
            f"/api/v1/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )

        assert response.status_code == 403
