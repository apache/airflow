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

from typing import Generator

import pytest
import time_machine

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models.dataset import (
    DatasetDagRunQueue,
    DatasetModel,
)
from airflow.security import permissions
from airflow.utils import timezone
from tests.providers.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user
from tests.test_utils.db import clear_db_datasets, clear_db_runs
from tests.test_utils.www import _check_last_log

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api
    create_user(
        app,
        username="test_queued_event",
        role_name="TestQueuedEvent",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DATASET),
        ],
    )

    yield app

    delete_user(app, username="test_queued_event")


class TestDatasetEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()
        clear_db_datasets()
        clear_db_runs()

    def teardown_method(self) -> None:
        clear_db_datasets()
        clear_db_runs()

    def _create_dataset(self, session):
        dataset_model = DatasetModel(
            id=1,
            uri="s3://bucket/key",
            extra={"foo": "bar"},
            created_at=timezone.parse(self.default_time),
            updated_at=timezone.parse(self.default_time),
        )
        session.add(dataset_model)
        session.commit()
        return dataset_model


class TestQueuedEventEndpoint(TestDatasetEndpoint):
    @pytest.fixture
    def time_freezer(self) -> Generator:
        freezer = time_machine.travel(self.default_time, tick=False)
        freezer.start()

        yield

        freezer.stop()

    def _create_dataset_dag_run_queues(self, dag_id, dataset_id, session):
        ddrq = DatasetDagRunQueue(target_dag_id=dag_id, dataset_id=dataset_id)
        session.add(ddrq)
        session.commit()
        return ddrq


class TestGetDagDatasetQueuedEvent(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        dataset_id = self._create_dataset(session).id
        self._create_dataset_dag_run_queues(dag_id, dataset_id, session)
        dataset_uri = "s3://bucket/key"

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 200
        assert response.json == {
            "created_at": self.default_time,
            "uri": "s3://bucket/key",
            "dag_id": "dag",
        }

    def test_should_respond_404(self):
        dag_id = "not_exists"
        dataset_uri = "not_exists"

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 404
        assert {
            "detail": "Queue event with dag_id: `not_exists` and dataset uri: `not_exists` was not found",
            "status": 404,
            "title": "Queue event not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestDeleteDagDatasetQueuedEvent(TestDatasetEndpoint):
    def test_delete_should_respond_204(self, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        dataset_uri = "s3://bucket/key"
        dataset_id = self._create_dataset(session).id

        ddrq = DatasetDagRunQueue(target_dag_id=dag_id, dataset_id=dataset_id)
        session.add(ddrq)
        session.commit()
        conn = session.query(DatasetDagRunQueue).all()
        assert len(conn) == 1

        response = self.client.delete(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 204
        conn = session.query(DatasetDagRunQueue).all()
        assert len(conn) == 0
        _check_last_log(
            session, dag_id=dag_id, event="api.delete_dag_dataset_queued_event", execution_date=None
        )

    def test_should_respond_404(self):
        dag_id = "not_exists"
        dataset_uri = "not_exists"

        response = self.client.delete(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 404
        assert {
            "detail": "Queue event with dag_id: `not_exists` and dataset uri: `not_exists` was not found",
            "status": 404,
            "title": "Queue event not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestGetDagDatasetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        dataset_id = self._create_dataset(session).id
        self._create_dataset_dag_run_queues(dag_id, dataset_id, session)

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 200
        assert response.json == {
            "queued_events": [
                {
                    "created_at": self.default_time,
                    "uri": "s3://bucket/key",
                    "dag_id": "dag",
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_404(self):
        dag_id = "not_exists"

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 404
        assert {
            "detail": "Queue event with dag_id: `not_exists` was not found",
            "status": 404,
            "title": "Queue event not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestDeleteDagDatasetQueuedEvents(TestDatasetEndpoint):
    def test_should_respond_404(self):
        dag_id = "not_exists"

        response = self.client.delete(
            f"/api/v1/dags/{dag_id}/datasets/queuedEvent",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 404
        assert {
            "detail": "Queue event with dag_id: `not_exists` was not found",
            "status": 404,
            "title": "Queue event not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestGetDatasetQueuedEvents(TestQueuedEventEndpoint):
    @pytest.mark.usefixtures("time_freezer")
    def test_should_respond_200(self, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        dataset_id = self._create_dataset(session).id
        self._create_dataset_dag_run_queues(dag_id, dataset_id, session)
        dataset_uri = "s3://bucket/key"

        response = self.client.get(
            f"/api/v1/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 200
        assert response.json == {
            "queued_events": [
                {
                    "created_at": self.default_time,
                    "uri": "s3://bucket/key",
                    "dag_id": "dag",
                }
            ],
            "total_entries": 1,
        }

    def test_should_respond_404(self):
        dataset_uri = "not_exists"

        response = self.client.get(
            f"/api/v1/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 404
        assert {
            "detail": "Queue event with dataset uri: `not_exists` was not found",
            "status": 404,
            "title": "Queue event not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestDeleteDatasetQueuedEvents(TestQueuedEventEndpoint):
    def test_delete_should_respond_204(self, session, create_dummy_dag):
        dag, _ = create_dummy_dag()
        dag_id = dag.dag_id
        dataset_id = self._create_dataset(session).id
        self._create_dataset_dag_run_queues(dag_id, dataset_id, session)
        dataset_uri = "s3://bucket/key"

        response = self.client.delete(
            f"/api/v1/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 204
        conn = session.query(DatasetDagRunQueue).all()
        assert len(conn) == 0
        _check_last_log(session, dag_id=None, event="api.delete_dataset_queued_events", execution_date=None)

    def test_should_respond_404(self):
        dataset_uri = "not_exists"

        response = self.client.delete(
            f"/api/v1/datasets/queuedEvent/{dataset_uri}",
            environ_overrides={"REMOTE_USER": "test_queued_event"},
        )

        assert response.status_code == 404
        assert {
            "detail": "Queue event with dataset uri: `not_exists` was not found",
            "status": 404,
            "title": "Queue event not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json
