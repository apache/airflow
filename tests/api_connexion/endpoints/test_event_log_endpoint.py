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

import pytest

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import Log
from airflow.utils import timezone

from dev.tests_common.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from dev.tests_common.test_utils.config import conf_vars
from dev.tests_common.test_utils.db import clear_db_logs

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


@pytest.fixture
def task_instance(session, create_task_instance, request):
    return create_task_instance(
        session=session,
        dag_id="TEST_DAG_ID",
        task_id="TEST_TASK_ID",
        run_id="TEST_RUN_ID",
        execution_date=request.instance.default_time,
    )


@pytest.fixture
def log_model(create_log_model, request):
    return create_log_model(
        event="TEST_EVENT",
        when=request.instance.default_time,
    )


@pytest.fixture
def create_log_model(create_task_instance, task_instance, session, request):
    def maker(event, when, **kwargs):
        log_model = Log(
            event=event,
            task_instance=task_instance,
            **kwargs,
        )
        log_model.dttm = when

        session.add(log_model)
        session.flush()
        return log_model

    return maker


class TestEventLogEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_logs()
        self.default_time = timezone.parse("2020-06-10T20:00:00+00:00")
        self.default_time_2 = timezone.parse("2020-06-11T07:00:00+00:00")

    def teardown_method(self) -> None:
        clear_db_logs()


class TestGetEventLog(TestEventLogEndpoint):
    def test_should_respond_200(self, log_model):
        event_log_id = log_model.id
        response = self.client.get(
            f"/api/v1/eventLogs/{event_log_id}", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        data = response.json
        data["try_number"]
        assert response.json == {
            "event_log_id": event_log_id,
            "event": "TEST_EVENT",
            "dag_id": "TEST_DAG_ID",
            "task_id": "TEST_TASK_ID",
            "run_id": "TEST_RUN_ID",
            "map_index": -1,
            "try_number": 0,
            "execution_date": self.default_time.isoformat(),
            "owner": "airflow",
            "when": self.default_time.isoformat(),
            "extra": None,
        }

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/eventLogs/1", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404
        assert {
            "detail": None,
            "status": 404,
            "title": "Event Log not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, log_model):
        event_log_id = log_model.id

        response = self.client.get(f"/api/v1/eventLogs/{event_log_id}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/eventLogs", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetEventLogs(TestEventLogEndpoint):
    def test_should_respond_200(self, session, create_log_model):
        log_model_1 = create_log_model(event="TEST_EVENT_1", when=self.default_time)
        log_model_2 = create_log_model(event="TEST_EVENT_2", when=self.default_time_2)
        log_model_3 = Log(event="cli_scheduler", owner="root", extra='{"host_name": "e24b454f002a"}')
        log_model_3.dttm = self.default_time_2

        session.add(log_model_3)
        session.flush()
        response = self.client.get("/api/v1/eventLogs", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {
            "event_logs": [
                {
                    "event_log_id": log_model_1.id,
                    "event": "TEST_EVENT_1",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "run_id": "TEST_RUN_ID",
                    "map_index": -1,
                    "try_number": 0,
                    "execution_date": self.default_time.isoformat(),
                    "owner": "airflow",
                    "when": self.default_time.isoformat(),
                    "extra": None,
                },
                {
                    "event_log_id": log_model_2.id,
                    "event": "TEST_EVENT_2",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "run_id": "TEST_RUN_ID",
                    "map_index": -1,
                    "try_number": 0,
                    "execution_date": self.default_time.isoformat(),
                    "owner": "airflow",
                    "when": self.default_time_2.isoformat(),
                    "extra": None,
                },
                {
                    "event_log_id": log_model_3.id,
                    "event": "cli_scheduler",
                    "dag_id": None,
                    "task_id": None,
                    "run_id": None,
                    "map_index": None,
                    "try_number": None,
                    "execution_date": None,
                    "owner": "root",
                    "when": self.default_time_2.isoformat(),
                    "extra": '{"host_name": "e24b454f002a"}',
                },
            ],
            "total_entries": 3,
        }

    def test_order_eventlogs_by_owner(self, create_log_model, session):
        log_model_1 = create_log_model(event="TEST_EVENT_1", when=self.default_time)
        log_model_2 = create_log_model(event="TEST_EVENT_2", when=self.default_time_2, owner="zsh")
        log_model_3 = Log(event="cli_scheduler", owner="root", extra='{"host_name": "e24b454f002a"}')
        log_model_3.dttm = self.default_time_2
        session.add(log_model_3)
        session.flush()
        response = self.client.get(
            "/api/v1/eventLogs?order_by=-owner", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert response.json == {
            "event_logs": [
                {
                    "event_log_id": log_model_2.id,
                    "event": "TEST_EVENT_2",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "run_id": "TEST_RUN_ID",
                    "map_index": -1,
                    "try_number": 0,
                    "execution_date": self.default_time.isoformat(),
                    "owner": "zsh",  # Order by name, sort order is descending(-)
                    "when": self.default_time_2.isoformat(),
                    "extra": None,
                },
                {
                    "event_log_id": log_model_3.id,
                    "event": "cli_scheduler",
                    "dag_id": None,
                    "task_id": None,
                    "run_id": None,
                    "map_index": None,
                    "try_number": None,
                    "execution_date": None,
                    "owner": "root",
                    "when": self.default_time_2.isoformat(),
                    "extra": '{"host_name": "e24b454f002a"}',
                },
                {
                    "event_log_id": log_model_1.id,
                    "event": "TEST_EVENT_1",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "run_id": "TEST_RUN_ID",
                    "map_index": -1,
                    "try_number": 0,
                    "execution_date": self.default_time.isoformat(),
                    "owner": "airflow",
                    "when": self.default_time.isoformat(),
                    "extra": None,
                },
            ],
            "total_entries": 3,
        }

    def test_should_raises_401_unauthenticated(self, log_model):
        response = self.client.get("/api/v1/eventLogs")

        assert_401(response)

    def test_should_filter_eventlogs_by_when(self, create_log_model, session):
        eventlog1 = create_log_model(event="TEST_EVENT_1", when=self.default_time)
        eventlog2 = create_log_model(event="TEST_EVENT_2", when=self.default_time_2)
        session.add_all([eventlog1, eventlog2])
        session.commit()
        for when_attr, expected_eventlog_event in {
            "before": "TEST_EVENT_1",
            "after": "TEST_EVENT_2",
        }.items():
            response = self.client.get(
                f"/api/v1/eventLogs?{when_attr}=2020-06-10T20%3A00%3A01%2B00%3A00",  # self.default_time + 1s
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.status_code == 200
            assert response.json["total_entries"] == 1
            assert len(response.json["event_logs"]) == 1
            assert response.json["event_logs"][0]["event"] == expected_eventlog_event

    def test_should_filter_eventlogs_by_run_id(self, create_log_model, session):
        eventlog1 = create_log_model(event="TEST_EVENT_1", when=self.default_time, run_id="run_1")
        eventlog2 = create_log_model(event="TEST_EVENT_2", when=self.default_time, run_id="run_2")
        eventlog3 = create_log_model(event="TEST_EVENT_3", when=self.default_time, run_id="run_2")
        session.add_all([eventlog1, eventlog2, eventlog3])
        session.commit()
        for run_id, expected_eventlogs in {
            "run_1": {"TEST_EVENT_1"},
            "run_2": {"TEST_EVENT_2", "TEST_EVENT_3"},
        }.items():
            response = self.client.get(
                f"/api/v1/eventLogs?run_id={run_id}",
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.status_code == 200
            assert response.json["total_entries"] == len(expected_eventlogs)
            assert len(response.json["event_logs"]) == len(expected_eventlogs)
            assert {eventlog["event"] for eventlog in response.json["event_logs"]} == expected_eventlogs
            assert all({eventlog["run_id"] == run_id for eventlog in response.json["event_logs"]})


class TestGetEventLogPagination(TestEventLogEndpoint):
    @pytest.mark.parametrize(
        ("url", "expected_events"),
        [
            ("api/v1/eventLogs?limit=1", ["TEST_EVENT_1"]),
            ("api/v1/eventLogs?limit=2", ["TEST_EVENT_1", "TEST_EVENT_2"]),
            (
                "api/v1/eventLogs?offset=5",
                [
                    "TEST_EVENT_6",
                    "TEST_EVENT_7",
                    "TEST_EVENT_8",
                    "TEST_EVENT_9",
                    "TEST_EVENT_10",
                ],
            ),
            (
                "api/v1/eventLogs?offset=0",
                [
                    "TEST_EVENT_1",
                    "TEST_EVENT_2",
                    "TEST_EVENT_3",
                    "TEST_EVENT_4",
                    "TEST_EVENT_5",
                    "TEST_EVENT_6",
                    "TEST_EVENT_7",
                    "TEST_EVENT_8",
                    "TEST_EVENT_9",
                    "TEST_EVENT_10",
                ],
            ),
            ("api/v1/eventLogs?limit=1&offset=5", ["TEST_EVENT_6"]),
            ("api/v1/eventLogs?limit=1&offset=1", ["TEST_EVENT_2"]),
            (
                "api/v1/eventLogs?limit=2&offset=2",
                ["TEST_EVENT_3", "TEST_EVENT_4"],
            ),
        ],
    )
    def test_handle_limit_and_offset(self, url, expected_events, task_instance, session):
        log_models = self._create_event_logs(task_instance, 10)
        session.add_all(log_models)
        session.commit()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 10
        events = [event_log["event"] for event_log in response.json["event_logs"]]
        assert events == expected_events

    def test_should_respect_page_size_limit_default(self, task_instance, session):
        log_models = self._create_event_logs(task_instance, 200)
        session.add_all(log_models)
        session.flush()

        response = self.client.get("/api/v1/eventLogs", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["event_logs"]) == 100  # default 100

    def test_should_raise_400_for_invalid_order_by_name(self, task_instance, session):
        log_models = self._create_event_logs(task_instance, 200)
        session.add_all(log_models)
        session.flush()

        response = self.client.get(
            "/api/v1/eventLogs?order_by=invalid", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, task_instance, session):
        log_models = self._create_event_logs(task_instance, 200)
        session.add_all(log_models)
        session.flush()

        response = self.client.get("/api/v1/eventLogs?limit=180", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert len(response.json["event_logs"]) == 150

    def _create_event_logs(self, task_instance, count):
        return [Log(event=f"TEST_EVENT_{i}", task_instance=task_instance) for i in range(1, count + 1)]
