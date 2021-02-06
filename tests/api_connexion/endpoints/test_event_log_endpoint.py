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
import unittest

from parameterized import parameterized

from airflow import DAG
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import Log, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_logs


class TestEventLogEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        create_user(
            cls.app,  # type:ignore
            username="test",
            role_name="Test",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG)],  # type: ignore
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        delete_user(cls.app, username="test_no_permissions")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        clear_db_logs()
        self.default_time = "2020-06-10T20:00:00+00:00"
        self.default_time_2 = '2020-06-11T07:00:00+00:00'

    def tearDown(self) -> None:
        clear_db_logs()

    def _create_task_instance(self):
        dag = DAG(
            'TEST_DAG_ID',
            start_date=timezone.parse(self.default_time),
            end_date=timezone.parse(self.default_time),
        )
        op1 = DummyOperator(
            task_id="TEST_TASK_ID",
            owner="airflow",
        )
        dag.add_task(op1)
        ti = TaskInstance(task=op1, execution_date=timezone.parse(self.default_time))
        return ti


class TestGetEventLog(TestEventLogEndpoint):
    @provide_session
    def test_should_respond_200(self, session):
        log_model = Log(
            event='TEST_EVENT',
            task_instance=self._create_task_instance(),
        )
        log_model.dttm = timezone.parse(self.default_time)
        session.add(log_model)
        session.commit()
        event_log_id = log_model.id
        response = self.client.get(
            f"/api/v1/eventLogs/{event_log_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == {
            "event_log_id": event_log_id,
            "event": "TEST_EVENT",
            "dag_id": "TEST_DAG_ID",
            "task_id": "TEST_TASK_ID",
            "execution_date": self.default_time,
            "owner": 'airflow',
            "when": self.default_time,
            "extra": None,
        }

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/eventLogs/1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404
        assert {
            'detail': None,
            'status': 404,
            'title': 'Event Log not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    @provide_session
    def test_should_raises_401_unauthenticated(self, session):
        log_model = Log(
            event='TEST_EVENT',
            task_instance=self._create_task_instance(),
        )
        log_model.dttm = timezone.parse(self.default_time)
        session.add(log_model)
        session.commit()
        event_log_id = log_model.id

        response = self.client.get(f"/api/v1/eventLogs/{event_log_id}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/eventLogs", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetEventLogs(TestEventLogEndpoint):
    @provide_session
    def test_should_respond_200(self, session):
        log_model_1 = Log(
            event='TEST_EVENT_1',
            task_instance=self._create_task_instance(),
        )
        log_model_2 = Log(
            event='TEST_EVENT_2',
            task_instance=self._create_task_instance(),
        )
        log_model_3 = Log(event="cli_scheduler", owner='root', extra='{"host_name": "e24b454f002a"}')
        log_model_1.dttm = timezone.parse(self.default_time)
        log_model_2.dttm = timezone.parse(self.default_time_2)
        log_model_3.dttm = timezone.parse(self.default_time_2)
        session.add_all([log_model_1, log_model_2, log_model_3])
        session.commit()
        response = self.client.get("/api/v1/eventLogs", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json == {
            "event_logs": [
                {
                    "event_log_id": log_model_1.id,
                    "event": "TEST_EVENT_1",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "execution_date": self.default_time,
                    "owner": 'airflow',
                    "when": self.default_time,
                    "extra": None,
                },
                {
                    "event_log_id": log_model_2.id,
                    "event": "TEST_EVENT_2",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "execution_date": self.default_time,
                    "owner": 'airflow',
                    "when": self.default_time_2,
                    "extra": None,
                },
                {
                    "event_log_id": log_model_3.id,
                    "event": "cli_scheduler",
                    "dag_id": None,
                    "task_id": None,
                    "execution_date": None,
                    "owner": 'root',
                    "when": self.default_time_2,
                    "extra": '{"host_name": "e24b454f002a"}',
                },
            ],
            "total_entries": 3,
        }

    @provide_session
    def test_should_raises_401_unauthenticated(self, session):
        log_model_1 = Log(
            event='TEST_EVENT_1',
            task_instance=self._create_task_instance(),
        )
        log_model_2 = Log(
            event='TEST_EVENT_2',
            task_instance=self._create_task_instance(),
        )
        log_model_1.dttm = timezone.parse(self.default_time)
        log_model_2.dttm = timezone.parse(self.default_time_2)
        session.add_all([log_model_1, log_model_2])
        session.commit()

        response = self.client.get("/api/v1/eventLogs")

        assert_401(response)


class TestGetEventLogPagination(TestEventLogEndpoint):
    @parameterized.expand(
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
        ]
    )
    @provide_session
    def test_handle_limit_and_offset(self, url, expected_events, session):
        log_models = self._create_event_logs(10)
        session.add_all(log_models)
        session.commit()

        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 10
        events = [event_log["event"] for event_log in response.json["event_logs"]]
        assert events == expected_events

    @provide_session
    def test_should_respect_page_size_limit_default(self, session):
        log_models = self._create_event_logs(200)
        session.add_all(log_models)
        session.commit()

        response = self.client.get("/api/v1/eventLogs", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["event_logs"]) == 100  # default 100

    @provide_session
    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        log_models = self._create_event_logs(200)
        session.add_all(log_models)
        session.commit()

        response = self.client.get("/api/v1/eventLogs?limit=180", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert len(response.json['event_logs']) == 150

    def _create_event_logs(self, count):
        return [
            Log(event="TEST_EVENT_" + str(i), task_instance=self._create_task_instance())
            for i in range(1, count + 1)
        ]
