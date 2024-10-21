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

from airflow.models import Log
from airflow.security import permissions
from airflow.utils import timezone

from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.db import clear_db_logs

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skip_if_database_isolation_mode,
    pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+"),
]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api
    create_user(
        app,
        username="test_granular",
        role_name="TestGranular",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG)],
    )
    app.appbuilder.sm.sync_perm_for_dag(
        "TEST_DAG_ID_1",
        access_control={"TestGranular": [permissions.ACTION_CAN_READ]},
    )
    app.appbuilder.sm.sync_perm_for_dag(
        "TEST_DAG_ID_2",
        access_control={"TestGranular": [permissions.ACTION_CAN_READ]},
    )

    yield app

    delete_user(app, username="test_granular")


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


class TestGetEventLogs(TestEventLogEndpoint):
    def test_should_filter_eventlogs_by_allowed_attributes(self, create_log_model, session):
        eventlog1 = create_log_model(
            event="TEST_EVENT_1",
            dag_id="TEST_DAG_ID_1",
            task_id="TEST_TASK_ID_1",
            owner="TEST_OWNER_1",
            when=self.default_time,
        )
        eventlog2 = create_log_model(
            event="TEST_EVENT_2",
            dag_id="TEST_DAG_ID_2",
            task_id="TEST_TASK_ID_2",
            owner="TEST_OWNER_2",
            when=self.default_time_2,
        )
        session.add_all([eventlog1, eventlog2])
        session.commit()
        for attr in ["dag_id", "task_id", "owner", "event"]:
            attr_value = f"TEST_{attr}_1".upper()
            response = self.client.get(
                f"/api/v1/eventLogs?{attr}={attr_value}", environ_overrides={"REMOTE_USER": "test_granular"}
            )
            assert response.status_code == 200
            assert response.json["total_entries"] == 1
            assert len(response.json["event_logs"]) == 1
            assert response.json["event_logs"][0][attr] == attr_value

    def test_should_filter_eventlogs_by_included_events(self, create_log_model):
        for event in ["TEST_EVENT_1", "TEST_EVENT_2", "cli_scheduler"]:
            create_log_model(event=event, when=self.default_time)
        response = self.client.get(
            "/api/v1/eventLogs?included_events=TEST_EVENT_1,TEST_EVENT_2",
            environ_overrides={"REMOTE_USER": "test_granular"},
        )
        assert response.status_code == 200
        response_data = response.json
        assert len(response_data["event_logs"]) == 2
        assert response_data["total_entries"] == 2
        assert {"TEST_EVENT_1", "TEST_EVENT_2"} == {x["event"] for x in response_data["event_logs"]}

    def test_should_filter_eventlogs_by_excluded_events(self, create_log_model):
        for event in ["TEST_EVENT_1", "TEST_EVENT_2", "cli_scheduler"]:
            create_log_model(event=event, when=self.default_time)
        response = self.client.get(
            "/api/v1/eventLogs?excluded_events=TEST_EVENT_1,TEST_EVENT_2",
            environ_overrides={"REMOTE_USER": "test_granular"},
        )
        assert response.status_code == 200
        response_data = response.json
        assert len(response_data["event_logs"]) == 1
        assert response_data["total_entries"] == 1
        assert {"cli_scheduler"} == {x["event"] for x in response_data["event_logs"]}
