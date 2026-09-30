#
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

from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from airflow.models import Connection

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("Waiting for a remote Airflow deployment needs Airflow 3+", allow_module_level=True)

from airflow.providers.http.triggers.external_task import HttpExternalTaskTrigger, _AirflowApiClient
from airflow.triggers.base import TriggerEvent

BASE_URL = "https://airflow.example.com"
API_URL = f"{BASE_URL}/api/v2"
DATE_1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
DATE_2 = datetime(2026, 1, 2, tzinfo=timezone.utc)


def get_query(request) -> dict[str, list[str]]:
    # requests_mock's ``qs`` lowercases values, which mangles ISO timestamps.
    return parse_qs(urlsplit(request.url).query)


@pytest.fixture(autouse=True)
def setup_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(conn_id="with_login", conn_type="http", host=BASE_URL, login="user", password="secret")
    )
    create_connection_without_db(
        Connection(conn_id="with_token", conn_type="http", host=BASE_URL, password="api-token")
    )
    create_connection_without_db(Connection(conn_id="anonymous", conn_type="http", host=BASE_URL))


@pytest.fixture
def token_mock(requests_mock):
    return requests_mock.post(
        f"{BASE_URL}/auth/token",
        [
            {"json": {"access_token": "jwt-1"}, "status_code": 201},
            {"json": {"access_token": "jwt-2"}, "status_code": 201},
        ],
    )


@pytest.fixture
def client(token_mock):
    return _AirflowApiClient("with_login")


class TestAirflowApiClientAuth:
    def test_login_is_exchanged_for_access_token_once(self, requests_mock, token_mock):
        requests_mock.get(f"{API_URL}/version", json={"version": "3.2.0"})
        client = _AirflowApiClient("with_login")

        client._request("GET", "version")
        client._request("GET", "version")

        assert token_mock.call_count == 1
        assert token_mock.last_request.json() == {"username": "user", "password": "secret"}
        assert "Authorization" not in token_mock.last_request.headers
        assert requests_mock.last_request.headers["Authorization"] == "Bearer jwt-1"

    def test_expired_access_token_is_refreshed(self, requests_mock, token_mock):
        version_mock = requests_mock.get(
            f"{API_URL}/version", [{"status_code": 401}, {"json": {"version": "3.2.0"}}]
        )

        assert _AirflowApiClient("with_login")._request("GET", "version") == {"version": "3.2.0"}
        assert token_mock.call_count == 2
        assert version_mock.last_request.headers["Authorization"] == "Bearer jwt-2"

    @pytest.mark.parametrize(
        ("conn_id", "expected_header"), [("with_token", "Bearer api-token"), ("anonymous", None)]
    )
    def test_without_login(self, requests_mock, conn_id, expected_header):
        version_mock = requests_mock.get(f"{API_URL}/version", [{"status_code": 401}])

        with pytest.raises(requests.HTTPError, match="401"):
            _AirflowApiClient(conn_id)._request("GET", "version")

        assert version_mock.call_count == 1
        assert version_mock.last_request.headers.get("Authorization") == expected_header


class TestAirflowApiClient:
    def test_get_dr_count(self, requests_mock, client):
        dr_mock = requests_mock.get(
            f"{API_URL}/dags/my_dag/dagRuns",
            [{"json": {"dag_runs": [], "total_entries": 1}}, {"json": {"dag_runs": [], "total_entries": 0}}],
        )

        assert client.get_dr_count("my_dag", [DATE_1, DATE_2], ["success", "failed"]) == 1
        assert [get_query(r) for r in dr_mock.request_history] == [
            {
                "logical_date_gte": [date.isoformat()],
                "logical_date_lte": [date.isoformat()],
                "state": ["success", "failed"],
                "limit": ["1"],
            }
            for date in (DATE_1, DATE_2)
        ]

    def test_get_ti_count(self, requests_mock, client):
        ti_mock = requests_mock.post(
            f"{API_URL}/dags/~/dagRuns/~/taskInstances/list",
            [
                {"json": {"task_instances": [], "total_entries": 3}},
                {"json": {"task_instances": [], "total_entries": 2}},
            ],
        )

        assert client.get_ti_count("my_dag", ["t1", "t2"], [DATE_1, DATE_2], ["success"]) == 5
        assert [r.json() for r in ti_mock.request_history] == [
            {
                "dag_ids": ["my_dag"],
                "task_ids": ["t1", "t2"],
                "state": ["success"],
                "logical_date_gte": date.isoformat(),
                "logical_date_lte": date.isoformat(),
                "page_limit": 1,
            }
            for date in (DATE_1, DATE_2)
        ]

    def test_get_task_group_states(self, requests_mock, client, monkeypatch):
        monkeypatch.setattr(_AirflowApiClient, "page_limit", 2)
        requests_mock.get(f"{API_URL}/version", json={"version": "3.2.0"})

        def ti(task_id, map_index, state):
            return {"dag_run_id": "run_1", "task_id": task_id, "map_index": map_index, "state": state}

        ti_mock = requests_mock.get(
            f"{API_URL}/dags/my_dag/dagRuns/~/taskInstances",
            [
                {
                    "json": {
                        "task_instances": [ti("g.a", -1, "success"), ti("g.b", 0, "failed")],
                        "total_entries": 3,
                    }
                },
                {"json": {"task_instances": [ti("g.b", 1, None)], "total_entries": 3}},
            ],
        )

        assert client.get_task_group_states("my_dag", "g", [DATE_1]) == {
            "run_1": {"g.a": "success", "g.b_0": "failed", "g.b_1": None}
        }
        assert [get_query(r)["offset"] for r in ti_mock.request_history] == [["0"], ["2"]]
        assert get_query(ti_mock.last_request) == {
            "task_group_id": ["g"],
            "logical_date_gte": [DATE_1.isoformat()],
            "logical_date_lte": [DATE_1.isoformat()],
            "order_by": ["id"],
            "limit": ["2"],
            "offset": ["2"],
        }

    def test_get_task_group_states_requires_airflow_3_2(self, requests_mock, client):
        requests_mock.get(f"{API_URL}/version", json={"version": "3.1.8"})

        with pytest.raises(
            ValueError, match=r"requires the remote Airflow deployment to run Airflow 3\.2\.0"
        ):
            client.get_task_group_states("my_dag", "g", [DATE_1])


def create_trigger(**kwargs) -> HttpExternalTaskTrigger:
    return HttpExternalTaskTrigger(
        http_conn_id="anonymous",
        external_dag_id="my_dag",
        logical_dates=[DATE_1],
        allowed_states=["success"],
        poke_interval=5,
        **kwargs,
    )


async def get_first_event(trigger: HttpExternalTaskTrigger) -> TriggerEvent:
    return await anext(aiter(trigger.run()))


class TestHttpExternalTaskTrigger:
    def test_serialize(self):
        classpath, kwargs = create_trigger(external_task_ids=["t1"]).serialize()

        assert classpath == "airflow.providers.http.triggers.external_task.HttpExternalTaskTrigger"
        assert kwargs["http_conn_id"] == "anonymous"
        assert HttpExternalTaskTrigger(**kwargs).serialize() == (classpath, kwargs)

    @pytest.mark.asyncio
    async def test_run_dag(self, requests_mock):
        dr_mock = requests_mock.get(
            f"{API_URL}/dags/my_dag/dagRuns", json={"dag_runs": [], "total_entries": 1}
        )

        assert await get_first_event(create_trigger()) == TriggerEvent({"status": "success"})
        assert get_query(dr_mock.last_request)["state"] == ["success"]

    @pytest.mark.asyncio
    async def test_run_tasks(self, requests_mock):
        ti_mock = requests_mock.post(
            f"{API_URL}/dags/~/dagRuns/~/taskInstances/list", json={"task_instances": [], "total_entries": 2}
        )

        event = await get_first_event(
            create_trigger(external_task_ids=["t1", "t2"], failed_states=["failed"])
        )

        assert event == TriggerEvent({"status": "failed"})
        assert ti_mock.last_request.json()["task_ids"] == ["t1", "t2"]
        assert ti_mock.last_request.json()["state"] == ["failed"]

    @pytest.mark.asyncio
    async def test_run_task_group(self, requests_mock):
        requests_mock.get(f"{API_URL}/version", json={"version": "3.2.0"})
        ti_mock = requests_mock.get(
            f"{API_URL}/dags/my_dag/dagRuns/~/taskInstances",
            json={
                "task_instances": [
                    {"dag_run_id": "run_1", "task_id": "g.a", "map_index": -1, "state": "success"}
                ],
                "total_entries": 1,
            },
        )

        assert await get_first_event(create_trigger(external_task_group_id="g")) == TriggerEvent(
            {"status": "success"}
        )
        assert get_query(ti_mock.last_request)["task_group_id"] == ["g"]
