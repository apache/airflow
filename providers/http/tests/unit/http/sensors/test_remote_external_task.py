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

import base64
import datetime

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowFailException, AirflowSkipException
from airflow.providers.http.sensors.remote_external_task import RemoteExternalTaskSensor

REMOTE = "http://remote-airflow:8080"
LOGICAL_DATE = datetime.datetime(2026, 11, 17, 5, 0, tzinfo=datetime.UTC)
LOGICAL_DATE_ISO = "2026-11-17T05:00:00+00:00"
DAG_RUNS_V1 = f"{REMOTE}/api/v1/dags/~/dagRuns/list"
DAG_RUNS_V2 = f"{REMOTE}/api/v2/dags/~/dagRuns/list"
TASK_INSTANCES_V1 = f"{REMOTE}/api/v1/dags/~/dagRuns/~/taskInstances/list"
TASK_INSTANCES_V2 = f"{REMOTE}/api/v2/dags/~/dagRuns/~/taskInstances/list"
TOKEN_URL = f"{REMOTE}/auth/token"


@pytest.fixture(autouse=True)
def remote_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(conn_id="remote_no_auth", conn_type="http", host="remote-airflow", port=8080)
    )
    create_connection_without_db(
        Connection(
            conn_id="remote_with_login",
            conn_type="http",
            host="http://remote-airflow",
            port=8080,
            login="sensor",
            password="secret",
        )
    )


def make_sensor(**kwargs) -> RemoteExternalTaskSensor:
    kwargs.setdefault("http_conn_id", "remote_no_auth")
    kwargs.setdefault("api_version", "v1")
    return RemoteExternalTaskSensor(task_id="wait_remote", external_dag_id="remote_dag", **kwargs)


def poke(sensor: RemoteExternalTaskSensor) -> bool:
    return sensor.poke({"logical_date": LOGICAL_DATE})


def total(count: int) -> dict:
    return {"json": {"total_entries": count}}


class TestRemoteExternalTaskSensorInit:
    def test_rejects_unknown_api_version(self):
        with pytest.raises(ValueError, match="api_version must be one of"):
            make_sensor(api_version="v3")

    def test_rejects_deferrable(self):
        with pytest.raises(ValueError, match="deferrable"):
            make_sensor(deferrable=True)

    def test_rejects_task_group(self):
        with pytest.raises(ValueError, match="external_task_group_id"):
            make_sensor(external_task_group_id="group")

    def test_keeps_external_task_sensor_validation(self):
        with pytest.raises(ValueError, match="Only one of `external_task_id` or `external_task_ids`"):
            make_sensor(external_task_id="a", external_task_ids=["b"])

    def test_has_no_link_to_local_ui(self):
        assert make_sensor().operator_extra_links == []


class TestRemoteExternalTaskSensorV1:
    def test_dag_run_success(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))

        assert poke(make_sensor()) is True

        request = requests_mock.last_request
        assert request.json() == {
            "dag_ids": ["remote_dag"],
            "states": ["success"],
            "execution_date_gte": LOGICAL_DATE_ISO,
            "execution_date_lte": LOGICAL_DATE_ISO,
            "page_limit": 1,
        }
        assert "Authorization" not in request.headers

    def test_dag_run_not_finished(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(0))

        assert poke(make_sensor()) is False

    def test_login_is_sent_with_basic_auth(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))

        assert poke(make_sensor(http_conn_id="remote_with_login")) is True

        expected = base64.b64encode(b"sensor:secret").decode()
        assert requests_mock.last_request.headers["Authorization"] == f"Basic {expected}"
        assert not any(r.url == TOKEN_URL for r in requests_mock.request_history)

    @pytest.mark.parametrize(("finished", "expected"), [(1, False), (2, True)])
    def test_all_external_tasks_must_be_in_allowed_state(self, requests_mock, finished, expected):
        requests_mock.post(TASK_INSTANCES_V1, **total(finished))

        assert poke(make_sensor(external_task_ids=["group.a", "b"])) is expected

        assert requests_mock.last_request.json() == {
            "dag_ids": ["remote_dag"],
            "task_ids": ["group.a", "b"],
            "state": ["success"],
            "execution_date_gte": LOGICAL_DATE_ISO,
            "execution_date_lte": LOGICAL_DATE_ISO,
            "page_limit": 1,
        }

    def test_execution_delta_shifts_the_remote_date(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))

        poke(make_sensor(execution_delta=datetime.timedelta(days=1, hours=2)))

        body = requests_mock.last_request.json()
        assert body["execution_date_gte"] == body["execution_date_lte"] == "2026-11-16T03:00:00+00:00"

    def test_execution_date_in_another_timezone_is_sent_in_utc(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))
        bangkok = datetime.timezone(datetime.timedelta(hours=7))

        make_sensor().poke({"logical_date": datetime.datetime(2026, 11, 17, 12, 0, tzinfo=bangkok)})

        assert requests_mock.last_request.json()["execution_date_gte"] == LOGICAL_DATE_ISO


class TestRemoteExternalTaskSensorV2:
    def test_login_is_exchanged_for_a_token(self, requests_mock):
        requests_mock.post(TOKEN_URL, json={"access_token": "jwt-token"})
        requests_mock.post(DAG_RUNS_V2, **total(1))

        assert poke(make_sensor(http_conn_id="remote_with_login", api_version="v2")) is True

        token_request, list_request = requests_mock.request_history
        assert token_request.json() == {"username": "sensor", "password": "secret"}
        assert list_request.headers["Authorization"] == "Bearer jwt-token"
        assert list_request.json() == {
            "dag_ids": ["remote_dag"],
            "states": ["success"],
            "logical_date_gte": LOGICAL_DATE_ISO,
            "logical_date_lte": LOGICAL_DATE_ISO,
            "page_limit": 1,
        }

    def test_new_token_on_every_poke(self, requests_mock):
        requests_mock.post(
            TOKEN_URL, [{"json": {"access_token": "first"}}, {"json": {"access_token": "second"}}]
        )
        requests_mock.post(DAG_RUNS_V2, **total(0))
        sensor = make_sensor(http_conn_id="remote_with_login", api_version="v2")

        poke(sensor)
        poke(sensor)

        assert requests_mock.last_request.headers["Authorization"] == "Bearer second"

    def test_task_instances_use_logical_date(self, requests_mock):
        requests_mock.post(TASK_INSTANCES_V2, **total(1))

        assert poke(make_sensor(api_version="v2", external_task_id="a")) is True

        body = requests_mock.last_request.json()
        assert body["logical_date_gte"] == body["logical_date_lte"] == LOGICAL_DATE_ISO
        assert body["task_ids"] == ["a"]

    def test_without_login_no_token_is_requested(self, requests_mock):
        requests_mock.post(DAG_RUNS_V2, **total(1))

        assert poke(make_sensor(api_version="v2")) is True

        assert [r.url for r in requests_mock.request_history] == [DAG_RUNS_V2]
        assert "Authorization" not in requests_mock.last_request.headers

    def test_wrong_credentials_fail_the_task(self, requests_mock):
        requests_mock.post(TOKEN_URL, status_code=401, text="Invalid credentials")

        with pytest.raises(AirflowFailException, match="401"):
            poke(make_sensor(http_conn_id="remote_with_login", api_version="v2"))


class TestRemoteExternalTaskSensorStates:
    def test_failed_state_fails_without_waiting_for_the_timeout(self, requests_mock):
        requests_mock.post(
            DAG_RUNS_V1, additional_matcher=lambda r: r.json()["states"] == ["failed"], **total(1)
        )

        with pytest.raises(Exception, match="The external DAG remote_dag failed"):
            poke(make_sensor(failed_states=["failed"]))

    def test_failed_state_with_soft_fail_skips(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))

        with pytest.raises(AirflowSkipException):
            poke(make_sensor(failed_states=["failed"], soft_fail=True))

    def test_failed_external_task(self, requests_mock):
        requests_mock.post(TASK_INSTANCES_V1, **total(1))

        with pytest.raises(Exception, match=r"Some of the external tasks \['a'\] in DAG remote_dag failed"):
            poke(make_sensor(external_task_id="a", failed_states=["failed"]))

    def test_skipped_state_skips(self, requests_mock):
        requests_mock.post(
            TASK_INSTANCES_V1, additional_matcher=lambda r: r.json()["state"] == ["skipped"], **total(1)
        )

        with pytest.raises(AirflowSkipException):
            poke(make_sensor(external_task_id="a", skipped_states=["skipped"]))

    def test_no_failed_runs_keeps_waiting(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(0))

        assert poke(make_sensor(failed_states=["failed"])) is False
        assert [r.json()["states"] for r in requests_mock.request_history] == [["failed"], ["success"]]


class TestRemoteExternalTaskSensorErrors:
    def test_api_error_fails_the_task(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, status_code=422, text="unknown field")

        with pytest.raises(AirflowFailException, match="422: unknown field"):
            poke(make_sensor())

    def test_check_existence_missing_dag(self, requests_mock):
        requests_mock.get(f"{REMOTE}/api/v1/dags/remote_dag", status_code=404)

        with pytest.raises(AirflowFailException, match="dags/remote_dag does not exist"):
            poke(make_sensor(check_existence=True))

    def test_check_existence_missing_task(self, requests_mock):
        requests_mock.get(f"{REMOTE}/api/v1/dags/remote_dag", json={})
        requests_mock.get(f"{REMOTE}/api/v1/dags/remote_dag/tasks/a", status_code=404)

        with pytest.raises(AirflowFailException, match="dags/remote_dag/tasks/a does not exist"):
            poke(make_sensor(external_task_id="a", check_existence=True))

    def test_check_existence_runs_once(self, requests_mock):
        dag = requests_mock.get(f"{REMOTE}/api/v1/dags/remote_dag", json={})
        requests_mock.post(DAG_RUNS_V1, **total(0))
        sensor = make_sensor(check_existence=True)

        poke(sensor)
        poke(sensor)

        assert dag.call_count == 1
