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
from typing import TYPE_CHECKING, cast

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException, AirflowFailException, AirflowSkipException
from airflow.providers.http.sensors.remote_external_task import RemoteExternalTaskSensor

if TYPE_CHECKING:
    from airflow.sdk import Context

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


def context_for(date: datetime.datetime) -> Context:
    # ExternalTaskSensor reads "logical_date" on Airflow 3 and "execution_date" on Airflow 2.
    return cast("Context", {"logical_date": date, "execution_date": date})


def poke(sensor: RemoteExternalTaskSensor) -> bool:
    return sensor.poke(context_for(LOGICAL_DATE))


def total(count: int) -> dict:
    return {"json": {"total_entries": count}}


def task_instances(*instances: tuple[str, int, str | None]) -> dict:
    """List endpoint response with task instances given as (task_id, map_index, state)."""
    return {
        "json": {
            "task_instances": [
                {"task_id": task_id, "map_index": map_index, "state": state}
                for task_id, map_index, state in instances
            ],
            "total_entries": len(instances),
        }
    }


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

    def test_all_external_tasks_in_allowed_state(self, requests_mock):
        requests_mock.post(
            TASK_INSTANCES_V1, **task_instances(("group.a", -1, "success"), ("b", -1, "success"))
        )

        assert poke(make_sensor(external_task_ids=["group.a", "b"])) is True

        assert requests_mock.last_request.json() == {
            "dag_ids": ["remote_dag"],
            "task_ids": ["group.a", "b"],
            "execution_date_gte": LOGICAL_DATE_ISO,
            "execution_date_lte": LOGICAL_DATE_ISO,
            "page_offset": 0,
            "page_limit": 100,
        }

    @pytest.mark.parametrize(
        "instances",
        [
            pytest.param([("a", -1, "success"), ("b", -1, "running")], id="one-task-running"),
            pytest.param([("a", -1, "success")], id="one-task-has-no-instance-yet"),
            pytest.param(
                [("a", 0, "success"), ("a", 1, "success"), ("b", -1, "running")],
                id="map-indexes-of-one-task-do-not-count-for-another",
            ),
            pytest.param(
                [("a", 0, "success"), ("a", 1, "running"), ("b", -1, "success")],
                id="mapped-task-with-one-index-running",
            ),
        ],
    )
    def test_waits_until_every_task_and_map_index_is_done(self, requests_mock, instances):
        requests_mock.post(TASK_INSTANCES_V1, **task_instances(*instances))

        assert poke(make_sensor(external_task_ids=["a", "b"])) is False

    def test_mapped_task_with_all_indexes_done(self, requests_mock):
        requests_mock.post(
            TASK_INSTANCES_V1,
            **task_instances(
                ("a", 0, "success"), ("a", 1, "success"), ("a", 2, "success"), ("b", -1, "success")
            ),
        )

        assert poke(make_sensor(external_task_ids=["a", "b"])) is True

    def test_reads_all_pages_of_task_instances(self, requests_mock):
        first_page = [{"task_id": "a", "map_index": i, "state": "success"} for i in range(100)]
        requests_mock.post(
            TASK_INSTANCES_V1,
            [
                {"json": {"task_instances": first_page, "total_entries": 101}},
                {
                    "json": {
                        "task_instances": [{"task_id": "a", "map_index": 100, "state": "running"}],
                        "total_entries": 101,
                    }
                },
            ],
        )

        assert poke(make_sensor(external_task_id="a")) is False

        assert [r.json()["page_offset"] for r in requests_mock.request_history] == [0, 100]

    def test_execution_delta_shifts_the_remote_date(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))

        poke(make_sensor(execution_delta=datetime.timedelta(days=1, hours=2)))

        body = requests_mock.last_request.json()
        assert body["execution_date_gte"] == body["execution_date_lte"] == "2026-11-16T03:00:00+00:00"

    def test_execution_date_in_another_timezone_is_sent_in_utc(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))
        bangkok = datetime.timezone(datetime.timedelta(hours=7))

        make_sensor().poke(context_for(datetime.datetime(2026, 11, 17, 12, 0, tzinfo=bangkok)))

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
        requests_mock.post(TASK_INSTANCES_V2, **task_instances(("a", -1, "success")))

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

        # ExternalDagFailedError in recent standard providers, AirflowException in older ones.
        with pytest.raises(AirflowException, match="The external DAG remote_dag failed") as error:
            poke(make_sensor(failed_states=["failed"]))

        assert not isinstance(error.value, AirflowSkipException)

    def test_failed_state_with_soft_fail_skips(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(1))

        with pytest.raises(AirflowSkipException):
            poke(make_sensor(failed_states=["failed"], soft_fail=True))

    def test_one_failed_map_index_fails_the_sensor(self, requests_mock):
        requests_mock.post(
            TASK_INSTANCES_V1, **task_instances(("a", 0, "success"), ("a", 1, "failed"), ("b", -1, "running"))
        )

        with pytest.raises(
            AirflowException, match=r"Some of the external tasks \['a', 'b'\] in DAG remote_dag failed"
        ):
            poke(make_sensor(external_task_ids=["a", "b"], failed_states=["failed"]))

    def test_skipped_state_skips(self, requests_mock):
        requests_mock.post(TASK_INSTANCES_V1, **task_instances(("a", -1, "skipped")))

        with pytest.raises(AirflowSkipException):
            poke(make_sensor(external_task_id="a", skipped_states=["skipped"]))

    def test_task_states_are_read_once_per_poke(self, requests_mock):
        requests_mock.post(TASK_INSTANCES_V1, **task_instances(("a", -1, "running")))

        assert (
            poke(make_sensor(external_task_id="a", failed_states=["failed"], skipped_states=["skipped"]))
            is False
        )

        assert requests_mock.call_count == 1

    def test_no_failed_runs_keeps_waiting(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, **total(0))

        assert poke(make_sensor(failed_states=["failed"])) is False
        assert [r.json()["states"] for r in requests_mock.request_history] == [["failed"], ["success"]]


class TestRemoteExternalTaskSensorErrors:
    def test_client_error_fails_without_retries(self, requests_mock):
        requests_mock.post(DAG_RUNS_V1, status_code=422, text="unknown field")

        with pytest.raises(AirflowFailException, match="422: unknown field"):
            poke(make_sensor())

    @pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
    def test_temporary_error_keeps_retries(self, requests_mock, status_code):
        requests_mock.post(DAG_RUNS_V1, status_code=status_code, text="try later")

        with pytest.raises(AirflowException, match=f"{status_code}: try later") as error:
            poke(make_sensor())

        assert not isinstance(error.value, AirflowFailException)

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            pytest.param({"external_task_ids": [""]}, "empty task id", id="empty-task-id"),
            pytest.param({"external_task_ids": ["a", "  "]}, "empty task id", id="blank-task-id"),
            pytest.param({"external_task_ids": ["a", "a"]}, "Duplicate task ids", id="duplicate-task-ids"),
        ],
    )
    def test_invalid_task_ids_fail_before_any_request(self, requests_mock, kwargs, message):
        with pytest.raises(AirflowFailException, match=message):
            poke(make_sensor(**kwargs))

        assert requests_mock.call_count == 0

    def test_empty_dag_id_fails_before_any_request(self, requests_mock):
        sensor = make_sensor()
        sensor.external_dag_id = ""  # as rendered from "{{ params.dag_id }}" with an empty param

        with pytest.raises(AirflowFailException, match="external_dag_id is empty"):
            poke(sensor)

        assert requests_mock.call_count == 0

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
