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
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.trigger import Trigger
from airflow.sdk import task
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"


@pytest.mark.parametrize(
    ("version", "expected_status"), [("2025-04-11", 404), ("2026-06-30", 404), ("2026-10-30", 410)]
)
def test_retired_state_report_response_by_version(
    client, session, create_task_instance, version, expected_status
):
    ti = create_task_instance(task_id="retired_state_report", state=State.RUNNING)
    ti.try_number = 1
    old_id = ti.id
    session.commit()
    client.headers["Airflow-API-Version"] = version
    payload = {"state": "up_for_retry", "end_date": TIMESTAMP_STR}
    assert client.patch(f"/execution/task-instances/{old_id}/state", json=payload).status_code == 204

    response = client.patch(f"/execution/task-instances/{old_id}/state", json=payload)

    assert response.status_code == expected_status
    session.expunge_all()
    replacement = session.scalar(select(TaskInstance).where(TaskInstance.task_id == "retired_state_report"))
    assert replacement.id != old_id
    assert (replacement.try_number, replacement.state) == (2, State.UP_FOR_RETRY)


@pytest.mark.parametrize("version", ["2025-04-11", "2026-06-30"])
def test_old_api_omits_stopped_report_from_schema(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    schemas = response.json()["components"]["schemas"]
    assert "server_terminated" not in schemas["TerminalStateNonSuccess"]["enum"]
    assert "hostname" not in schemas["TITerminalStatePayload"]["properties"]
    assert "pid" not in schemas["TITerminalStatePayload"]["properties"]


@pytest.mark.parametrize("version", ["2025-04-11", "2026-06-30"])
def test_old_api_rejects_stopped_report(client, session, create_task_instance, version):
    ti = create_task_instance(task_id="legacy_stopped_report", state=State.RESTARTING)
    ti.hostname = "worker"
    ti.pid = 123
    old_id = ti.id
    session.commit()
    client.headers["Airflow-API-Version"] = version

    response = client.patch(
        f"/execution/task-instances/{old_id}/state",
        json={"state": "server_terminated", "end_date": TIMESTAMP_STR, "hostname": "worker", "pid": 123},
    )

    assert response.status_code == 422
    session.refresh(ti)
    assert (ti.id, ti.state) == (old_id, State.RESTARTING)


@pytest.mark.parametrize("version", ["2025-04-11", "2026-06-30"])
@pytest.mark.parametrize("extra_field", [None, "hostname", "pid"])
def test_legacy_worker_finish_payload(client, session, create_task_instance, version, extra_field):
    ti = create_task_instance(task_id="legacy_finish", state=State.RUNNING)
    session.commit()
    client.headers["Airflow-API-Version"] = version
    payload = {"state": "failed", "end_date": TIMESTAMP_STR}
    if extra_field is not None:
        payload[extra_field] = {"hostname": "worker", "pid": 123}[extra_field]
    response = client.patch(
        f"/execution/task-instances/{ti.id}/state",
        json=payload,
    )
    assert response.status_code == (204 if extra_field is None else 422)
    if extra_field is not None:
        assert any(
            error["type"] == "extra_forbidden" and error["loc"][-1] == extra_field
            for error in response.json()["detail"]
        )
    session.refresh(ti)
    assert ti.state == (State.FAILED if extra_field is None else State.RUNNING)


@pytest.mark.parametrize("version", ["2025-04-11", "2026-06-30"])
@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            {
                "state": "success",
                "end_date": TIMESTAMP_STR,
                "task_outlets": [],
                "outlet_events": [],
            },
            id="success",
        ),
        pytest.param(
            {
                "state": "deferred",
                "classpath": "my.trigger",
                "trigger_kwargs": {"__type": "dict", "__var": {"key": "value"}},
                "trigger_timeout": None,
                "next_method": "execute_complete",
                "next_kwargs": {"__type": "dict", "__var": {"argument": "value"}},
            },
            id="deferred",
        ),
        pytest.param(
            {
                "state": "up_for_reschedule",
                "end_date": TIMESTAMP_STR,
                "reschedule_date": "2024-09-30T12:05:00Z",
            },
            id="reschedule",
        ),
    ],
)
def test_legacy_worker_completion_payload_preserves_attempt(
    client, session, create_task_instance, time_machine, version, payload
):
    time_machine.move_to("2024-09-30T11:59:00Z", tick=False)
    ti = create_task_instance(task_id="legacy_completion", state=State.RUNNING)
    ti.start_date = timezone.parse("2024-09-30T11:59:00Z")
    old_identity = (ti.id, ti.try_number)
    session.commit()
    client.headers["Airflow-API-Version"] = version

    response = client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)

    assert response.status_code == 204
    session.refresh(ti)
    assert (ti.id, ti.try_number) == old_identity
    assert ti.state == payload["state"]
    if ti.state == State.DEFERRED:
        trigger = session.get(Trigger, ti.trigger_id)
        assert trigger.classpath == payload["classpath"]
        assert trigger.kwargs == {"key": "value"}
        assert ti.next_method == payload["next_method"]
        assert ti.next_kwargs == payload["next_kwargs"]
        assert ti.trigger_timeout is None
    else:
        assert ti.end_date == timezone.parse(TIMESTAMP_STR)
        assert ti.duration == 60
        if ti.state == State.UP_FOR_RESCHEDULE:
            reschedule = session.scalars(select(TaskReschedule).where(TaskReschedule.ti_id == ti.id)).one()
            assert reschedule.start_date == ti.start_date
            assert reschedule.end_date == ti.end_date
            assert reschedule.reschedule_date == timezone.parse(payload["reschedule_date"])
            assert reschedule.duration == 60


def test_legacy_worker_heartbeat_rejects_restarting_task(client, session, create_task_instance):
    ti = create_task_instance(task_id="legacy_clear", state=State.RESTARTING)
    ti.hostname = "worker"
    ti.pid = 123
    old_id = ti.id
    old_try = ti.try_number
    session.commit()
    client.headers["Airflow-API-Version"] = "2025-04-11"
    response = client.put(
        f"/execution/task-instances/{ti.id}/heartbeat",
        json={"hostname": "worker", "pid": 123},
    )
    assert response.status_code == 409
    session.refresh(ti)
    assert (ti.id, ti.try_number, ti.state) == (old_id, old_try, State.RESTARTING)


RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "h",
    "unixname": "u",
    "pid": 1,
    "start_date": TIMESTAMP_STR,
}


@pytest.fixture
def old_ver_client(client):
    """Execution API version immediately before ``arg_bindings`` was added."""
    client.headers["Airflow-API-Version"] = "2026-06-30"
    return client


class TestArgBindingsFieldBackwardCompat:
    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.fixture
    def stub_ti(self, dag_maker):
        with dag_maker("test_arg_bindings_compat_dag", serialized=True):

            @task.stub
            def extract(): ...

            @task.stub
            def transform(country: str, extracted: dict, limit: int = 10): ...

            transform("uk", extract())

        dr = dag_maker.create_dagrun()
        tis = {ti.task_id: ti for ti in dr.get_task_instances()}
        for ti in tis.values():
            ti.set_state(State.QUEUED)
        dag_maker.session.flush()
        return tis["transform"]

    def test_old_version_strips_arg_bindings_even_when_set(self, old_ver_client, stub_ti):
        response = old_ver_client.patch(f"/execution/task-instances/{stub_ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert "arg_bindings" not in response.json()

    def test_head_version_includes_arg_bindings(self, client, stub_ti):
        response = client.patch(f"/execution/task-instances/{stub_ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert response.json()["arg_bindings"] == [
            {"name": "country", "kind": "literal", "value_schema": {"type": "string"}, "value": "uk"},
            {
                "name": "extracted",
                "kind": "xcom",
                "value_schema": {"type": "object", "additionalProperties": True},
                "task_id": "extract",
            },
            {
                "name": "limit",
                "kind": "literal",
                "value_schema": {"type": "integer", "format": "int64"},
                "value": 10,
                "from_default": True,
            },
        ]
