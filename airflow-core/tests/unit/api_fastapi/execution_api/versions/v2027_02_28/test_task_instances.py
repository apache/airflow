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

from datetime import timedelta

import pytest

from airflow.sdk import task
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "h",
    "unixname": "u",
    "pid": 1,
    "start_date": "2024-09-30T12:00:00Z",
}


@pytest.fixture
def old_ver_client(client):
    """Execution API version immediately before 2027-02-28."""
    client.headers["Airflow-API-Version"] = "2026-10-30"
    return client


@pytest.mark.parametrize("version", ["2026-10-30", "2027-02-28"])
def test_execution_timeout_schema_by_version(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")
    assert response.status_code == 200
    context = response.json()["components"]["schemas"]["TIRunContext"]["properties"]
    assert ("execution_timeout" in context) == (version == "2027-02-28")


class TestExecutionTimeoutBackwardCompat:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.fixture
    def stub_ti(self, dag_maker):
        with dag_maker("test_execution_timeout_compat_dag", serialized=True):

            @task.stub(execution_timeout=timedelta(seconds=90))
            def sleeper(): ...

            sleeper()

        (ti,) = dag_maker.create_dagrun().get_task_instances()
        ti.set_state(State.QUEUED)
        dag_maker.session.flush()
        return ti

    def test_old_version_strips_execution_timeout(self, old_ver_client, stub_ti):
        response = old_ver_client.patch(f"/execution/task-instances/{stub_ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert "execution_timeout" not in response.json()

    def test_head_version_includes_execution_timeout(self, client, stub_ti):
        response = client.patch(f"/execution/task-instances/{stub_ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert response.json()["execution_timeout"] == 90.0


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            {"state": "success", "end_date": "2024-09-30T12:00:00Z", "task_outlets": [], "outlet_events": []},
            id="success",
        ),
        pytest.param(
            {"state": "deferred", "classpath": "my.trigger", "next_method": "execute"}, id="deferred"
        ),
        pytest.param(
            {
                "state": "up_for_reschedule",
                "end_date": "2024-09-30T12:00:00Z",
                "reschedule_date": "2024-09-30T12:05:00Z",
            },
            id="up_for_reschedule",
        ),
        pytest.param({"state": "awaiting_input", "next_method": "execute"}, id="awaiting_input"),
        pytest.param({"state": "up_for_retry", "end_date": "2024-09-30T12:00:00Z"}, id="up_for_retry"),
    ],
)
def test_old_version_state_update_without_sender_is_accepted(
    old_ver_client, session, create_task_instance, payload
):
    ti = create_task_instance(task_id="old_version_sender", state=State.RUNNING)
    ti.hostname, ti.pid = "worker-b", 200
    session.commit()

    response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)

    assert response.status_code == 204


@pytest.mark.parametrize(
    ("sender_field", "value"),
    [pytest.param("hostname", "worker-a", id="hostname"), pytest.param("pid", 100, id="pid")],
)
def test_old_version_state_update_rejects_sender_fields(
    old_ver_client, session, create_task_instance, sender_field, value
):
    ti = create_task_instance(task_id="old_version_sender", state=State.RUNNING)
    session.commit()
    payload = {"state": "deferred", "classpath": "my.trigger", "next_method": "execute", sender_field: value}

    response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)

    assert response.status_code == 422
    assert any(
        error["type"] == "extra_forbidden" and error["loc"][-1] == sender_field
        for error in response.json()["detail"]
    )
