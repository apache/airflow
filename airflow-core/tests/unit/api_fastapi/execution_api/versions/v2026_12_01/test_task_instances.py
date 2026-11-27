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

from airflow.models import TaskInstance, Trigger
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_runs, clear_db_triggers

pytestmark = pytest.mark.db_test

PREVIOUS_VERSION = "2026-10-30"


@pytest.fixture(autouse=True)
def _clean_db():
    clear_db_runs()
    clear_db_triggers()
    yield
    clear_db_runs()
    clear_db_triggers()


@pytest.mark.parametrize(
    ("trigger_kwargs", "expected_status", "expected_state"),
    [
        pytest.param({"key": "value"}, 204, State.DEFERRED, id="dict"),
        pytest.param('{"key": "value"}', 422, State.RUNNING, id="string"),
    ],
)
def test_old_version_deferral_trigger_kwargs(
    client, session, create_task_instance, trigger_kwargs, expected_status, expected_state
):
    ti = create_task_instance(
        task_id="test_old_version_deferral_trigger_kwargs", state=State.RUNNING, session=session
    )
    session.commit()
    client.headers["Airflow-API-Version"] = PREVIOUS_VERSION

    response = client.patch(
        f"/execution/task-instances/{ti.id}/state",
        json={
            "state": "deferred",
            "trigger_kwargs": trigger_kwargs,
            "classpath": "my-classpath",
            "next_method": "execute_callback",
        },
    )

    assert response.status_code == expected_status, response.text
    if expected_status == 422:
        assert response.json()["detail"]["reason"] == "invalid_trigger_kwargs"
    session.expire_all()
    assert session.scalar(select(TaskInstance.state).where(TaskInstance.id == ti.id)) == expected_state
    triggers = session.scalars(select(Trigger)).all()
    assert [t.kwargs for t in triggers] == ([trigger_kwargs] if expected_status == 204 else [])


@pytest.mark.parametrize("version", [PREVIOUS_VERSION, "2026-12-01"])
def test_trigger_kwargs_schema_by_version(client, version):
    response = client.get(f"/execution/openapi.json?version={version}")

    assert response.status_code == 200
    field = response.json()["components"]["schemas"]["TIDeferredStatePayload"]["properties"]["trigger_kwargs"]
    types = {option.get("type") for option in field.get("oneOf", [field])}
    assert ("string" in types) == (version == PREVIOUS_VERSION)
