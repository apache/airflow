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

from airflow.sdk import task
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"

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
            def transform(country: str, extracted: dict): ...

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
            {"name": "country", "kind": "literal", "data_type": "string", "value": "uk"},
            {"name": "extracted", "kind": "xcom", "data_type": "object", "task_id": "extract"},
        ]
