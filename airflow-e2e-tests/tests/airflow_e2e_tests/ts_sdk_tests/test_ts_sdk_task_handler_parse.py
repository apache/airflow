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
"""
E2E tests for what the Dag processor does with the TypeScript stub tasks when it parses a Dag file.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_task_handler_parse.py -xvs

The Dag processor asks the packed bundle which task handlers it registers, binds each stub task to it,
and records the bindings. These tests read what it recorded from the metadata database, and the log it
wrote when it parsed each Dag file. Nothing here runs a task.
"""

from __future__ import annotations

import pytest

from airflow_e2e_tests.constants import TS_SDK_BUNDLE_FILE, TS_SDK_QUEUE, TS_SDK_TASK_HANDLER_BUNDLE
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    ArtifactRef,
    assert_later_parses_probe_nothing,
    get_routed_stub_tasks,
    get_task_handler_artifacts,
    get_task_handler_bindings,
)

_ARTIFACT = ArtifactRef(TS_SDK_TASK_HANDLER_BUNDLE, TS_SDK_BUNDLE_FILE)


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


def test_every_routed_stub_task_is_bound_to_the_bundle(client: AirflowClient, compose_instance):
    """Every stub task of both example Dags binds to the one packed bundle, whatever it is named."""
    routed = get_routed_stub_tasks(client, [TS_SDK_QUEUE])
    assert {"typescript_example", "typescript_taskflow_example"} <= {dag_id for dag_id, _ in routed}

    bindings = get_task_handler_bindings(compose_instance)
    wrong = {task: bindings.get(task) for task in routed if bindings.get(task) != _ARTIFACT}
    assert not wrong, f"Stub tasks not bound to {_ARTIFACT} (task: bound): {wrong}"
    assert set(get_task_handler_artifacts(compose_instance)) == {_ARTIFACT}


def test_a_later_parse_probes_nothing(client: AirflowClient, compose_instance, airflow_logs_path):
    """A file parsed again probes no artifact, and no artifact is probed twice for a file."""
    assert_later_parses_probe_nothing(
        client,
        compose_instance,
        airflow_logs_path,
        {
            "typescript_example.py": "typescript_example",
            "typescript_taskflow_example.py": "typescript_taskflow_example",
        },
    )
