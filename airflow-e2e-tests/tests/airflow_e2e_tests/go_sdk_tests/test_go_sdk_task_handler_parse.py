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
E2E tests for what the Dag processor does with the Go stub tasks when it parses a Dag file.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/test_go_sdk_task_handler_parse.py -xvs

The Dag processor asks each Go bundle which task handlers it registers, binds each stub task to the bundle
that registers it, and records the bindings. These tests read what it recorded from the metadata database,
and the log it wrote when it parsed each Dag file. Nothing here runs a task.
"""

from __future__ import annotations

import pytest

from airflow_e2e_tests.constants import (
    GO_SDK_BUNDLE_NAME,
    GO_SDK_QUEUE,
    GO_SDK_TASK_HANDLER_BUNDLE,
    GO_TEST_QUEUE,
    GO_TEST_TASK_HANDLER_BUNDLE,
)
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    ArtifactRef,
    assert_later_parses_probe_nothing,
    get_routed_stub_tasks,
    get_task_handler_artifacts,
    get_task_handler_bindings,
)

_EXAMPLE_ARTIFACT = ArtifactRef(GO_SDK_TASK_HANDLER_BUNDLE, GO_SDK_BUNDLE_NAME)
_HANDLERS_A = ArtifactRef(GO_TEST_TASK_HANDLER_BUNDLE, "handlers_a")
_HANDLERS_B = ArtifactRef(GO_TEST_TASK_HANDLER_BUNDLE, "handlers_b")


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


def _expected_artifact(dag_id: str, task_id: str, queue: str) -> ArtifactRef:
    if queue == GO_SDK_QUEUE:
        return _EXAMPLE_ARTIFACT
    if (dag_id, task_id) == ("go_split_artifacts", "from_b"):
        return _HANDLERS_B
    return _HANDLERS_A


def test_every_routed_stub_task_is_bound_to_the_artifact_that_registers_it(
    client: AirflowClient, compose_instance, go_dynamic_dag_ids
):
    """
    Each stub task binds to the artifact that registers it.

    The example's tasks bind to its bundle, ``from_b`` to ``handlers_b`` and every other test task to
    ``handlers_a``. The Dag with a task handler problem and the Dag on the queue the Dag processor does not
    route have no binding, and the recorded artifacts are the three that were deployed.
    """
    routed = get_routed_stub_tasks(client, [GO_SDK_QUEUE, GO_TEST_QUEUE])
    expected_tasks = {
        ("simple_dag", "extract"),
        ("go_split_artifacts", "from_a"),
        ("go_split_artifacts", "from_b"),
        *((dag_id, "greet") for dag_id in go_dynamic_dag_ids),
    }
    assert expected_tasks <= set(routed), (
        f"Stub tasks the Dag processor did not serialize: {expected_tasks - set(routed)}"
    )

    bindings = get_task_handler_bindings(compose_instance)
    wrong = {
        task: (bindings.get(task), _expected_artifact(*task, queue))
        for task, queue in routed.items()
        if bindings.get(task) != _expected_artifact(*task, queue)
    }
    assert not wrong, (
        f"Stub tasks not bound to the artifact that registers them (task: bound, expected): {wrong}"
    )

    bound_dag_ids = {dag_id for dag_id, _ in bindings}
    assert not bound_dag_ids & {"go_task_handler_failures", "go_unbound_stub"}, (
        f"A Dag with a task handler problem, or on a queue the Dag processor does not route, has bindings: "
        f"{sorted(bound_dag_ids)}"
    )
    assert set(get_task_handler_artifacts(compose_instance)) == {_EXAMPLE_ARTIFACT, _HANDLERS_A, _HANDLERS_B}


def test_a_later_parse_probes_nothing(client: AirflowClient, compose_instance, airflow_logs_path):
    """
    A file parsed again probes no artifact, and no artifact is probed twice for a file.

    The Dag file with the task handler problem is among them: a later parse of it probes nothing either.
    """
    assert_later_parses_probe_nothing(
        client,
        compose_instance,
        airflow_logs_path,
        {
            "go_examples.py": "simple_dag",
            "go_test_dags.py": "go_split_artifacts",
            "go_task_handler_failures.py": "go_task_handler_failures",
        },
    )
