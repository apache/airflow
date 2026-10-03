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
E2E tests for what the Dag processor does with the Java stub tasks when it parses a Dag file.

Run with::

    E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/java_sdk_tests/test_java_sdk_task_handler_parse.py -xvs

The Dag processor asks each handler JAR which task handlers it registers, binds each stub task to the JAR
that registers it, and records the bindings. These tests read what it recorded from the metadata database,
and the log it wrote when it parsed each Dag file. Nothing here runs a task.
"""

from __future__ import annotations

import pytest

from airflow_e2e_tests.constants import (
    JAVA_SDK_QUEUE,
    JAVA_SDK_TASK_HANDLER_BUNDLE,
    JAVA_TEST_QUEUE,
    JAVA_TEST_TASK_HANDLER_BUNDLE,
    SCALA_SPARK_QUEUE,
    SCALA_SPARK_TASK_HANDLER_BUNDLE,
)
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    assert_later_parses_probe_nothing,
    get_import_errors,
    get_routed_stub_tasks,
    get_task_handler_artifacts,
    get_task_handler_bindings,
)

# Each queue is served by its own coordinator, which reads its own Dag bundle of one handler JAR.
_BUNDLE_BY_QUEUE = {
    JAVA_SDK_QUEUE: JAVA_SDK_TASK_HANDLER_BUNDLE,
    SCALA_SPARK_QUEUE: SCALA_SPARK_TASK_HANDLER_BUNDLE,
    JAVA_TEST_QUEUE: JAVA_TEST_TASK_HANDLER_BUNDLE,
}


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


def test_every_routed_stub_task_is_bound_to_the_jar_of_its_queue(client: AirflowClient, compose_instance):
    """All the stub tasks of a queue bind to the one JAR of the Dag bundle that its coordinator reads."""
    routed = get_routed_stub_tasks(client, list(_BUNDLE_BY_QUEUE))
    bindings = get_task_handler_bindings(compose_instance)

    jars_by_queue: dict[str, set[str]] = {queue: set() for queue in _BUNDLE_BY_QUEUE}
    for task, queue in routed.items():
        artifact = bindings.get(task)
        assert artifact is not None, f"The stub task {task} on queue {queue!r} is not bound."
        assert artifact.bundle_name == _BUNDLE_BY_QUEUE[queue], (
            f"The stub task {task} on queue {queue!r} is bound to {artifact}, not a JAR of "
            f"{_BUNDLE_BY_QUEUE[queue]!r}."
        )
        assert artifact.rel_path.endswith(".jar"), f"The stub task {task} is bound to {artifact}, not a JAR."
        jars_by_queue[queue].add(artifact.rel_path)

    assert all(len(jars) == 1 for jars in jars_by_queue.values()), (
        f"Each queue's stub tasks bind to one JAR (queue: JARs): {jars_by_queue}"
    )
    assert {artifact.bundle_name for artifact in get_task_handler_artifacts(compose_instance)} == set(
        _BUNDLE_BY_QUEUE.values()
    )
    assert len(get_task_handler_artifacts(compose_instance)) == len(_BUNDLE_BY_QUEUE)


def test_a_later_parse_probes_nothing(client: AirflowClient, compose_instance, airflow_logs_path):
    """A file parsed again probes no artifact, and no artifact is probed twice for a file."""
    assert_later_parses_probe_nothing(
        client,
        compose_instance,
        airflow_logs_path,
        {
            "java_examples.py": "java_annotation_example",
            "scala_spark_examples.py": "scala_spark_example",
            "java_test_dags.py": "java_variable_write",
        },
    )


def test_no_dag_file_has_an_import_error(client: AirflowClient):
    """Every Dag file of the Dags folder imports: each stub task has exactly one handler that fits it."""
    assert get_import_errors(client) == {}
