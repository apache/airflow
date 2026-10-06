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

The Dag processor probes each handler JAR for the task handlers it registers and checks the Dag file's stub
tasks against them at parse time, against the artifact the worker's own pick would run. These tests read
what it recorded in its parse logs and the REST API. Nothing here triggers or runs a task.

A JAR's name comes from the Gradle build, so it is not pinned here: a file's artifact is asserted by its Dag
bundle name and that it is a single ``.jar``.
"""

from __future__ import annotations

import pytest

from airflow_e2e_tests.constants import (
    DAGS_BUNDLE_NAME,
    JAVA_SDK_TASK_HANDLER_BUNDLE,
    JAVA_TEST_TASK_HANDLER_BUNDLE,
    SCALA_SPARK_TASK_HANDLER_BUNDLE,
)
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    ArtifactRef,
    assert_later_parses_probe_nothing_new,
    count_probes,
    get_import_errors,
    read_parse_attempts,
)

_FAILURE_FILE = "java_task_handler_failures.py"

# Each Lang-SDK Dag file of this mode, with the Dag bundle its one JAR is probed from.
_BUNDLE_BY_FILE = {
    "java_examples.py": JAVA_SDK_TASK_HANDLER_BUNDLE,
    "scala_spark_examples.py": SCALA_SPARK_TASK_HANDLER_BUNDLE,
    "java_test_dags.py": JAVA_TEST_TASK_HANDLER_BUNDLE,
    _FAILURE_FILE: JAVA_TEST_TASK_HANDLER_BUNDLE,
}


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


def _assert_probed_one_jar_in(file: str, bundle: str, airflow_logs_path) -> ArtifactRef:
    """Assert *file*'s parse log holds a single JAR's probe record, in *bundle*, with no duplicate."""
    attempts = read_parse_attempts(airflow_logs_path, file)
    assert attempts, f"{file} was not parsed."
    probed: set[ArtifactRef] = set()
    for attempt in attempts:
        counts = count_probes(attempt)
        duplicated = {ref: count for ref, count in counts.items() if count > 1}
        assert not duplicated, f"A parse of {file} probed an artifact more than once: {duplicated}"
        probed |= set(counts)
    assert len(probed) == 1, f"{file}: expected a single probed JAR, got {probed}"
    artifact = next(iter(probed))
    assert artifact.bundle_name == bundle, f"{file} probed {artifact}, not a JAR of {bundle!r}"
    assert artifact.rel_path.endswith(".jar"), f"{file} probed {artifact}, not a JAR"
    return artifact


def test_every_lang_sdk_dag_file_is_probed_once(airflow_logs_path):
    """Every stub Dag file has a probe record of one JAR, in the Dag bundle its queue's coordinator reads."""
    for file, bundle in _BUNDLE_BY_FILE.items():
        _assert_probed_one_jar_in(file, bundle, airflow_logs_path)


def test_problems_are_one_import_error_of_their_dag_file(
    client: AirflowClient, airflow_logs_path, lang_sdk_dag_files
):
    """
    A missing handler and a positional count mismatch are one import error, one line each.

    The error is the one of the file, and lists each problem on a line of its own, sorted by task id
    ("not_registered" before "takes_two_numbers"). Its Dag is serialized but marked as having import errors.
    """
    artifact = _assert_probed_one_jar_in(_FAILURE_FILE, JAVA_TEST_TASK_HANDLER_BUNDLE, airflow_logs_path)
    expected = "\n".join(
        [
            f"Stub tasks in {_FAILURE_FILE} do not match their task handlers:",
            f"- Dag 'java_task_handler_failures', task 'not_registered': {artifact.rel_path!r} in Dag "
            f"bundle {artifact.bundle_name!r} registers no task handler for it",
            f"- Dag 'java_task_handler_failures', task 'takes_two_numbers' ({artifact.rel_path!r} in Dag "
            f"bundle {artifact.bundle_name!r}): passes 3 arguments, the task handler takes 2",
        ]
    )
    assert get_import_errors(client, [_FAILURE_FILE]).get(_FAILURE_FILE) == expected

    # Scoped to this mode's own Dag files: the Dags folder also holds stock example Dags (such as
    # example_event_driven.py) that may fail to import for reasons that have nothing to do with this check.
    failing_dags = client.list_dags(bundle_name=DAGS_BUNDLE_NAME, exclude_stale=False, has_import_errors=True)
    failing_lang_sdk_dags = [
        dag["dag_id"] for dag in failing_dags if dag["relative_fileloc"] in lang_sdk_dag_files
    ]
    assert failing_lang_sdk_dags == ["java_task_handler_failures"]


def test_a_later_parse_probes_nothing_new(client: AirflowClient, airflow_logs_path):
    """A file parsed again probes no artifact its first parse did not, and none twice."""
    assert_later_parses_probe_nothing_new(
        client,
        airflow_logs_path,
        {
            "java_examples.py": "java_annotation_example",
            "scala_spark_examples.py": "scala_spark_example",
            "java_test_dags.py": "java_variable_write",
            _FAILURE_FILE: "java_task_handler_failures",
        },
    )


def test_only_the_failing_dag_file_has_an_import_error(client: AirflowClient, lang_sdk_dag_files):
    """No other Lang-SDK Dag file fails to import, so no fixture hides behind the expected error."""
    assert set(get_import_errors(client, lang_sdk_dag_files)) == {_FAILURE_FILE}
