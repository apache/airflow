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

The Dag processor probes the packed bundle for the task handlers it registers and checks each Dag file's
stub tasks against them at parse time, against the artifact the worker's own pick would run. These tests
read what it recorded in its parse logs and the REST API. Nothing here triggers or runs a task.

TypeScript is presence only (no bound arguments are checked yet), so there is no failure Dag here: both
Dag files are expected to import cleanly.
"""

from __future__ import annotations

import pytest

from airflow_e2e_tests.constants import TS_SDK_BUNDLE_FILE, TS_SDK_TASK_HANDLER_BUNDLE
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    ArtifactRef,
    assert_every_file_is_probed_once,
    assert_later_parses_probe_nothing_new,
    get_import_errors,
)

_ARTIFACT = ArtifactRef(TS_SDK_TASK_HANDLER_BUNDLE, TS_SDK_BUNDLE_FILE)


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


def test_every_lang_sdk_dag_file_is_probed_once(airflow_logs_path):
    """Both example Dag files have a probe record of the one packed bundle, whatever it is named."""
    assert_every_file_is_probed_once(
        airflow_logs_path,
        {
            "typescript_example.py": {_ARTIFACT},
            "typescript_taskflow_example.py": {_ARTIFACT},
        },
    )


def test_a_later_parse_probes_nothing_new(client: AirflowClient, airflow_logs_path):
    """A file parsed again probes no artifact its first parse did not, and none twice."""
    assert_later_parses_probe_nothing_new(
        client,
        airflow_logs_path,
        {
            "typescript_example.py": "typescript_example",
            "typescript_taskflow_example.py": "typescript_taskflow_example",
        },
    )


def test_no_dag_file_has_an_import_error(client: AirflowClient, lang_sdk_dag_files):
    """Every Lang-SDK Dag file imports: the presence check finds the bundle and its task handlers."""
    assert get_import_errors(client, lang_sdk_dag_files) == {}
