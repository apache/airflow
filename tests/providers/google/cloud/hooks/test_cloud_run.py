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

from unittest import mock
from unittest.mock import PropertyMock
from uuid import UUID

import pytest

from airflow.providers.google.cloud.hooks.cloud_run import CloudRunJobHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

TASK_ID = "test-cloud-run-operator"
JOB_NAME = "test-cloud-run-job"
CLOUD_RUN_JOBS_STRING = "airflow.providers.google.cloud.hooks.cloud_run.%s"
TEST_PROJECT = "test-project"
MOCK_UUID = UUID("cf4a56d2-8101-4217-b027-2af6216feb48")
MOCK_UUID_PREFIX = str(MOCK_UUID)[:8]
EXECUTION_ID = f"{JOB_NAME}-{MOCK_UUID_PREFIX}"


@mock.patch(CLOUD_RUN_JOBS_STRING % "_CloudRunJobExecutionController")
@mock.patch(CLOUD_RUN_JOBS_STRING % "CloudRunJobHook.get_conn")
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
    mock_base_gcp_hook_no_default_project_id,
)
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
    new_callable=PropertyMock,
    return_value=None,
)
@pytest.mark.parametrize("wait_until_finished", [True, False])
def test_execute_cloud_run_job(mock_hook, mock_conn, mock_controller, wait_until_finished):
    execute_method = mock_conn.return_value.namespaces.return_value.jobs.return_value.run
    execute_method.return_value.execute.return_value = {
        "apiVersion": "run.googleapis.com/v1",
        "kind": "Execution",
        "metadata": {"name": EXECUTION_ID},
    }

    hook = CloudRunJobHook(gcp_conn_id="google_cloud_default", wait_until_finished=wait_until_finished)
    hook.execute_cloud_run_job(job_name=JOB_NAME, project_id=TEST_PROJECT)
    execute_method.assert_called_once_with(name=f"namespaces/{TEST_PROJECT}/jobs/{JOB_NAME}")

    mock_controller.assert_called_once_with(
        cloud_run=mock_conn.return_value,
        project_id=TEST_PROJECT,
        execution_id=EXECUTION_ID,
        num_retries=hook.num_retries,
        region=hook.DEFAULT_CLOUD_RUN_REGION,
        wait_until_finished=wait_until_finished,
    )

    mock_controller.return_value.wait_for_done.assert_called_once()
