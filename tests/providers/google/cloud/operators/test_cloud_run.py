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

import pytest

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

TASK_ID = "test-cloud-run-operator"
JOB_NAME = "test-cloud-run-job"
REGION = "us-central1"
TEST_PROJECT = "test-project"


@pytest.fixture
def operator():
    return CloudRunExecuteJobOperator(
        task_id="execute_cloud_run_job_test", job_name=JOB_NAME, region=REGION, project_id=TEST_PROJECT
    )


@mock.patch("airflow.providers.google.cloud.operators.cloud_run.CloudRunJobHook")
def test_execute(mock_cloud_run, operator):
    operator.execute(mock.MagicMock())
    mock_cloud_run.assert_called_once_with(
        gcp_conn_id="google_cloud_default",
        region=REGION,
        delegate_to=None,
        wait_until_finished=False,
        impersonation_chain=None,
    )
    mock_cloud_run.return_value.execute_cloud_run_job.assert_called_once_with(
        project_id=TEST_PROJECT,
        job_name=JOB_NAME,
        on_new_execution_callback=mock.ANY,
    )
