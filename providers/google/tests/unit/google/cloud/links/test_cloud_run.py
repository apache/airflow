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

from airflow.providers.google.cloud.links.cloud_run import CloudRunJobLoggingLink
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

TEST_LOG_URI = (
    "https://console.cloud.google.com/run/jobs/logs?project=test-project&region=test-region&job=test-job"
)


class TestCloudRunJobLoggingLink:
    def test_class_attributes(self):
        assert CloudRunJobLoggingLink.key == "log_uri"
        assert CloudRunJobLoggingLink.name == "Cloud Run Job Logging"
        assert CloudRunJobLoggingLink.format_str == "{log_uri}"

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock()

        CloudRunJobLoggingLink.persist(
            context=mock_context,
            log_uri=TEST_LOG_URI,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key=CloudRunJobLoggingLink.key,
            value={"log_uri": TEST_LOG_URI},
        )

    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
        link = CloudRunJobLoggingLink()
        ti = create_task_instance_of_operator(
            CloudRunExecuteJobOperator,
            dag_id="test_cloud_run_job_logging_link_dag",
            task_id="test_cloud_run_job_logging_link_task",
            job_name="test-job",
            project_id="test-project",
            region="test-region",
        )
        session.add(ti)
        session.commit()

        link.persist(context={"ti": ti, "task": ti.task}, log_uri=TEST_LOG_URI)

        if mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={"log_uri": TEST_LOG_URI},
            )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == TEST_LOG_URI
