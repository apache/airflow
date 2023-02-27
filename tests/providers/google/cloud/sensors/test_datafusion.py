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
from parameterized.parameterized import parameterized

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.google.cloud.hooks.datafusion import PipelineStates
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor

LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE_URL = "http://datafusion.instance.com"
PIPELINE_NAME = "shrubberyPipeline"
PIPELINE_ID = "test_pipeline_id"
PROJECT_ID = "test_project_id"
GCP_CONN_ID = "test_conn_id"
DELEGATE_TO = "test_delegate_to"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
FAILURE_STATUSES = {"FAILED"}


class TestCloudDataFusionPipelineStateSensor:
    @parameterized.expand(
        [
            (PipelineStates.COMPLETED, PipelineStates.COMPLETED, True),
            (PipelineStates.COMPLETED, PipelineStates.RUNNING, False),
        ]
    )
    @mock.patch("airflow.providers.google.cloud.sensors.datafusion.DataFusionHook")
    def test_poke(self, expected_status, current_status, sensor_return, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}

        task = CloudDataFusionPipelineStateSensor(
            task_id="test_task_id",
            pipeline_name=PIPELINE_NAME,
            pipeline_id=PIPELINE_ID,
            project_id=PROJECT_ID,
            expected_statuses={expected_status},
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_pipeline_workflow.return_value = {"status": current_status}
        result = task.poke(mock.MagicMock())

        assert sensor_return == result

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

    @mock.patch("airflow.providers.google.cloud.sensors.datafusion.DataFusionHook")
    def test_assertion(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}

        task = CloudDataFusionPipelineStateSensor(
            task_id="test_task_id",
            pipeline_name=PIPELINE_NAME,
            pipeline_id=PIPELINE_ID,
            project_id=PROJECT_ID,
            expected_statuses={PipelineStates.COMPLETED},
            failure_statuses=FAILURE_STATUSES,
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        with pytest.raises(
            AirflowException,
            match=f"Pipeline with id '{PIPELINE_ID}' state is: FAILED. Terminating sensor...",
        ):
            mock_hook.return_value.get_pipeline_workflow.return_value = {"status": "FAILED"}
            task.poke(mock.MagicMock())

    @mock.patch("airflow.providers.google.cloud.sensors.datafusion.DataFusionHook")
    def test_not_found_exception(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"apiEndpoint": INSTANCE_URL}
        mock_hook.return_value.get_pipeline_workflow.side_effect = AirflowNotFoundException()

        task = CloudDataFusionPipelineStateSensor(
            task_id="test_task_id",
            pipeline_name=PIPELINE_NAME,
            pipeline_id=PIPELINE_ID,
            project_id=PROJECT_ID,
            expected_statuses={PipelineStates.COMPLETED},
            failure_statuses=FAILURE_STATUSES,
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        with pytest.raises(
            AirflowException,
            match="Specified Pipeline ID was not found.",
        ):
            task.poke(mock.MagicMock())
