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

import unittest
from unittest.mock import MagicMock, patch

import pytest
from parameterized import parameterized

from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

PIPELINE_RUN_RESPONSE = {"additional_properties": {}, "run_id": "run_id"}


class TestAzureDataFactoryRunPipelineOperator(unittest.TestCase):
    def setUp(self):
        self.mock_ti = MagicMock()
        self.mock_context = {"ti": self.mock_ti}
        self.config = {
            "task_id": "run_pipeline_op",
            "azure_data_factory_conn_id": "azure_data_factory_test",
            "pipeline_name": "pipeline1",
            "resource_group_name": "resource-group-name",
            "factory_name": "factory-name",
            "check_interval": 1,
            "timeout": 3,
        }

    @staticmethod
    def create_pipeline_run(status: str):
        """Helper function to create a mock pipeline run with a given execution status."""

        run = MagicMock()
        run.status = status

        return run

    @parameterized.expand(
        [
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, None),
            (AzureDataFactoryPipelineRunStatus.FAILED, "exception"),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "exception"),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, "timeout"),
            (AzureDataFactoryPipelineRunStatus.QUEUED, "timeout"),
            (AzureDataFactoryPipelineRunStatus.CANCELING, "timeout"),
        ]
    )
    @patch.object(AzureDataFactoryHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    def test_execute_wait_for_termination(self, pipeline_run_status, expected_output, mock_run_pipeline):
        operator = AzureDataFactoryRunPipelineOperator(**self.config)

        assert operator.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert operator.resource_group_name == self.config["resource_group_name"]
        assert operator.factory_name == self.config["factory_name"]
        assert operator.check_interval == self.config["check_interval"]
        assert operator.timeout == self.config["timeout"]
        assert operator.wait_for_termination

        with patch.object(AzureDataFactoryHook, "get_pipeline_run") as mock_get_pipeline_run:
            mock_get_pipeline_run.return_value = TestAzureDataFactoryRunPipelineOperator.create_pipeline_run(
                pipeline_run_status
            )

            if not expected_output:
                # A successful operator execution should not return any values.
                assert not operator.execute(context=self.mock_context)
            elif expected_output == "exception":
                # The operator should fail if the pipeline run fails or is canceled.
                with pytest.raises(
                    AzureDataFactoryPipelineRunException,
                    match=f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has failed or has been cancelled.",
                ):
                    operator.execute(context=self.mock_context)
            else:
                # Demonstrating the operator timing out after surpassing the configured timeout value.
                with pytest.raises(
                    AzureDataFactoryPipelineRunException,
                    match=(
                        f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has not reached a terminal status "
                        f"after {self.config['timeout']} seconds."
                    ),
                ):
                    operator.execute(context=self.mock_context)

            # Check the ``run_id`` attr is assigned after executing the pipeline.
            assert operator.run_id == PIPELINE_RUN_RESPONSE["run_id"]

            # Check to ensure an `XCom` is pushed regardless of pipeline run result.
            self.mock_ti.xcom_push.assert_called_once_with(
                key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"]
            )

            mock_run_pipeline.assert_called_once_with(
                pipeline_name=self.config["pipeline_name"],
                resource_group_name=self.config["resource_group_name"],
                factory_name=self.config["factory_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                start_from_failure=None,
                parameters=None,
            )

            if pipeline_run_status in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES:
                mock_get_pipeline_run.assert_called_once_with(
                    run_id=mock_run_pipeline.return_value.run_id,
                    factory_name=self.config["factory_name"],
                    resource_group_name=self.config["resource_group_name"],
                )
            else:
                # When the pipeline run status is not in a terminal status or "Succeeded", the operator will
                # continue to call ``get_pipeline_run()`` until a ``timeout`` number of seconds has passed
                # (3 seconds for this test).  Therefore, there should be 4 calls of this function: one
                # initially and 3 for each check done at a 1 second interval.
                assert mock_get_pipeline_run.call_count == 4

                mock_get_pipeline_run.assert_called_with(
                    run_id=mock_run_pipeline.return_value.run_id,
                    factory_name=self.config["factory_name"],
                    resource_group_name=self.config["resource_group_name"],
                )

    @patch.object(AzureDataFactoryHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    def test_execute_no_wait_for_termination(self, mock_run_pipeline):
        operator = AzureDataFactoryRunPipelineOperator(wait_for_termination=False, **self.config)

        assert operator.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert operator.resource_group_name == self.config["resource_group_name"]
        assert operator.factory_name == self.config["factory_name"]
        assert operator.check_interval == self.config["check_interval"]
        assert not operator.wait_for_termination

        with patch.object(AzureDataFactoryHook, "get_pipeline_run", autospec=True) as mock_get_pipeline_run:
            operator.execute(context=self.mock_context)

            # Check the ``run_id`` attr is assigned after executing the pipeline.
            assert operator.run_id == PIPELINE_RUN_RESPONSE["run_id"]

            # Check to ensure an `XCom` is pushed regardless of pipeline run result.
            self.mock_ti.xcom_push.assert_called_once_with(
                key="run_id", value=PIPELINE_RUN_RESPONSE["run_id"]
            )

            mock_run_pipeline.assert_called_once_with(
                pipeline_name=self.config["pipeline_name"],
                resource_group_name=self.config["resource_group_name"],
                factory_name=self.config["factory_name"],
                reference_pipeline_run_id=None,
                is_recovery=None,
                start_activity_name=None,
                start_from_failure=None,
                parameters=None,
            )

            # Checking the pipeline run status should _not_ be called when ``wait_for_termination`` is False.
            mock_get_pipeline_run.assert_not_called()
