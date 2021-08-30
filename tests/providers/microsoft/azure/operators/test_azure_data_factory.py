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

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
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
            "poke_interval": 1,
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
            (AzureDataFactoryPipelineRunStatus.FAILED, "AirflowException"),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "AirflowException"),
        ]
    )
    @patch.object(AzureDataFactoryHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    def test_execute_wait_for_completion(self, pipeline_run_status, expected_status, mock_run_pipeline):
        operator = AzureDataFactoryRunPipelineOperator(**self.config)

        assert operator.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert operator.resource_group_name == self.config["resource_group_name"]
        assert operator.factory_name == self.config["factory_name"]
        assert operator.poke_interval == self.config["poke_interval"]
        assert operator.wait_for_completion

        with patch.object(AzureDataFactoryHook, "get_pipeline_run") as mock_get_pipeline_run:
            mock_get_pipeline_run.return_value = TestAzureDataFactoryRunPipelineOperator.create_pipeline_run(
                pipeline_run_status
            )

            assert mock_get_pipeline_run.return_value.status == pipeline_run_status

            if not expected_status:
                # A successful operator execution should not return any values.
                assert not operator.execute(context=self.mock_context)
            elif expected_status == "AirflowException":
                if mock_get_pipeline_run.return_value.status == AzureDataFactoryPipelineRunStatus.CANCELLED:
                    error_message = (
                        f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has been "
                        f"{pipeline_run_status.lower()}."
                    )
                else:
                    error_message = (
                        f"Pipeline run {PIPELINE_RUN_RESPONSE['run_id']} has "
                        f"{pipeline_run_status.lower()}."
                    )
                # The operator should fail if the pipeline run fails or is canceled.
                with pytest.raises(AirflowException, match=error_message):
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

            mock_get_pipeline_run.assert_called_once_with(
                run_id=mock_run_pipeline.return_value.run_id,
                factory_name=self.config["factory_name"],
                resource_group_name=self.config["resource_group_name"],
            )

    @patch.object(AzureDataFactoryHook, "run_pipeline", return_value=MagicMock(**PIPELINE_RUN_RESPONSE))
    def test_execute_no_wait_for_completion(self, mock_run_pipeline):
        operator = AzureDataFactoryRunPipelineOperator(wait_for_completion=False, **self.config)

        assert operator.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert operator.pipeline_name == self.config["pipeline_name"]
        assert operator.resource_group_name == self.config["resource_group_name"]
        assert operator.factory_name == self.config["factory_name"]
        assert operator.poke_interval == self.config["poke_interval"]
        assert not operator.wait_for_completion

        with patch.object(AzureDataFactoryHook, "get_pipeline_run") as mock_get_pipeline_run:
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

            # Checking the pipeline run status should _not_ be called when ``wait_for_completion`` is False.
            mock_get_pipeline_run.assert_not_called()
