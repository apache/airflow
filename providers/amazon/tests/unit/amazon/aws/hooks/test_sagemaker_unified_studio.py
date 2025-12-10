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

from unittest.mock import MagicMock, call, patch

import pytest
from sagemaker_studio.models.execution import ExecutionClient

from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio import (
    SageMakerNotebookHook,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.session import create_session


class TestSageMakerNotebookHook:
    @pytest.fixture(autouse=True)
    def setup(self):
        with patch(
            "airflow.providers.amazon.aws.hooks.sagemaker_unified_studio.SageMakerStudioAPI",
            autospec=True,
        ) as mock_sdk:
            self.execution_name = "test-execution"
            self.waiter_delay = 10
            sdk_instance = mock_sdk.return_value
            sdk_instance.execution_client = MagicMock(spec=ExecutionClient)
            sdk_instance.execution_client.start_execution.return_value = {
                "execution_id": "execution_id",
                "execution_name": "execution_name",
            }
            self.hook = SageMakerNotebookHook(
                input_config={
                    "input_path": "test-data/notebook/test_notebook.ipynb",
                    "input_params": {"key": "value"},
                },
                output_config={"output_formats": ["NOTEBOOK"]},
                execution_name=self.execution_name,
                waiter_delay=self.waiter_delay,
                compute={"instance_type": "ml.c4.2xlarge"},
            )

            self.hook._sagemaker_studio = mock_sdk
            self.files = [
                {"display_name": "file1.txt", "url": "http://example.com/file1.txt"},
                {"display_name": "file2.txt", "url": "http://example.com/file2.txt"},
            ]
            self.context = {
                "ti": MagicMock(spec=TaskInstance),
            }
            self.s3Path = "S3Path"
            yield

    def test_format_input_config(self):
        expected_config = {
            "notebook_config": {
                "input_path": "test-data/notebook/test_notebook.ipynb",
                "input_parameters": {"key": "value"},
            }
        }

        config = self.hook._format_start_execution_input_config()
        assert config == expected_config

    def test_format_output_config(self):
        expected_config = {
            "notebook_config": {
                "output_formats": ["NOTEBOOK"],
            }
        }

        config = self.hook._format_start_execution_output_config()
        assert config == expected_config

    def test_format_output_config_default(self):
        no_output_config_hook = SageMakerNotebookHook(
            input_config={
                "input_path": "test-data/notebook/test_notebook.ipynb",
                "input_params": {"key": "value"},
            },
            execution_name=self.execution_name,
            waiter_delay=self.waiter_delay,
        )

        no_output_config_hook._sagemaker_studio = self.hook._sagemaker_studio
        expected_config = {"notebook_config": {"output_formats": ["NOTEBOOK"]}}

        config = no_output_config_hook._format_start_execution_output_config()
        assert config == expected_config

    def test_start_notebook_execution(self):
        self.hook._sagemaker_studio = MagicMock()
        self.hook._sagemaker_studio.execution_client = MagicMock(spec=ExecutionClient)

        self.hook._sagemaker_studio.execution_client.start_execution.return_value = {"executionId": "123456"}
        result = self.hook.start_notebook_execution()
        assert result == {"executionId": "123456"}
        self.hook._sagemaker_studio.execution_client.start_execution.assert_called_once()

    @patch("time.sleep", return_value=None)  # To avoid actual sleep during tests
    def test_wait_for_execution_completion(self, mock_sleep):
        execution_id = "123456"
        self.hook._sagemaker_studio = MagicMock()
        self.hook._sagemaker_studio.execution_client = MagicMock(spec=ExecutionClient)
        self.hook._sagemaker_studio.execution_client.get_execution.return_value = {"status": "COMPLETED"}

        result = self.hook.wait_for_execution_completion(execution_id, {})
        assert result == {"Status": "COMPLETED", "ExecutionId": execution_id}
        self.hook._sagemaker_studio.execution_client.get_execution.assert_called()
        mock_sleep.assert_called_once()

    @patch("time.sleep", return_value=None)
    def test_wait_for_execution_completion_failed(self, mock_sleep):
        execution_id = "123456"
        self.hook._sagemaker_studio = MagicMock()
        self.hook._sagemaker_studio.execution_client = MagicMock(spec=ExecutionClient)
        self.hook._sagemaker_studio.execution_client.get_execution.return_value = {
            "status": "FAILED",
            "error_details": {"error_message": "Execution failed"},
        }

        with pytest.raises(AirflowException, match="Execution failed"):
            self.hook.wait_for_execution_completion(execution_id, self.context)

    def test_handle_in_progress_state(self):
        execution_id = "123456"
        states = ["IN_PROGRESS", "STOPPING"]

        for status in states:
            result = self.hook._handle_state(execution_id, status, None)
            assert result is None

    def test_handle_finished_state(self):
        execution_id = "123456"
        states = ["COMPLETED"]

        for status in states:
            result = self.hook._handle_state(execution_id, status, None)
            assert result == {"Status": status, "ExecutionId": execution_id}

    def test_handle_failed_state(self):
        execution_id = "123456"
        status = "FAILED"
        error_message = "Execution failed"
        with pytest.raises(AirflowException, match=error_message):
            self.hook._handle_state(execution_id, status, error_message)

        status = "STOPPED"
        error_message = ""
        with pytest.raises(AirflowException, match=f"Exiting Execution {execution_id} State: {status}"):
            self.hook._handle_state(execution_id, status, error_message)

    def test_handle_unexpected_state(self):
        execution_id = "123456"
        status = "PENDING"
        error_message = f"Exiting Execution {execution_id} State: {status}"
        with pytest.raises(AirflowException, match=error_message):
            self.hook._handle_state(execution_id, status, error_message)

    @pytest.mark.db_test
    @patch(
        "airflow.providers.amazon.aws.hooks.sagemaker_unified_studio.SageMakerNotebookHook._set_xcom_files"
    )
    def test_set_xcom_files(self, mock_set_xcom_files):
        with create_session():
            self.hook._set_xcom_files(self.files, self.context)
        expected_call = call(self.files, self.context)
        mock_set_xcom_files.assert_called_once_with(*expected_call.args, **expected_call.kwargs)

    def test_set_xcom_files_negative_missing_context(self):
        with pytest.raises(AirflowException, match="context is required"):
            self.hook._set_xcom_files(self.files, {})

    @pytest.mark.db_test
    @patch(
        "airflow.providers.amazon.aws.hooks.sagemaker_unified_studio.SageMakerNotebookHook._set_xcom_s3_path"
    )
    def test_set_xcom_s3_path(self, mock_set_xcom_s3_path):
        with create_session():
            self.hook._set_xcom_s3_path(self.s3Path, self.context)
        expected_call = call(self.s3Path, self.context)
        mock_set_xcom_s3_path.assert_called_once_with(*expected_call.args, **expected_call.kwargs)

    def test_set_xcom_s3_path_negative_missing_context(self):
        with pytest.raises(AirflowException, match="context is required"):
            self.hook._set_xcom_s3_path(self.s3Path, {})
