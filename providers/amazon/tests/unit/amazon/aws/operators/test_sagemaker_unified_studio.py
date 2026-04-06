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

from unittest.mock import patch

import pytest

from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import (
    SageMakerNotebookOperator,
)
from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio import (
    SageMakerNotebookJobTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException


class TestSageMakerNotebookOperator:
    def test_init(self):
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={
                "notebook_path": "tests/amazon/aws/operators/test_notebook.ipynb",
            },
            output_config={"output_format": "ipynb"},
        )

        assert operator.task_id == "test_id"
        assert operator.input_config == {
            "notebook_path": "tests/amazon/aws/operators/test_notebook.ipynb",
        }
        assert operator.output_config == {"output_format": "ipynb"}

    def test_init_with_domain_id_project_id_domain_region(self):
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "path/to/notebook.ipynb"},
            domain_id="dzd-example123456",
            project_id="proj-example123456",
            domain_region="us-east-1",
        )

        assert operator.domain_id == "dzd-example123456"
        assert operator.project_id == "proj-example123456"
        assert operator.domain_region == "us-east-1"

    def test_init_domain_params_default_to_none(self):
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "path/to/notebook.ipynb"},
        )

        assert operator.domain_id is None
        assert operator.project_id is None
        assert operator.domain_region is None

    def test_only_required_params_init(self):
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={
                "notebook_path": "tests/amazon/aws/operators/test_notebook.ipynb",
            },
        )
        assert isinstance(operator, SageMakerNotebookOperator)

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_passes_domain_id_project_id_domain_region_to_hook(self, mock_notebook_hook):
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.start_notebook_execution.return_value = {
            "execution_id": "123456",
            "executionType": "test",
        }

        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "path/to/notebook.ipynb"},
            domain_id="dzd-example123456",
            project_id="proj-example123456",
            domain_region="us-west-2",
        )

        operator.execute({})

        mock_notebook_hook.assert_called_once_with(
            domain_id="dzd-example123456",
            project_id="proj-example123456",
            input_config={"input_path": "path/to/notebook.ipynb"},
            output_config={"output_formats": ["NOTEBOOK"]},
            execution_name="test_id",
            domain_region="us-west-2",
            compute={},
            termination_condition={},
            tags={},
            waiter_delay=10,
            waiter_max_attempts=1440,
        )

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_passes_none_domain_params_to_hook(self, mock_notebook_hook):
        """When domain_id/project_id/domain_region are omitted, None is forwarded so the SDK resolves them."""
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.start_notebook_execution.return_value = {
            "execution_id": "123456",
            "executionType": "test",
        }

        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "path/to/notebook.ipynb"},
        )

        operator.execute({})

        call_kwargs = mock_notebook_hook.call_args[1]
        assert call_kwargs["domain_id"] is None
        assert call_kwargs["project_id"] is None
        assert call_kwargs["domain_region"] is None

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_success(self, mock_notebook_hook):  # Mock the NotebookHook and its execute method
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.start_notebook_execution.return_value = {
            "execution_id": "123456",
            "executionType": "test",
        }

        # Create the operator
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "test_input_path"},
            output_config={"output_uri": "test_output_uri", "output_format": "ipynb"},
        )

        # Execute the operator
        operator.execute({})
        mock_hook_instance.start_notebook_execution.assert_called_once_with()
        mock_hook_instance.wait_for_execution_completion.assert_called_once_with("123456", {})

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_failure_missing_input_config(self, mock_notebook_hook):
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={},
            output_config={"output_uri": "test_output_uri", "output_format": "ipynb"},
        )

        with pytest.raises(AirflowException, match="input_config is required"):
            operator.execute({})

        mock_notebook_hook.assert_not_called()

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_failure_missing_input_path(self, mock_notebook_hook):
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"invalid_key": "test_input_path"},
            output_config={"output_uri": "test_output_uri", "output_format": "ipynb"},
        )

        with pytest.raises(AirflowException, match="input_path is a required field in the input_config"):
            operator.execute({})

        mock_notebook_hook.assert_not_called()

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_with_wait_for_completion(self, mock_notebook_hook):
        # Mock the execute and job_completion methods of NotebookHook
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.start_notebook_execution.return_value = {
            "execution_id": "123456",
            "executionType": "test",
        }
        mock_hook_instance.wait_for_execution_completion.return_value = {"Status": "COMPLETED"}

        # Create the operator with wait_for_completion set to True
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "test_input_path"},
            output_config={"output_uri": "test_output_uri", "output_format": "ipynb"},
            wait_for_completion=True,
        )
        # Execute the operator
        operator.execute({})

        # Verify that execute and wait_for_execution_completion methods are called
        mock_hook_instance.start_notebook_execution.assert_called_once_with()
        mock_hook_instance.wait_for_execution_completion.assert_called_once_with("123456", {})

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    @patch.object(SageMakerNotebookOperator, "defer")
    def test_execute_with_deferrable(self, mock_defer, mock_notebook_hook):
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.start_notebook_execution.return_value = {
            "execution_id": "123456",
            "executionType": "test",
        }

        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "test_input_path"},
            output_config={"output_format": "ipynb"},
            deferrable=True,
        )

        operator.execute({})

        mock_hook_instance.start_notebook_execution.assert_called_once_with()
        mock_defer.assert_called_once()
        trigger_call = mock_defer.call_args[1]["trigger"]
        assert isinstance(trigger_call, SageMakerNotebookJobTrigger)
        assert trigger_call.execution_id == "123456"
        assert trigger_call.execution_name == "test_id"
        assert trigger_call.waiter_delay == 10
        mock_hook_instance.wait_for_execution_completion.assert_not_called()

    @patch("airflow.providers.amazon.aws.operators.sagemaker_unified_studio.SageMakerNotebookHook")
    def test_execute_without_wait_for_completion(self, mock_notebook_hook):
        # Mock the execute method of NotebookHook
        mock_hook_instance = mock_notebook_hook.return_value
        mock_hook_instance.start_notebook_execution.return_value = {
            "execution_id": "123456",
            "executionType": "test",
        }

        # Create the operator with wait_for_completion set to False
        operator = SageMakerNotebookOperator(
            task_id="test_id",
            input_config={"input_path": "test_input_path"},
            output_config={"output_uri": "test_output_uri", "output_format": "ipynb"},
            wait_for_completion=False,
        )

        # Execute the operator
        operator.execute({})

        # Verify that execute and wait_for_execution_completion methods are called
        mock_hook_instance.start_notebook_execution.assert_called_once_with()
        mock_hook_instance.wait_for_execution_completion.assert_not_called()
