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

"""This module contains the Amazon SageMaker Unified Studio Notebook hook."""

from __future__ import annotations

import time
from functools import cached_property

from airflow.providers.amazon.aws.hooks._sagemaker_unified_studio_client import (
    SageMakerUnifiedStudioExecutionClient,
)
from airflow.providers.common.compat.sdk import AirflowException, BaseHook


class SageMakerNotebookHook(BaseHook):
    """
    Interact with Sagemaker Unified Studio Workflows.

    This hook provides a wrapper around the Sagemaker Workflows Notebook Execution API.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio import SageMakerNotebookHook

        notebook_hook = SageMakerNotebookHook(
            execution_name="notebook_execution",
            domain_id="dzd-example123456",
            project_id="example123456",
            input_config={"input_path": "path/to/notebook.ipynb", "input_params": {"param1": "value1"}},
            output_config={"output_uri": "folder/output/location/prefix", "output_formats": "NOTEBOOK"},
            domain_region="us-east-1",
            waiter_delay=10,
            waiter_max_attempts=1440,
        )

    :param execution_name: The name of the notebook job to be executed, this is same as task_id.
    :param domain_id: The domain ID for Amazon SageMaker Unified Studio. Optional - if not provided,
        it will be resolved from the environment.
    :param project_id: The project ID for Amazon SageMaker Unified Studio. Optional - if not provided,
        it will be resolved from the environment.
    :param input_config: Configuration for the input file.
        Example: {'input_path': 'folder/input/notebook.ipynb', 'input_params': {'param1': 'value1'}}
    :param output_config: Configuration for the output format. It should include an output_formats
        parameter to specify the output format.
        Example: {'output_formats': ['NOTEBOOK']}
    :param domain_region: The AWS region for the domain. If not provided, the default AWS region 'us-east-1'
        will be used.
    :param compute: compute configuration to use for the notebook execution. This is a required
        attribute if the execution is on a remote compute.
        Example::

            {
                "instance_type": "ml.c5.xlarge",
                "image_details": {
                    "image_name": "sagemaker-distribution-prod",
                    "image_version": "3",
                    "ecr_uri": "123456123456.dkr.ecr.us-west-2.amazonaws.com/ImageName:latest",
                },
            }

    :param termination_condition: conditions to match to terminate the remote execution.
        Example: ``{"MaxRuntimeInSeconds": 3600}``
    :param tags: tags to be associated with the remote execution runs.
        Example: ``{"md_analytics": "logs"}``
    :param waiter_delay: Interval in seconds to check the task execution status.
    :param waiter_max_attempts: Number of attempts to wait before returning FAILED.
    """

    def __init__(
        self,
        execution_name: str,
        input_config: dict | None = None,
        domain_id: str | None = None,
        project_id: str | None = None,
        output_config: dict | None = None,
        domain_region: str | None = None,
        compute: dict | None = None,
        termination_condition: dict | None = None,
        tags: dict | None = None,
        waiter_delay: int = 10,
        waiter_max_attempts: int = 1440,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.execution_name = execution_name
        self.domain_id = domain_id
        self.project_id = project_id
        self.domain_region = domain_region
        self.input_config = input_config or {}
        self.output_config = output_config or {"output_formats": ["NOTEBOOK"]}
        self.compute = compute
        self.termination_condition = termination_condition or {}
        self.tags = tags or {}
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    @cached_property
    def _execution_client(self) -> SageMakerUnifiedStudioExecutionClient:
        """Get the execution client."""
        return SageMakerUnifiedStudioExecutionClient(
            domain_id=self.domain_id,
            project_id=self.project_id,
            domain_region=self.domain_region,
        )

    def _format_start_execution_input_config(self) -> dict:
        """Format the input configuration for start_execution."""
        return {
            "notebook_config": {
                "input_path": self.input_config.get("input_path"),
                "input_parameters": self.input_config.get("input_params"),
            },
        }

    def _format_start_execution_output_config(self) -> dict:
        """Format the output configuration for start_execution."""
        output_formats = self.output_config.get("output_formats")
        return {
            "notebook_config": {
                "output_formats": output_formats,
            }
        }

    def start_notebook_execution(self) -> dict:
        """
        Start a notebook execution.

        :return: Execution details including execution_id
        """
        input_config = self._format_start_execution_input_config()
        output_config = self._format_start_execution_output_config()

        return self._execution_client.start_execution(
            execution_name=self.execution_name,
            input_config=input_config,
            output_config=output_config,
            compute=self.compute,
            termination_condition=self.termination_condition,
            tags=self.tags,
        )

    def get_execution(self, execution_id: str) -> dict:
        """
        Get details of a specific execution.

        :param execution_id: The unique identifier of the execution
        :return: Execution details including status, times, and error info
        """
        return self._execution_client.get_execution(execution_id)

    def get_execution_status(self, execution_id: str) -> str:
        """
        Get the status of a specific execution.

        :param execution_id: The unique identifier of the execution
        :return: The execution status string
        """
        execution = self.get_execution(execution_id)
        return execution["status"]

    def wait_for_execution_completion(self, execution_id: str, context: dict | None) -> dict:
        """
        Wait for an execution to complete.

        :param execution_id: The unique identifier of the execution
        :param context: The Airflow context for XCom operations
        :return: Dict with Status and ExecutionId
        """
        wait_attempts = 0
        while wait_attempts < self.waiter_max_attempts:
            wait_attempts += 1
            time.sleep(self.waiter_delay)
            response = self.get_execution(execution_id=execution_id)
            error_message = response.get("error_details", {}).get("error_message")
            status = response["status"]
            if "files" in response:
                self._set_xcom_files(response["files"], context)
            if "s3_path" in response:
                self._set_xcom_s3_path(response["s3_path"], context)

            ret = self._handle_state(execution_id, status, error_message)
            if ret:
                return ret

        # If timeout, handle state FAILED with timeout message
        return self._handle_state(execution_id, "FAILED", "Execution timed out")

    def _set_xcom_files(self, files: list, context: dict | None) -> None:
        """Push file information to XCom."""
        if not context:
            raise AirflowException("context is required")
        for file in files:
            context["ti"].xcom_push(
                key=f"{file['display_name']}.{file['file_format']}",
                value=file["file_path"],
            )

    def _set_xcom_s3_path(self, s3_path: str, context: dict | None) -> None:
        """Push S3 path to XCom."""
        if not context:
            raise AirflowException("context is required")
        context["ti"].xcom_push(
            key="s3_path",
            value=s3_path,
        )

    def _handle_state(self, execution_id: str, status: str, error_message: str | None) -> dict | None:
        """Handle execution state and determine if we should continue waiting."""
        finished_states = ["COMPLETED"]
        in_progress_states = ["IN_PROGRESS", "STOPPING"]

        if status in in_progress_states:
            self.log.info(
                "Execution %s is still in progress with state: %s, "
                "will check for a terminal status again in %s seconds",
                execution_id,
                status,
                self.waiter_delay,
            )
            return None

        execution_message = f"Exiting Execution {execution_id} State: {status}"
        if status in finished_states:
            self.log.info(execution_message)
            return {"Status": status, "ExecutionId": execution_id}

        self.log.error("Execution %s failed with error: %s", execution_id, error_message)
        if not error_message:
            error_message = execution_message
        raise AirflowException(error_message)
