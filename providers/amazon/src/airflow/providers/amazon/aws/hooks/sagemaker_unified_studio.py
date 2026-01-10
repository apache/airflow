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

from sagemaker_studio import ClientConfig
from sagemaker_studio.sagemaker_studio_api import SageMakerStudioAPI

from airflow.providers.amazon.aws.utils.sagemaker_unified_studio import is_local_runner
from airflow.providers.common.compat.sdk import AirflowException, BaseHook


class SageMakerNotebookHook(BaseHook):
    """
    Interact with Sagemaker Unified Studio Workflows.

    This hook provides a wrapper around the Sagemaker Workflows Notebook Execution API.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio import SageMakerNotebookHook

        notebook_hook = SageMakerNotebookHook(
            input_config={"input_path": "path/to/notebook.ipynb", "input_params": {"param1": "value1"}},
            output_config={"output_uri": "folder/output/location/prefix", "output_formats": "NOTEBOOK"},
            execution_name="notebook_execution",
            waiter_delay=10,
            waiter_max_attempts=1440,
        )

    :param execution_name: The name of the notebook job to be executed, this is same as task_id.
    :param input_config: Configuration for the input file.
        Example: {'input_path': 'folder/input/notebook.ipynb', 'input_params': {'param1': 'value1'}}
    :param output_config: Configuration for the output format. It should include an output_formats parameter to specify the output format.
        Example: {'output_formats': ['NOTEBOOK']}
    :param compute: compute configuration to use for the notebook execution. This is a required attribute
        if the execution is on a remote compute.
        Example: {
            "instance_type": "ml.m5.large",
            "volume_size_in_gb": 30,
            "volume_kms_key_id": "",
            "image_details": {
                "ecr_uri": "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-image:latest"
            },
            "container_entrypoint": ["string"]
        }
    :param termination_condition: conditions to match to terminate the remote execution.
        Example: { "MaxRuntimeInSeconds": 3600 }
    :param tags: tags to be associated with the remote execution runs.
        Example: { "md_analytics": "logs" }
    :param waiter_delay: Interval in seconds to check the task execution status.
    :param waiter_max_attempts: Number of attempts to wait before returning FAILED.
    """

    def __init__(
        self,
        execution_name: str,
        input_config: dict | None = None,
        output_config: dict | None = None,
        compute: dict | None = None,
        termination_condition: dict | None = None,
        tags: dict | None = None,
        waiter_delay: int = 10,
        waiter_max_attempts: int = 1440,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._sagemaker_studio = SageMakerStudioAPI(self._get_sagemaker_studio_config())
        self.execution_name = execution_name
        self.input_config = input_config or {}
        self.output_config = output_config or {"output_formats": ["NOTEBOOK"]}
        self.compute = compute
        self.termination_condition = termination_condition or {}
        self.tags = tags or {}
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def _get_sagemaker_studio_config(self):
        config = ClientConfig()
        config.overrides["execution"] = {"local": is_local_runner()}
        return config

    def _format_start_execution_input_config(self):
        config = {
            "notebook_config": {
                "input_path": self.input_config.get("input_path"),
                "input_parameters": self.input_config.get("input_params"),
            },
        }

        return config

    def _format_start_execution_output_config(self):
        output_formats = self.output_config.get("output_formats")
        config = {
            "notebook_config": {
                "output_formats": output_formats,
            }
        }
        return config

    def start_notebook_execution(self):
        start_execution_params = {
            "execution_name": self.execution_name,
            "execution_type": "NOTEBOOK",
            "input_config": self._format_start_execution_input_config(),
            "output_config": self._format_start_execution_output_config(),
            "termination_condition": self.termination_condition,
            "tags": self.tags,
        }
        if self.compute:
            start_execution_params["compute"] = self.compute
        else:
            start_execution_params["compute"] = {"instance_type": "ml.m6i.xlarge"}

        print(start_execution_params)
        return self._sagemaker_studio.execution_client.start_execution(**start_execution_params)

    def wait_for_execution_completion(self, execution_id, context):
        wait_attempts = 0
        while wait_attempts < self.waiter_max_attempts:
            wait_attempts += 1
            time.sleep(self.waiter_delay)
            response = self._sagemaker_studio.execution_client.get_execution(execution_id=execution_id)
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

    def _set_xcom_files(self, files, context):
        if not context:
            error_message = "context is required"
            raise AirflowException(error_message)
        for file in files:
            context["ti"].xcom_push(
                key=f"{file['display_name']}.{file['file_format']}",
                value=file["file_path"],
            )

    def _set_xcom_s3_path(self, s3_path, context):
        if not context:
            error_message = "context is required"
            raise AirflowException(error_message)
        context["ti"].xcom_push(
            key="s3_path",
            value=s3_path,
        )

    def _handle_state(self, execution_id, status, error_message):
        finished_states = ["COMPLETED"]
        in_progress_states = ["IN_PROGRESS", "STOPPING"]

        if status in in_progress_states:
            info_message = f"Execution {execution_id} is still in progress with state:{status}, will check for a terminal status again in {self.waiter_delay}"
            self.log.info(info_message)
            return None
        execution_message = f"Exiting Execution {execution_id} State: {status}"
        if status in finished_states:
            self.log.info(execution_message)
            return {"Status": status, "ExecutionId": execution_id}
        log_error_message = f"Execution {execution_id} failed with error: {error_message}"
        self.log.error(log_error_message)
        if error_message == "":
            error_message = execution_message
        raise AirflowException(error_message)
