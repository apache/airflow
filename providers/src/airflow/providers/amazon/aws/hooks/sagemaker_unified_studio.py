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

import time

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from sagemaker_studio import ClientConfig
from sagemaker_studio._openapi.models import GetExecutionRequest, StartExecutionRequest
from sagemaker_studio.sagemaker_studio_api import SageMakerStudioAPI

from airflow.providers.amazon.aws.utils.sagemaker_unified_studio import is_local_runner


class SageMakerNotebookHook(BaseHook):
    """
    Interact with the Sagemaker Workflows API.

    This hook provides a wrapper around the Sagemaker Workflows Notebook Execution API.

    Examples:
     .. code-block:: python

        from workflows.airflow.providers.amazon.aws.hooks.notebook_hook import NotebookHook

        notebook_hook = NotebookHook(
            input_config={'input_path': 'path/to/notebook.ipynb', 'input_params': {'param1': 'value1'}},
            output_config={'output_uri': 'folder/output/location/prefix', 'output_format': 'ipynb'},
            execution_name='notebook_execution',
            poll_interval=10,
        )
    :param execution_name: The name of the notebook job to be executed, this is same as task_id.
    :param input_config: Configuration for the input file.
        Example: {'input_path': 'folder/input/notebook.ipynb', 'input_params': {'param1': 'value1'}}
    :param output_config: Configuration for the output format. It should include an output_formats parameter to control
        Example: {'output_formats': ['NOTEBOOK']}
    :param compute: compute configuration to use for the notebook execution. This is an required attribute
        if the execution is on a remote compute.
        Example: { "InstanceType": "ml.m5.large", "VolumeSizeInGB": 30, "VolumeKmsKeyId": "", "ImageUri": "string", "ContainerEntrypoint": [ "string" ]}
    :param termination_condition: conditions to match to terminate the remote execution.
        Example: { "MaxRuntimeInSeconds": 3600 }
    :param tags: tags to be associated with the remote execution runs.
        Example: { "md_analytics": "logs" }
    :param poll_interval: Interval in seconds to check the notebook execution status.
    """

    def __init__(
        self,
        execution_name: str,
        input_config: dict = {},
        output_config: dict = {"output_formats": ["NOTEBOOK"]},
        compute: dict = {},
        termination_condition: dict = {},
        tags: dict = {},
        poll_interval: int = 10,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._sagemaker_studio = SageMakerStudioAPI(self._get_sagemaker_studio_config())
        self.execution_name = execution_name
        self.input_config = input_config
        self.output_config = output_config
        self.compute = compute
        self.termination_condition = termination_condition
        self.tags = tags
        self.poll_interval = poll_interval

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
        output_formats = (
            self.output_config.get("output_formats") if self.output_config else ["NOTEBOOK"]
        )
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

        request = StartExecutionRequest(**start_execution_params)

        return self._sagemaker_studio.execution_client.start_execution(request)

    def wait_for_execution_completion(self, execution_id, context):

        while True:
            time.sleep(self.poll_interval)
            response = self.get_execution_response(execution_id)
            error_message = response.get("error_details", {}).get("error_message")
            status = response["status"]
            if "files" in response:
                self._set_xcom_files(response["files"], context)
            if "s3_path" in response:
                self._set_xcom_s3_path(response["s3_path"], context)

            ret = self._handle_state(execution_id, status, error_message)
            if ret:
                return ret

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

    def get_execution_response(self, execution_id):
        response = self._sagemaker_studio.execution_client.get_execution(
            GetExecutionRequest(execution_id=execution_id)
        )
        return response

    def _handle_state(self, execution_id, status, error_message):
        finished_states = ["COMPLETED"]
        in_progress_states = ["IN_PROGRESS", "STOPPING"]

        if status in in_progress_states:
            self.log.info(
                f"Execution {execution_id} is still in progress with state:{status}, will check for a terminal status again in {self.poll_interval}"
            )
            return None
        execution_message = f"Exiting Execution {execution_id} State: {status}"
        if status in finished_states:
            self.log.info(execution_message)
            return {"Status": status, "ExecutionId": execution_id}
        else:
            self.log.error(f"{execution_message} Message: {error_message}")
            if error_message == "":
                error_message = execution_message
            raise AirflowException(error_message)