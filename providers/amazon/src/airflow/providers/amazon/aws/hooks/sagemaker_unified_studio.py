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
from typing import Any

from sagemaker_studio import ClientConfig
from sagemaker_studio.sagemaker_studio_api import SageMakerStudioAPI

from airflow.providers.amazon.aws.utils.sagemaker_unified_studio import is_local_runner
from airflow.providers.common.compat.sdk import AirflowException, BaseHook


class SageMakerNotebookHook(BaseHook):
    """
    Interact with Sagemaker Unified Studio Workflows for executing Jupyter notebooks, querybooks, and visual ETL jobs.

    This hook provides a wrapper around the Sagemaker Workflows Notebook Execution API.
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
        self._sagemaker_studio = SageMakerStudioAPI(self._get_sagemaker_studio_config())
        self.input_config = input_config or {}
        self.output_config = output_config or {"output_formats": ["NOTEBOOK"]}
        self.compute = compute
        self.termination_condition = termination_condition or {}
        self.tags = tags or {}
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def _get_sagemaker_studio_config(self):
        config = ClientConfig()
        if self.domain_region:
            config.region = self.domain_region
        config.overrides["execution"] = {
            "local": is_local_runner(),
            "domain_identifier": self.domain_id,
            "project_identifier": self.project_id,
            "datazone_domain_region": self.domain_region,
        }
        return config

    def _format_start_execution_input_config(self):
        return {
            "notebook_config": {
                "input_path": self.input_config.get("input_path"),
                "input_parameters": self.input_config.get("input_params"),
            },
        }

    def _format_start_execution_output_config(self):
        return {
            "notebook_config": {
                "output_formats": self.output_config.get("output_formats"),
            }
        }

    def start_notebook_execution(self):
        start_execution_params = {
            "execution_name": self.execution_name,
            "execution_type": "NOTEBOOK",
            "domain_id": self.domain_id,
            "project_id": self.project_id,
            "input_config": self._format_start_execution_input_config(),
            "output_config": self._format_start_execution_output_config(),
            "termination_condition": self.termination_condition,
            "tags": self.tags,
        }

        if self.domain_region:
            start_execution_params["domain_region"] = self.domain_region

        if self.compute:
            start_execution_params["compute"] = self.compute

        return self._sagemaker_studio.execution_client.start_execution(**start_execution_params)

    def get_notebook_execution(self, execution_id: str) -> dict[str, Any]:
        """Fetch the status of a SageMaker Notebook Job execution."""
        if self._sagemaker_studio.execution_client is None:
            raise AirflowException("SageMaker Studio execution client is not initialized.")
        return self._sagemaker_studio.execution_client.get_execution(execution_id=execution_id)

    def wait_for_execution_completion(self, execution_id, context):
        wait_attempts = 0
        while wait_attempts < self.waiter_max_attempts:
            wait_attempts += 1
            time.sleep(self.waiter_delay)

            response = self.get_notebook_execution(execution_id)

            error_message = response.get("error_details", {}).get("error_message")
            status = response["status"]

            if "files" in response:
                self._set_xcom_files(response["files"], context)

            if "s3_path" in response:
                self._set_xcom_s3_path(response["s3_path"], context)

            ret = self._handle_state(execution_id, status, error_message)
            if ret:
                return ret

        return self._handle_state(execution_id, "FAILED", "Execution timed out")

    def _set_xcom_files(self, files, context):
        if not context:
            return

        for file in files:
            context["ti"].xcom_push(
                key=f"{file['display_name']}.{file['file_format']}",
                value=file["file_path"],
            )

    def _set_xcom_s3_path(self, s3_path, context):
        if not context:
            return

        context["ti"].xcom_push(
            key="s3_path",
            value=s3_path,
        )

    def _handle_state(self, execution_id, status, error_message):
        finished_states = ["COMPLETED"]
        in_progress_states = ["IN_PROGRESS", "STOPPING"]

        if status in in_progress_states:
            self.log.info(
                "Execution %s is still in progress with state:%s, will check again in %ss",
                execution_id,
                status,
                self.waiter_delay,
            )
            return None

        if status in finished_states:
            self.log.info("Execution %s completed successfully", execution_id)
            return {"Status": status, "ExecutionId": execution_id}

        self.log.error("Execution %s failed with error: %s", execution_id, error_message)

        if not error_message:
            error_message = f"Execution {execution_id} ended with status {status}"

        raise AirflowException(error_message)
