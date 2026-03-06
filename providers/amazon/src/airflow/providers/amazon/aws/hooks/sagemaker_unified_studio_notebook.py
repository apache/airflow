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

"""This module contains the Amazon SageMaker Unified Studio Notebook Run hook."""

from __future__ import annotations

import time
import uuid
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

TWELVE_HOURS_IN_MINUTES = 12 * 60


class SageMakerUnifiedStudioNotebookHook(AwsBaseHook):
    """
    Interact with Sagemaker Unified Studio Workflows for asynchronous notebook execution.

    This hook provides a wrapper around the DataZone StartNotebookRun / GetNotebookRun APIs.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookHook,
        )

        hook = SageMakerUnifiedStudioNotebookHook(aws_conn_id="my_aws_conn")

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args: Any, **kwargs: Any):
        kwargs.setdefault("client_type", "datazone")
        super().__init__(*args, **kwargs)

    def _validate_api_availability(self) -> None:
        """
        Verify that the NotebookRun APIs are available in the installed boto3/botocore version.

        :raises RuntimeError: If the required APIs are not available.
        """
        required_methods = ("start_notebook_run", "get_notebook_run")
        for method_name in required_methods:
            if not hasattr(self.conn, method_name):
                raise RuntimeError(
                    f"The '{method_name}' API is not available in the installed boto3/botocore version. "
                    "Please upgrade boto3/botocore to a version that supports the DataZone "
                    "NotebookRun APIs."
                )

    def start_notebook_run(
        self,
        notebook_id: str,
        domain_id: str,
        project_id: str,
        client_token: str | None = None,
        notebook_parameters: dict | None = None,
        compute_configuration: dict | None = None,
        timeout_configuration: dict | None = None,
        workflow_name: str | None = None,
    ) -> dict:
        """
        Start an asynchronous notebook run via the DataZone StartNotebookRun API.

        :param notebook_id: The ID of the notebook to execute.
        :param domain_id: The ID of the DataZone domain containing the notebook.
        :param project_id: The ID of the DataZone project containing the notebook.
        :param client_token: Idempotency token. Auto-generated if not provided.
        :param notebook_parameters: Parameters to pass to the notebook.
        :param compute_configuration: Compute config (e.g. instance_type).
        :param timeout_configuration: Timeout settings (run_timeout_in_minutes).
        :param workflow_name: Name of the workflow (DAG) that triggered this run.
        :return: The StartNotebookRun API response dict.
        """
        params: dict = {
            "domain_id": domain_id,
            "project_id": project_id,
            "notebook_id": notebook_id,
            "client_token": client_token or str(uuid.uuid4()),
        }

        if notebook_parameters:
            params["notebook_parameters"] = notebook_parameters
        if compute_configuration:
            params["compute_configuration"] = compute_configuration
        if timeout_configuration:
            params["timeout_configuration"] = timeout_configuration
        if workflow_name:
            params["trigger_source"] = {"type": "workflow", "workflow_name": workflow_name}

        self.log.info("Starting notebook run for notebook %s in domain %s", notebook_id, domain_id)
        return self.conn.start_notebook_run(**params)

    def get_notebook_run(self, notebook_run_id: str, domain_id: str) -> dict:
        """
        Get the status of a notebook run via the DataZone GetNotebookRun API.

        :param notebook_run_id: The ID of the notebook run.
        :param domain_id: The ID of the DataZone domain.
        :return: The GetNotebookRun API response dict.
        """
        return self.conn.get_notebook_run(
            domain_id=domain_id,
            notebook_run_id=notebook_run_id,
        )

    def wait_for_notebook_run(
        self,
        notebook_run_id: str,
        domain_id: str,
        waiter_delay: int = 10,
        timeout_configuration: dict | None = None,
    ) -> dict:
        """
        Poll GetNotebookRun until the run reaches a terminal state.

        :param notebook_run_id: The ID of the notebook run to monitor.
        :param domain_id: The ID of the DataZone domain.
        :param waiter_delay: Interval in seconds to poll the notebook run status.
        :param timeout_configuration: Timeout settings for the notebook execution.
            When provided, the maximum number of poll attempts is derived from
            ``run_timeout_in_minutes * 60 / waiter_delay``. Defaults to 12 hours.
        :return: A dict with Status and NotebookRunId on success.
        :raises RuntimeError: If the run fails or times out.
        """
        run_timeout = (timeout_configuration or {}).get("run_timeout_in_minutes", TWELVE_HOURS_IN_MINUTES)
        waiter_max_attempts = int(run_timeout * 60 / waiter_delay)

        for _attempt in range(waiter_max_attempts):
            time.sleep(waiter_delay)
            response = self.get_notebook_run(notebook_run_id, domain_id=domain_id)
            status = response.get("status", "")
            error_message = response.get("errorMessage", "")

            ret = self._handle_state(notebook_run_id, status, error_message, waiter_delay)
            if ret:
                return ret

        error_message = "Execution timed out"
        self.log.error("Notebook run %s failed with error: %s", notebook_run_id, error_message)
        raise RuntimeError(error_message)

    def _handle_state(
        self, notebook_run_id: str, state: str, error_message: str, waiter_delay: int = 10
    ) -> dict | None:
        """
        Evaluate the current notebook run state and return or raise accordingly.

        :param notebook_run_id: The ID of the notebook run.
        :param state: The current state string.
        :param error_message: Error message from the API response, if any.
        :param waiter_delay: Interval in seconds between polls (for logging).
        :return: A dict with Status and NotebookRunId on success, None if still in progress.
        :raises RuntimeError: If the run has failed.
        """
        in_progress_states = {"QUEUED", "STARTING", "RUNNING", "STOPPING"}
        finished_states = {"SUCCEEDED", "STOPPED"}
        failure_states = {"FAILED"}

        if state in in_progress_states:
            self.log.info(
                "Notebook run %s is still in progress with state: %s, "
                "will check for a terminal status again in %ss",
                notebook_run_id,
                state,
                waiter_delay,
            )
            return None

        execution_message = f"Exiting notebook run {notebook_run_id}. State: {state}"

        if state in finished_states:
            self.log.info(execution_message)
            return {"State": state, "NotebookRunId": notebook_run_id}

        if state in failure_states:
            self.log.error("Notebook run %s failed with error: %s", notebook_run_id, error_message)
        else:
            self.log.error("Notebook run %s reached unexpected state: %s", notebook_run_id, state)

        if error_message == "":
            error_message = execution_message
        raise RuntimeError(error_message)
