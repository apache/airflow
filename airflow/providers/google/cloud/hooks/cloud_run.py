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
"""This module contains a Google Cloud Run Hook."""
from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List, Sequence, Union, cast

from google.api_core.client_options import ClientOptions
from googleapiclient.discovery import build

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudRunJobSteps:
    """
    Helper class with Cloud Run job status.
    Reference: https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs#JobStatus
    """

    EXEC_STEP_COMPLETED = "Completed"
    EXEC_STEP_RESOURCES_AVAILABLE = "ResourcesAvailable"
    EXEC_STEP_STARTED = "Started"
    AWAITING_STEPS = {EXEC_STEP_STARTED, EXEC_STEP_RESOURCES_AVAILABLE}
    ALL_STEPS = [EXEC_STEP_COMPLETED, EXEC_STEP_RESOURCES_AVAILABLE, EXEC_STEP_STARTED]


class _CloudRunJobExecutionController(LoggingMixin):
    """
    Interface for communication with Google API.

    :param cloud_run: Discovery resource
    :param project_id: The Google Cloud Project ID.
    :param execution_id: ID of a Cloud Run Job execution.
    :param poll_sleep: The status refresh rate for pending operations.
    :param num_retries: Maximum number of retries in case of connection problems.
    :param wait_until_finished: If True, wait for the end of pipeline execution
        before exiting. If False, it only submits job and check once if
        the job is not in terminal state.
    """

    def __init__(
        self,
        cloud_run: Any,
        project_id: str,
        region: str,
        wait_until_finished: bool,
        execution_id: str | None = None,
        poll_sleep: float = 10.0,
        num_retries: int = 0,
        job_exec_cold_start: float = 10.0,
    ) -> None:

        super().__init__()
        self._cloud_run = cloud_run
        self._project_id = project_id
        self._region = region
        self._exec_id = execution_id
        self._poll_sleep = poll_sleep
        self._num_retries = num_retries
        self._execution: dict | None = None
        self._execution_state: str | None = None
        self._job_exec_cold_start = job_exec_cold_start
        self._wait_until_finished = wait_until_finished

    def _fetch_execution_by_id(self):
        """
        Helper method to fetch the execution with the specified execution ID.

        :return: the Cloud Run job execution
        """
        self.log.debug("Fetching information for job execution %s", self._exec_id)
        return (
            self._cloud_run.namespaces()
            .executions()
            .get(name=f"namespaces/{self._project_id}/executions/{self._exec_id}")
            .execute(num_retries=self._num_retries)
        )

    def _check_execution_state(self) -> bool:
        """
        Helper method to check the state of job execution
        if execution failed raise exception

        :return: True if execution is done.
        :raise: Exception
        """
        logs_uri = (
            f"https://console.cloud.google.com/run/jobs/executions"
            f"/details/{self._region}/{self._exec_id}/logs?project={self._project_id}"
        )
        job_exec_exception = Exception(
            f"An error occurred when starting Google Cloud Run job execution {self._exec_id}."
            f"\nSee details at {logs_uri} "
        )
        if not isinstance(self._execution, dict):
            raise job_exec_exception
        execution = cast(Dict[str, Union[str, dict, int]], self._execution)
        if "status" not in execution.keys():
            raise job_exec_exception
        status = cast(Dict[str, Union[int, str, List[dict]]], execution["status"])
        if "conditions" not in status.keys():
            raise job_exec_exception
        conditions = cast(List[Dict[str, str]], status["conditions"])
        steps = []
        for c in conditions:
            step = c["type"]
            verdict = c["status"]
            if step not in CloudRunJobSteps.ALL_STEPS:
                raise Exception(
                    f"Unknown state {step} found for Google Cloud Run job execution {self._exec_id} "
                    f"See details at {logs_uri}."
                )
            if verdict == "False":
                # This verdict means that a step failed,
                # therefore the execution has failed
                raise Exception(
                    f"Cloud Run Job execution {self._exec_id} has failed. See details at {logs_uri} "
                )
            if verdict == "Unknown":
                # This verdict means that the step is not completed yet,
                # therefore the execution cannot be finished yet
                return False
            if verdict == "True":
                steps.append(step)
        return CloudRunJobSteps.EXEC_STEP_COMPLETED in steps

    def wait_for_done(self) -> None:
        """Helper method to wait for result of job execution."""
        # Wait a few seconds for the job execution status to be available
        # otherwise it fails
        time.sleep(self._job_exec_cold_start)
        self._execution = self._fetch_execution_by_id()
        if self._wait_until_finished:
            self.log.info("Starting to poll status for Google Cloud Run job execution %s", self._exec_id)
            self.log.info(json.dumps(self._execution))
            while not self._check_execution_state():
                self.log.info("Waiting for execution completion. Sleeping %d seconds", self._poll_sleep)
                time.sleep(self._poll_sleep)
                self._execution = self._fetch_execution_by_id()
        else:
            # Check execution state only once to ensure the job has started
            # Otherwise, _check_execution_state method would have raised an error
            self._check_execution_state()
            return


class CloudRunJobHook(GoogleBaseHook):
    """
    Hook for Google Cloud Run.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param wait_until_finished: If True, wait for the end of pipeline
    execution before exiting. If False, it only submits job
    and check once is job not in terminal state.
    """

    DEFAULT_CLOUD_RUN_REGION = "us-central1"

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        region: str = DEFAULT_CLOUD_RUN_REGION,
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        delete_timeout: int | None = 5 * 60,
        wait_until_finished: bool = False,
    ) -> None:
        self.region = region
        self.delete_timeout = delete_timeout
        self.wait_until_finished = wait_until_finished
        self.job_id: str | None = None
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to, impersonation_chain=impersonation_chain
        )

    def get_conn(self) -> build:
        """Returns a Google Cloud Run service object."""
        http_authorized = self._authorize()

        # Use a regional endpoint since job execution is not possible from the global endpoint
        client_options = ClientOptions(api_endpoint=f"https://{self.region}-run.googleapis.com")
        return build("run", "v1", client_options=client_options, http=http_authorized, cache_discovery=False)

    @GoogleBaseHook.fallback_to_default_project_id
    def execute_cloud_run_job(
        self, project_id: str, job_name: str, on_new_execution_callback: Callable[[str], None] | None = None
    ) -> dict:
        """
        Executes a Cloud Run job.

        :param project_id: Optional, the Google Cloud project ID in which
            to start a job. If set to None or missing, the default
            project_id from the Google Cloud connection is used.
        :param job_name: The name of the job
        :param on_new_execution_callback: A callback that is called
            when a job execution is detected.
        """
        service = self.get_conn()

        request = service.namespaces().jobs().run(name=f"namespaces/{project_id}/jobs/{job_name}")

        execution = request.execute(num_retries=self.num_retries)
        exec_id = execution["metadata"]["name"]
        self.log.info("Requested job execution with response:\n%s", json.dumps(execution))
        if on_new_execution_callback:
            on_new_execution_callback(execution)

        execution_controller = _CloudRunJobExecutionController(
            cloud_run=service,
            project_id=project_id,
            region=self.region,
            execution_id=exec_id,
            num_retries=self.num_retries,
            wait_until_finished=self.wait_until_finished,
        )
        self.log.info(
            "Job execution details available at "
            "https://console.cloud.google.com/run/jobs/executions/details/%s/%s?project=%s ",
            self.region,
            exec_id,
            self.project_id,
        )

        execution_controller.wait_for_done()
        return execution
