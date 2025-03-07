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
"""This module contains a Google Dataflow Hook."""

from __future__ import annotations

import functools
import json
import re
import shlex
import subprocess
import time
import uuid
import warnings
from collections.abc import Generator, Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from google.cloud.dataflow_v1beta3 import (
    GetJobRequest,
    Job,
    JobState,
    JobsV1Beta3AsyncClient,
    JobView,
    ListJobMessagesRequest,
    MessagesV1Beta3AsyncClient,
    MetricsV1Beta3AsyncClient,
)
from google.cloud.dataflow_v1beta3.types import (
    GetJobMetricsRequest,
    JobMessageImportance,
    JobMetrics,
)
from google.cloud.dataflow_v1beta3.types.jobs import ListJobsRequest
from googleapiclient.discovery import Resource, build

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType, beam_options_to_args
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timeout import timeout

if TYPE_CHECKING:
    from google.cloud.dataflow_v1beta3.services.jobs_v1_beta3.pagers import ListJobsAsyncPager
    from google.cloud.dataflow_v1beta3.services.messages_v1_beta3.pagers import ListJobMessagesAsyncPager
    from google.protobuf.timestamp_pb2 import Timestamp


# This is the default location
# https://cloud.google.com/dataflow/pipelines/specifying-exec-params
DEFAULT_DATAFLOW_LOCATION = "us-central1"


JOB_ID_PATTERN = re.compile(
    r"Submitted job: (?P<job_id_java>[^\"\n\s]*)|Created job with id: \[(?P<job_id_python>[^\"\n\s]*)\]"
)

T = TypeVar("T", bound=Callable)


def process_line_and_extract_dataflow_job_id_callback(
    on_new_job_id_callback: Callable[[str], None] | None,
) -> Callable[[str], None]:
    """
    Build callback that triggers the specified function.

    The returned callback is intended to be used as ``process_line_callback`` in
    :py:class:`~airflow.providers.apache.beam.hooks.beam.BeamCommandRunner`.

    :param on_new_job_id_callback: Callback called when the job ID is known
    """

    def _process_line_and_extract_job_id(line: str) -> None:
        # Job id info: https://goo.gl/SE29y9.
        if on_new_job_id_callback is None:
            return
        matched_job = JOB_ID_PATTERN.search(line)
        if matched_job is None:
            return
        job_id = matched_job.group("job_id_java") or matched_job.group("job_id_python")
        on_new_job_id_callback(job_id)

    return _process_line_and_extract_job_id


def _fallback_variable_parameter(parameter_name: str, variable_key_name: str) -> Callable[[T], T]:
    def _wrapper(func: T) -> T:
        """
        Fallback for location from `region` key in `variables` parameters.

        :param func: function to wrap
        :return: result of the function call
        """

        @functools.wraps(func)
        def inner_wrapper(self: DataflowHook, *args, **kwargs):
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )

            parameter_location = kwargs.get(parameter_name)
            variables_location = kwargs.get("variables", {}).get(variable_key_name)

            if parameter_location and variables_location:
                raise AirflowException(
                    f"The mutually exclusive parameter `{parameter_name}` and `{variable_key_name}` key "
                    f"in `variables` parameter are both present. Please remove one."
                )
            if parameter_location or variables_location:
                kwargs[parameter_name] = parameter_location or variables_location
            if variables_location:
                copy_variables = deepcopy(kwargs["variables"])
                del copy_variables[variable_key_name]
                kwargs["variables"] = copy_variables

            return func(self, *args, **kwargs)

        return cast(T, inner_wrapper)

    return _wrapper


_fallback_to_location_from_variables = _fallback_variable_parameter("location", "region")
_fallback_to_project_id_from_variables = _fallback_variable_parameter("project_id", "project")


class DataflowJobStatus:
    """
    Helper class with Dataflow job statuses.

    Reference: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    """

    JOB_STATE_DONE = "JOB_STATE_DONE"
    JOB_STATE_UNKNOWN = "JOB_STATE_UNKNOWN"
    JOB_STATE_STOPPED = "JOB_STATE_STOPPED"
    JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
    JOB_STATE_FAILED = "JOB_STATE_FAILED"
    JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
    JOB_STATE_UPDATED = "JOB_STATE_UPDATED"
    JOB_STATE_DRAINING = "JOB_STATE_DRAINING"
    JOB_STATE_DRAINED = "JOB_STATE_DRAINED"
    JOB_STATE_PENDING = "JOB_STATE_PENDING"
    JOB_STATE_CANCELLING = "JOB_STATE_CANCELLING"
    JOB_STATE_QUEUED = "JOB_STATE_QUEUED"
    FAILED_END_STATES = {JOB_STATE_FAILED, JOB_STATE_CANCELLED}
    SUCCEEDED_END_STATES = {JOB_STATE_DONE, JOB_STATE_UPDATED, JOB_STATE_DRAINED}
    TERMINAL_STATES = SUCCEEDED_END_STATES | FAILED_END_STATES
    AWAITING_STATES = {
        JOB_STATE_RUNNING,
        JOB_STATE_PENDING,
        JOB_STATE_QUEUED,
        JOB_STATE_CANCELLING,
        JOB_STATE_DRAINING,
        JOB_STATE_STOPPED,
    }


class DataflowJobType:
    """Helper class with Dataflow job types."""

    JOB_TYPE_UNKNOWN = "JOB_TYPE_UNKNOWN"
    JOB_TYPE_BATCH = "JOB_TYPE_BATCH"
    JOB_TYPE_STREAMING = "JOB_TYPE_STREAMING"


class _DataflowJobsController(LoggingMixin):
    """
    Interface for communication with Google Cloud Dataflow API.

    Does not use Apache Beam API.

    :param dataflow: Discovery resource
    :param project_number: The Google Cloud Project ID.
    :param location: Job location.
    :param poll_sleep: The status refresh rate for pending operations.
    :param name: The Job ID prefix used when the multiple_jobs option is passed is set to True.
    :param job_id: ID of a single job.
    :param num_retries: Maximum number of retries in case of connection problems.
    :param multiple_jobs: If set to true this task will be searched by name prefix (``name`` parameter),
        not by specific job ID, then actions will be performed on all matching jobs.
    :param drain_pipeline: Optional, set to True if we want to stop streaming job by draining it
        instead of canceling.
    :param cancel_timeout: wait time in seconds for successful job canceling
    :param wait_until_finished: If True, wait for the end of pipeline execution before exiting. If False,
        it only submits job and check once is job not in terminal state.

        The default behavior depends on the type of pipeline:

        * for the streaming pipeline, wait for jobs to be in JOB_STATE_RUNNING,
        * for the batch pipeline, wait for the jobs to be in JOB_STATE_DONE.
    """

    def __init__(
        self,
        dataflow: Any,
        project_number: str,
        location: str,
        poll_sleep: int = 10,
        name: str | None = None,
        job_id: str | None = None,
        num_retries: int = 0,
        multiple_jobs: bool = False,
        drain_pipeline: bool = False,
        cancel_timeout: int | None = 5 * 60,
        wait_until_finished: bool | None = None,
        expected_terminal_state: str | None = None,
    ) -> None:
        super().__init__()
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_location = location
        self._multiple_jobs = multiple_jobs
        self._job_id = job_id
        self._num_retries = num_retries
        self._poll_sleep = poll_sleep
        self._cancel_timeout = cancel_timeout
        self._jobs: list[dict] | None = None
        self.drain_pipeline = drain_pipeline
        self._wait_until_finished = wait_until_finished
        self._expected_terminal_state = expected_terminal_state

    def is_job_running(self) -> bool:
        """
        Check if job is still running in dataflow.

        :return: True if job is running.
        """
        self._refresh_jobs()
        if not self._jobs:
            return False

        return any(job["currentState"] not in DataflowJobStatus.TERMINAL_STATES for job in self._jobs)

    def _get_current_jobs(self) -> list[dict]:
        """
        Get list of jobs that start with job name or id.

        :return: list of jobs including id's
        """
        if not self._multiple_jobs and self._job_id:
            return [self.fetch_job_by_id(self._job_id)]
        elif self._jobs:
            return [self.fetch_job_by_id(job["id"]) for job in self._jobs]
        elif self._job_name:
            jobs = self._fetch_jobs_by_prefix_name(self._job_name.lower())
            if len(jobs) == 1:
                self._job_id = jobs[0]["id"]
            return jobs
        else:
            raise ValueError("Missing both dataflow job ID and name.")

    def fetch_job_by_id(self, job_id: str) -> dict[str, str]:
        """
        Fetch the job with the specified Job ID.

        :param job_id: ID of the job that needs to be fetched.
        :return: Dictionary containing the Job's data
        """
        return (
            self._dataflow.projects()
            .locations()
            .jobs()
            .get(
                projectId=self._project_number,
                location=self._job_location,
                jobId=job_id,
            )
            .execute(num_retries=self._num_retries)
        )

    def fetch_job_metrics_by_id(self, job_id: str) -> dict:
        """
        Fetch the job metrics with the specified Job ID.

        :param job_id: Job ID to get.
        :return: the JobMetrics. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
        """
        result = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .getMetrics(projectId=self._project_number, location=self._job_location, jobId=job_id)
            .execute(num_retries=self._num_retries)
        )

        self.log.debug("fetch_job_metrics_by_id %s:\n%s", job_id, result)
        return result

    def _fetch_list_job_messages_responses(self, job_id: str) -> Generator[dict, None, None]:
        """
        Fetch ListJobMessagesResponse with the specified Job ID.

        :param job_id: Job ID to get.
        :return: yields the ListJobMessagesResponse. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse
        """
        request = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .messages()
            .list(projectId=self._project_number, location=self._job_location, jobId=job_id)
        )

        while request is not None:
            response = request.execute(num_retries=self._num_retries)
            yield response

            request = (
                self._dataflow.projects()
                .locations()
                .jobs()
                .messages()
                .list_next(previous_request=request, previous_response=response)
            )

    def fetch_job_messages_by_id(self, job_id: str) -> list[dict]:
        """
        Fetch the job messages with the specified Job ID.

        :param job_id: Job ID to get.
        :return: the list of JobMessages. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#JobMessage
        """
        messages: list[dict] = []
        for response in self._fetch_list_job_messages_responses(job_id=job_id):
            messages.extend(response.get("jobMessages", []))
        return messages

    def fetch_job_autoscaling_events_by_id(self, job_id: str) -> list[dict]:
        """
        Fetch the job autoscaling events with the specified Job ID.

        :param job_id: Job ID to get.
        :return: the list of AutoscalingEvents. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#autoscalingevent
        """
        autoscaling_events: list[dict] = []
        for response in self._fetch_list_job_messages_responses(job_id=job_id):
            autoscaling_events.extend(response.get("autoscalingEvents", []))
        return autoscaling_events

    def _fetch_all_jobs(self) -> list[dict]:
        request = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .list(projectId=self._project_number, location=self._job_location)
        )
        all_jobs: list[dict] = []
        while request is not None:
            response = request.execute(num_retries=self._num_retries)
            jobs = response.get("jobs")
            if jobs is None:
                break
            all_jobs.extend(jobs)

            request = (
                self._dataflow.projects()
                .locations()
                .jobs()
                .list_next(previous_request=request, previous_response=response)
            )
        return all_jobs

    def _fetch_jobs_by_prefix_name(self, prefix_name: str) -> list[dict]:
        jobs = self._fetch_all_jobs()
        jobs = [job for job in jobs if job["name"].startswith(prefix_name)]
        return jobs

    def _refresh_jobs(self) -> None:
        """
        Get all jobs by name.

        :return: jobs
        """
        self._jobs = self._get_current_jobs()

        if self._jobs:
            for job in self._jobs:
                self.log.info(
                    "Google Cloud DataFlow job %s is state: %s",
                    job["name"],
                    job["currentState"],
                )
        else:
            self.log.info("Google Cloud DataFlow job not available yet..")

    def _check_dataflow_job_state(self, job) -> bool:
        """
        Check the state of one job in dataflow for this task if job failed raise exception.

        :return: True if job is done.
        :raise: Exception
        """
        current_state = job["currentState"]
        is_streaming = job.get("type") == DataflowJobType.JOB_TYPE_STREAMING

        current_expected_state = self._expected_terminal_state

        if current_expected_state is None:
            if is_streaming:
                current_expected_state = DataflowJobStatus.JOB_STATE_RUNNING
            else:
                current_expected_state = DataflowJobStatus.JOB_STATE_DONE

        terminal_states = DataflowJobStatus.TERMINAL_STATES | {DataflowJobStatus.JOB_STATE_RUNNING}
        if current_expected_state not in terminal_states:
            raise AirflowException(
                f"Google Cloud Dataflow job's expected terminal state "
                f"'{current_expected_state}' is invalid."
                f" The value should be any of the following: {terminal_states}"
            )
        elif is_streaming and current_expected_state == DataflowJobStatus.JOB_STATE_DONE:
            raise AirflowException(
                "Google Cloud Dataflow job's expected terminal state cannot be "
                "JOB_STATE_DONE while it is a streaming job"
            )
        elif not is_streaming and current_expected_state == DataflowJobStatus.JOB_STATE_DRAINED:
            raise AirflowException(
                "Google Cloud Dataflow job's expected terminal state cannot be "
                "JOB_STATE_DRAINED while it is a batch job"
            )
        if current_state == current_expected_state:
            if current_expected_state == DataflowJobStatus.JOB_STATE_RUNNING:
                return not self._wait_until_finished
            return True

        if current_state in DataflowJobStatus.AWAITING_STATES:
            return self._wait_until_finished is False

        self.log.debug("Current job: %s", job)
        raise AirflowException(
            f"Google Cloud Dataflow job {job['name']} is in an unexpected terminal state: {current_state}, "
            f"expected terminal state: {current_expected_state}"
        )

    def wait_for_done(self) -> None:
        """Wait for result of submitted job."""
        self.log.info("Start waiting for done.")
        self._refresh_jobs()
        while self._jobs and not all(self._check_dataflow_job_state(job) for job in self._jobs):
            self.log.info("Waiting for done. Sleep %s s", self._poll_sleep)
            time.sleep(self._poll_sleep)
            self._refresh_jobs()

    def get_jobs(self, refresh: bool = False) -> list[dict]:
        """
        Return Dataflow jobs.

        :param refresh: Forces the latest data to be fetched.
        :return: list of jobs
        """
        if not self._jobs or refresh:
            self._refresh_jobs()
        if not self._jobs:
            raise ValueError("Could not read _jobs")

        return self._jobs

    def _wait_for_states(self, expected_states: set[str]):
        """Wait for the jobs to reach a certain state."""
        if not self._jobs:
            raise ValueError("The _jobs should be set")
        while True:
            self._refresh_jobs()
            job_states = {job["currentState"] for job in self._jobs}
            if not job_states.difference(expected_states):
                return
            unexpected_failed_end_states = DataflowJobStatus.FAILED_END_STATES - expected_states
            if unexpected_failed_end_states.intersection(job_states):
                unexpected_failed_jobs = [
                    job for job in self._jobs if job["currentState"] in unexpected_failed_end_states
                ]
                raise AirflowException(
                    "Jobs failed: "
                    + ", ".join(
                        f"ID: {job['id']} name: {job['name']} state: {job['currentState']}"
                        for job in unexpected_failed_jobs
                    )
                )
            time.sleep(self._poll_sleep)

    def cancel(self) -> None:
        """Cancel or drains current job."""
        self._jobs = [
            job for job in self.get_jobs() if job["currentState"] not in DataflowJobStatus.TERMINAL_STATES
        ]
        job_ids = [job["id"] for job in self._jobs]
        if job_ids:
            self.log.info("Canceling jobs: %s", ", ".join(job_ids))
            for job in self._jobs:
                requested_state = (
                    DataflowJobStatus.JOB_STATE_DRAINED
                    if self.drain_pipeline and job["type"] == DataflowJobType.JOB_TYPE_STREAMING
                    else DataflowJobStatus.JOB_STATE_CANCELLED
                )
                request = (
                    self._dataflow.projects()
                    .locations()
                    .jobs()
                    .update(
                        projectId=self._project_number,
                        location=self._job_location,
                        jobId=job["id"],
                        body={"requestedState": requested_state},
                    )
                )
                request.execute(num_retries=self._num_retries)
            if self._cancel_timeout and isinstance(self._cancel_timeout, int):
                timeout_error_message = (
                    f"Canceling jobs failed due to timeout ({self._cancel_timeout}s): {', '.join(job_ids)}"
                )
                tm = timeout(seconds=self._cancel_timeout, error_message=timeout_error_message)
                with tm:
                    self._wait_for_states(
                        {DataflowJobStatus.JOB_STATE_CANCELLED, DataflowJobStatus.JOB_STATE_DRAINED}
                    )
        else:
            self.log.info("No jobs to cancel")


class DataflowHook(GoogleBaseHook):
    """
    Hook for Google Dataflow.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        drain_pipeline: bool = False,
        cancel_timeout: int | None = 5 * 60,
        wait_until_finished: bool | None = None,
        expected_terminal_state: str | None = None,
        **kwargs,
    ) -> None:
        self.poll_sleep = poll_sleep
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.job_id: str | None = None
        self.beam_hook = BeamHook(BeamRunnerType.DataflowRunner)
        self.expected_terminal_state = expected_terminal_state
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def get_conn(self) -> Resource:
        """Return a Google Cloud Dataflow service object."""
        http_authorized = self._authorize()
        return build("dataflow", "v1b3", http=http_authorized, cache_discovery=False)

    def get_pipelines_conn(self) -> build:
        """Return a Google Cloud Data Pipelines service object."""
        http_authorized = self._authorize()
        return build("datapipelines", "v1", http=http_authorized, cache_discovery=False)

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    @deprecated(
        planned_removal_date="March 01, 2025",
        use_instead="airflow.providers.apache.beam.hooks.beam.start.start_java_pipeline, "
        "providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done",
        instructions="Please use airflow.providers.apache.beam.hooks.beam.start.start_java_pipeline "
        "to start pipeline and providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done method "
        "to wait for the required pipeline state instead.",
        category=AirflowProviderDeprecationWarning,
    )
    def start_java_dataflow(
        self,
        job_name: str,
        variables: dict,
        jar: str,
        project_id: str,
        job_class: str | None = None,
        append_job_name: bool = True,
        multiple_jobs: bool = False,
        on_new_job_id_callback: Callable[[str], None] | None = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        """
        Start Dataflow java job.

        :param job_name: The name of the job.
        :param variables: Variables passed to the job.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param jar: Name of the jar for the job
        :param job_class: Name of the java class for the job.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :param multiple_jobs: True if to check for multiple job in dataflow
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :param location: Job location.
        """
        name = self.build_dataflow_job_name(job_name, append_job_name)

        variables["jobName"] = name
        variables["region"] = location
        variables["project"] = project_id

        if "labels" in variables:
            variables["labels"] = json.dumps(variables["labels"], separators=(",", ":"))

        self.beam_hook.start_java_pipeline(
            variables=variables,
            jar=jar,
            job_class=job_class,
            process_line_callback=process_line_and_extract_dataflow_job_id_callback(on_new_job_id_callback),
        )
        self.wait_for_done(
            job_name=name,
            location=location,
            job_id=self.job_id,
            multiple_jobs=multiple_jobs,
        )

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_template_dataflow(
        self,
        job_name: str,
        variables: dict,
        parameters: dict,
        dataflow_template: str,
        project_id: str,
        append_job_name: bool = True,
        on_new_job_id_callback: Callable[[str], None] | None = None,
        on_new_job_callback: Callable[[dict], None] | None = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        environment: dict | None = None,
    ) -> dict[str, str]:
        """
        Launch a Dataflow job with a Classic Template and wait for its completion.

        :param job_name: The name of the job.
        :param variables: Map of job runtime environment options.
            It will update environment argument if passed.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        :param parameters: Parameters for the template
        :param dataflow_template: GCS path to the template.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :param on_new_job_id_callback: (Deprecated) Callback called when the Job is known.
        :param on_new_job_callback: Callback called when the Job is known.
        :param location: Job location.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        """
        name = self.build_dataflow_job_name(job_name, append_job_name)

        environment = self._update_environment(
            variables=variables,
            environment=environment,
        )

        job: dict[str, str] = self.send_launch_template_request(
            project_id=project_id,
            location=location,
            gcs_path=dataflow_template,
            job_name=name,
            parameters=parameters,
            environment=environment,
        )

        if on_new_job_id_callback:
            warnings.warn(
                "on_new_job_id_callback is Deprecated. Please start using on_new_job_callback",
                AirflowProviderDeprecationWarning,
                stacklevel=3,
            )
            on_new_job_id_callback(job["id"])

        if on_new_job_callback:
            on_new_job_callback(job)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            job_id=job["id"],
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
            expected_terminal_state=self.expected_terminal_state,
        )
        jobs_controller.wait_for_done()
        return job

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def launch_job_with_template(
        self,
        *,
        job_name: str,
        variables: dict,
        parameters: dict,
        dataflow_template: str,
        project_id: str,
        append_job_name: bool = True,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        environment: dict | None = None,
    ) -> dict[str, str]:
        """
        Launch a Dataflow job with a Classic Template and exit without waiting for its completion.

        :param job_name: The name of the job.
        :param variables: Map of job runtime environment options.
            It will update environment argument if passed.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        :param parameters: Parameters for the template
        :param dataflow_template: GCS path to the template.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :param location: Job location.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__
        :return: the Dataflow job response
        """
        name = self.build_dataflow_job_name(job_name, append_job_name)
        environment = self._update_environment(
            variables=variables,
            environment=environment,
        )
        job: dict[str, str] = self.send_launch_template_request(
            project_id=project_id,
            location=location,
            gcs_path=dataflow_template,
            job_name=name,
            parameters=parameters,
            environment=environment,
        )
        return job

    def _update_environment(self, variables: dict, environment: dict | None = None) -> dict:
        environment = environment or {}
        # available keys for runtime environment are listed here:
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment_keys = {
            "numWorkers",
            "maxWorkers",
            "zone",
            "serviceAccountEmail",
            "tempLocation",
            "bypassTempDirValidation",
            "machineType",
            "additionalExperiments",
            "network",
            "subnetwork",
            "additionalUserLabels",
            "kmsKeyName",
            "ipConfiguration",
            "workerRegion",
            "workerZone",
        }

        def _check_one(key, val):
            if key in environment:
                self.log.warning(
                    "%r parameter in 'variables' will override the same one passed in 'environment'!",
                    key,
                )
            return key, val

        environment.update(_check_one(key, val) for key, val in variables.items() if key in environment_keys)

        return environment

    def send_launch_template_request(
        self,
        *,
        project_id: str,
        location: str,
        gcs_path: str,
        job_name: str,
        parameters: dict,
        environment: dict,
    ) -> dict[str, str]:
        service: Resource = self.get_conn()
        request = (
            service.projects()
            .locations()
            .templates()
            .launch(
                projectId=project_id,
                location=location,
                gcsPath=gcs_path,
                body={
                    "jobName": job_name,
                    "parameters": parameters,
                    "environment": environment,
                },
            )
        )
        response: dict = request.execute(num_retries=self.num_retries)
        return response["job"]

    @GoogleBaseHook.fallback_to_default_project_id
    def start_flex_template(
        self,
        body: dict,
        location: str,
        project_id: str,
        on_new_job_id_callback: Callable[[str], None] | None = None,
        on_new_job_callback: Callable[[dict], None] | None = None,
    ) -> dict[str, str]:
        """
        Launch a Dataflow job with a Flex Template and wait for its completion.

        :param body: The request body. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#request-body
        :param location: The location of the Dataflow job (for example europe-west1)
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :param on_new_job_id_callback: (Deprecated) A callback that is called when a Job ID is detected.
        :param on_new_job_callback: A callback that is called when a Job is detected.
        :return: the Job
        """
        service: Resource = self.get_conn()
        request = (
            service.projects()
            .locations()
            .flexTemplates()
            .launch(projectId=project_id, body=body, location=location)
        )
        response: dict = request.execute(num_retries=self.num_retries)
        job = response["job"]
        job_id: str = job["id"]

        if on_new_job_id_callback:
            warnings.warn(
                "on_new_job_id_callback is Deprecated. Please start using on_new_job_callback",
                AirflowProviderDeprecationWarning,
                stacklevel=3,
            )
            on_new_job_id_callback(job_id)

        if on_new_job_callback:
            on_new_job_callback(job)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
        )
        jobs_controller.wait_for_done()

        return jobs_controller.get_jobs(refresh=True)[0]

    @GoogleBaseHook.fallback_to_default_project_id
    def launch_job_with_flex_template(
        self,
        body: dict,
        location: str,
        project_id: str,
    ) -> dict[str, str]:
        """
        Launch a Dataflow Job with a Flex Template and exit without waiting for the job completion.

        :param body: The request body. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#request-body
        :param location: The location of the Dataflow job (for example europe-west1)
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :return: a Dataflow job response
        """
        service: Resource = self.get_conn()
        request = (
            service.projects()
            .locations()
            .flexTemplates()
            .launch(projectId=project_id, body=body, location=location)
        )
        response: dict = request.execute(num_retries=self.num_retries)
        return response["job"]

    @GoogleBaseHook.fallback_to_default_project_id
    def launch_beam_yaml_job(
        self,
        *,
        job_name: str,
        yaml_pipeline_file: str,
        append_job_name: bool,
        jinja_variables: dict[str, str] | None,
        options: dict[str, Any] | None,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> str:
        """
        Launch a Dataflow YAML job and run it until completion.

        :param job_name: The unique name to assign to the Cloud Dataflow job.
        :param yaml_pipeline_file: Path to a file defining the YAML pipeline to run.
            Must be a local file or a URL beginning with 'gs://'.
        :param append_job_name: Set to True if a unique suffix has to be appended to the `job_name`.
        :param jinja_variables: A dictionary of Jinja2 variables to be used in reifying the yaml pipeline file.
        :param options: Additional gcloud or Beam job parameters.
            It must be a dictionary with the keys matching the optional flag names in gcloud.
            The list of supported flags can be found at: `https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml/run`.
            Note that if a flag does not require a value, then its dictionary value must be either True or None.
            For example, the `--log-http` flag can be passed as {'log-http': True}.
        :param project_id: The ID of the GCP project that owns the job.
        :param location: Region ID of the job's regional endpoint. Defaults to 'us-central1'.
        :param on_new_job_callback: Callback function that passes the job to the operator once known.
        :return: Job ID.
        """
        gcp_flags = {
            "yaml-pipeline-file": yaml_pipeline_file,
            "project": project_id,
            "format": "value(job.id)",
            "region": location,
        }

        if jinja_variables:
            gcp_flags["jinja-variables"] = json.dumps(jinja_variables)

        if options:
            gcp_flags.update(options)

        job_name = self.build_dataflow_job_name(job_name, append_job_name)
        cmd = self._build_gcloud_command(
            command=["gcloud", "dataflow", "yaml", "run", job_name], parameters=gcp_flags
        )
        job_id = self._create_dataflow_job_with_gcloud(cmd=cmd)
        return job_id

    def _build_gcloud_command(self, command: list[str], parameters: dict[str, str]) -> list[str]:
        _parameters = deepcopy(parameters)
        if self.impersonation_chain:
            if isinstance(self.impersonation_chain, str):
                impersonation_account = self.impersonation_chain
            elif len(self.impersonation_chain) == 1:
                impersonation_account = self.impersonation_chain[0]
            else:
                raise AirflowException(
                    "Chained list of accounts is not supported, please specify only one service account."
                )
            _parameters["impersonate-service-account"] = impersonation_account
        return [*command, *(beam_options_to_args(_parameters))]

    def _create_dataflow_job_with_gcloud(self, cmd: list[str]) -> str:
        """Create a Dataflow job with a gcloud command and return the job's ID."""
        self.log.info("Executing command: %s", " ".join(shlex.quote(c) for c in cmd))
        success_code = 0

        with self.provide_authorized_gcloud():
            proc = subprocess.run(cmd, capture_output=True)

        if proc.returncode != success_code:
            stderr_last_20_lines = "\n".join(proc.stderr.decode().strip().splitlines()[-20:])
            raise AirflowException(
                f"Process exit with non-zero exit code. Exit code: {proc.returncode}. Error Details : "
                f"{stderr_last_20_lines}"
            )

        job_id = proc.stdout.decode().strip()
        self.log.info("Created job's ID: %s", job_id)

        return job_id

    @staticmethod
    def extract_job_id(job: dict) -> str:
        try:
            return job["id"]
        except KeyError:
            raise AirflowException(
                "While reading job object after template execution error occurred. Job object has no id."
            )

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    @deprecated(
        planned_removal_date="March 01, 2025",
        use_instead="airflow.providers.apache.beam.hooks.beam.start.start_python_pipeline method, "
        "providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done",
        instructions="Please use airflow.providers.apache.beam.hooks.beam.start.start_python_pipeline method "
        "to start pipeline and providers.google.cloud.hooks.dataflow.DataflowHook.wait_for_done method "
        "to wait for the required pipeline state instead.",
        category=AirflowProviderDeprecationWarning,
    )
    def start_python_dataflow(
        self,
        job_name: str,
        variables: dict,
        dataflow: str,
        py_options: list[str],
        project_id: str,
        py_interpreter: str = "python3",
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        append_job_name: bool = True,
        on_new_job_id_callback: Callable[[str], None] | None = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ):
        """
        Start Dataflow job.

        :param job_name: The name of the job.
        :param variables: Variables passed to the job.
        :param dataflow: Name of the Dataflow process.
        :param py_options: Additional options.
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :param py_interpreter: Python version of the beam pipeline.
            If None, this defaults to the python3.
            To track python versions supported by beam and related
            issues check: https://issues.apache.org/jira/browse/BEAM-1251
        :param py_requirements: Additional python package(s) to install.
            If a value is passed to this parameter, a new virtual environment has been created with
            additional packages installed.

            You could also install the apache-beam package if it is not installed on your system or you want
            to use a different version.
        :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
            See virtualenv documentation for more information.

            This option is only relevant if the ``py_requirements`` parameter is not None.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :param location: Job location.
        """
        name = self.build_dataflow_job_name(job_name, append_job_name)
        variables["job_name"] = name
        variables["region"] = location
        variables["project"] = project_id

        self.beam_hook.start_python_pipeline(
            variables=variables,
            py_file=dataflow,
            py_options=py_options,
            py_interpreter=py_interpreter,
            py_requirements=py_requirements,
            py_system_site_packages=py_system_site_packages,
            process_line_callback=process_line_and_extract_dataflow_job_id_callback(on_new_job_id_callback),
        )

        self.wait_for_done(
            job_name=name,
            location=location,
            job_id=self.job_id,
        )

    @staticmethod
    def build_dataflow_job_name(job_name: str, append_job_name: bool = True) -> str:
        """Build Dataflow job name."""
        base_job_name = str(job_name).replace("_", "-")

        if not re.fullmatch(r"[a-z]([-a-z0-9]*[a-z0-9])?", base_job_name):
            raise ValueError(
                f"Invalid job_name ({base_job_name}); the name must consist of only the characters "
                f"[-a-z0-9], starting with a letter and ending with a letter or number "
            )

        if append_job_name:
            safe_job_name = f"{base_job_name}-{uuid.uuid4()!s:.8}"
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def is_job_dataflow_running(
        self,
        name: str,
        project_id: str,
        location: str | None = None,
        variables: dict | None = None,
    ) -> bool:
        """
        Check if job is still running in dataflow.

        :param name: The name of the job.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: Job location.
        :return: True if job is running.
        """
        if variables:
            warnings.warn(
                "The variables parameter has been deprecated. You should pass project_id using "
                "the project_id parameter.",
                AirflowProviderDeprecationWarning,
                stacklevel=4,
            )

        if location is None:
            location = DEFAULT_DATAFLOW_LOCATION
            warnings.warn(
                "The location argument will be become mandatory in future versions, "
                f"currently, it defaults to {DEFAULT_DATAFLOW_LOCATION}, please set the location explicitly.",
                AirflowProviderDeprecationWarning,
                stacklevel=4,
            )

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            location=location,
            poll_sleep=self.poll_sleep,
            drain_pipeline=self.drain_pipeline,
            num_retries=self.num_retries,
            cancel_timeout=self.cancel_timeout,
        )
        return jobs_controller.is_job_running()

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        project_id: str,
        job_name: str | None = None,
        job_id: str | None = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        """
        Cancel the job with the specified name prefix or Job ID.

        Parameter ``name`` and ``job_id`` are mutually exclusive.

        :param job_name: Name prefix specifying which jobs are to be canceled.
        :param job_id: Job ID specifying which jobs are to be canceled.
        :param location: Job location.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=job_name,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            drain_pipeline=self.drain_pipeline,
            num_retries=self.num_retries,
            cancel_timeout=self.cancel_timeout,
        )
        jobs_controller.cancel()

    @GoogleBaseHook.fallback_to_default_project_id
    def start_sql_job(
        self,
        job_name: str,
        query: str,
        options: dict[str, Any],
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        on_new_job_id_callback: Callable[[str], None] | None = None,
        on_new_job_callback: Callable[[dict], None] | None = None,
    ):
        """
        Start Dataflow SQL query.

        :param job_name: The unique name to assign to the Cloud Dataflow job.
        :param query: The SQL query to execute.
        :param options: Job parameters to be executed.
            For more information, look at:
            `https://cloud.google.com/sdk/gcloud/reference/beta/dataflow/sql/query
            <gcloud beta dataflow sql query>`__
            command reference
        :param location: The location of the Dataflow job (for example europe-west1)
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :param on_new_job_id_callback: (Deprecated) Callback called when the job ID is known.
        :param on_new_job_callback: Callback called when the job is known.
        :return: the new job object
        """
        gcp_options = {
            "project": project_id,
            "format": "value(job.id)",
            "job-name": job_name,
            "region": location,
        }
        cmd = self._build_gcloud_command(
            command=["gcloud", "dataflow", "sql", "query", query], parameters={**gcp_options, **options}
        )
        self.log.info("Executing command: %s", " ".join(shlex.quote(c) for c in cmd))
        with self.provide_authorized_gcloud():
            proc = subprocess.run(cmd, capture_output=True)
        self.log.info("Output: %s", proc.stdout.decode())
        self.log.warning("Stderr: %s", proc.stderr.decode())
        self.log.info("Exit code %d", proc.returncode)
        stderr_last_20_lines = "\n".join(proc.stderr.decode().strip().splitlines()[-20:])
        if proc.returncode != 0:
            raise AirflowException(
                f"Process exit with non-zero exit code. Exit code: {proc.returncode} Error Details : "
                f"{stderr_last_20_lines}"
            )
        job_id = proc.stdout.decode().strip()

        self.log.info("Created job ID: %s", job_id)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
            drain_pipeline=self.drain_pipeline,
            wait_until_finished=self.wait_until_finished,
        )
        job = jobs_controller.get_jobs(refresh=True)[0]

        if on_new_job_id_callback:
            warnings.warn(
                "on_new_job_id_callback is Deprecated. Please start using on_new_job_callback",
                AirflowProviderDeprecationWarning,
                stacklevel=3,
            )
            on_new_job_id_callback(cast(str, job.get("id")))

        if on_new_job_callback:
            on_new_job_callback(job)

        jobs_controller.wait_for_done()
        return jobs_controller.get_jobs(refresh=True)[0]

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Get the job with the specified Job ID.

        :param job_id: Job ID to get.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        :return: the Job
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_job_metrics_by_id(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Get the job metrics with the specified Job ID.

        :param job_id: Job ID to get.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        :return: the JobMetrics. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/JobMetrics
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_metrics_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_job_messages_by_id(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> list[dict]:
        """
        Get the job messages with the specified Job ID.

        :param job_id: Job ID to get.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: Job location.
        :return: the list of JobMessages. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#JobMessage
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_messages_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def fetch_job_autoscaling_events_by_id(
        self,
        job_id: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> list[dict]:
        """
        Get the job autoscaling events with the specified Job ID.

        :param job_id: Job ID to get.
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: Job location.
        :return: the list of AutoscalingEvents. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/ListJobMessagesResponse#autoscalingevent
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        return jobs_controller.fetch_job_autoscaling_events_by_id(job_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_done(
        self,
        job_name: str,
        location: str,
        project_id: str,
        job_id: str | None = None,
        multiple_jobs: bool = False,
    ) -> None:
        """
        Wait for Dataflow job.

        :param job_name: The 'jobName' to use when executing the DataFlow job
            (templated). This ends up being set in the pipeline options, so any entry
            with key ``'jobName'`` in ``options`` will be overwritten.
        :param location: location the job is running
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param job_id: a Dataflow job ID
        :param multiple_jobs: If pipeline creates multiple jobs then monitor all jobs
        """
        job_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=job_name,
            location=location,
            poll_sleep=self.poll_sleep,
            job_id=job_id or self.job_id,
            num_retries=self.num_retries,
            multiple_jobs=multiple_jobs,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
        )
        job_controller.wait_for_done()

    @GoogleBaseHook.fallback_to_default_project_id
    def is_job_done(self, location: str, project_id: str, job_id: str) -> bool:
        """
        Check that Dataflow job is started(for streaming job) or finished(for batch job).

        :param location: location the job is running
        :param project_id: Google Cloud project ID in which to start a job
        :param job_id: Dataflow job ID
        """
        job_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            location=location,
        )
        job = job_controller.fetch_job_by_id(job_id)

        return job_controller._check_dataflow_job_state(job)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_data_pipeline(
        self,
        body: dict,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ):
        """
        Create a new Dataflow Data Pipelines instance.

        :param body: The request body (contains instance of Pipeline). See:
            https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/projects.locations.pipelines/create#request-body
        :param project_id: The ID of the GCP project that owns the job.
        :param location: The location to direct the Data Pipelines instance to (for example us-central1).

        Returns the created Data Pipelines instance in JSON representation.
        """
        parent = self.build_parent_name(project_id, location)
        service = self.get_pipelines_conn()
        request = (
            service.projects()
            .locations()
            .pipelines()
            .create(
                parent=parent,
                body=body,
            )
        )
        response = request.execute(num_retries=self.num_retries)
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def get_data_pipeline(
        self,
        pipeline_name: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Retrieve a new Dataflow Data Pipelines instance.

        :param pipeline_name: The display name of the pipeline. In example
            projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
        :param project_id: The ID of the GCP project that owns the job.
        :param location: The location to direct the Data Pipelines instance to (for example us-central1).

        Returns the created Data Pipelines instance in JSON representation.
        """
        parent = self.build_parent_name(project_id, location)
        service = self.get_pipelines_conn()
        request = (
            service.projects()
            .locations()
            .pipelines()
            .get(
                name=f"{parent}/pipelines/{pipeline_name}",
            )
        )
        response = request.execute(num_retries=self.num_retries)
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def run_data_pipeline(
        self,
        pipeline_name: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict:
        """
        Run a Dataflow Data Pipeline Instance.

        :param pipeline_name: The display name of the pipeline. In example
            projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
        :param project_id: The ID of the GCP project that owns the job.
        :param location: The location to direct the Data Pipelines instance to (for example us-central1).

        Returns the created Job in JSON representation.
        """
        parent = self.build_parent_name(project_id, location)
        service = self.get_pipelines_conn()
        request = (
            service.projects()
            .locations()
            .pipelines()
            .run(
                name=f"{parent}/pipelines/{pipeline_name}",
                body={},
            )
        )
        response = request.execute(num_retries=self.num_retries)
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_data_pipeline(
        self,
        pipeline_name: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> dict | None:
        """
        Delete a Dataflow Data Pipelines Instance.

        :param pipeline_name: The display name of the pipeline. In example
            projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
        :param project_id: The ID of the GCP project that owns the job.
        :param location: The location to direct the Data Pipelines instance to (for example us-central1).

        Returns the created Job in JSON representation.
        """
        parent = self.build_parent_name(project_id, location)
        service = self.get_pipelines_conn()
        request = (
            service.projects()
            .locations()
            .pipelines()
            .delete(
                name=f"{parent}/pipelines/{pipeline_name}",
            )
        )
        response = request.execute(num_retries=self.num_retries)
        return response

    @staticmethod
    def build_parent_name(project_id: str, location: str):
        return f"projects/{project_id}/locations/{location}"


class AsyncDataflowHook(GoogleBaseAsyncHook):
    """Async hook class for dataflow service."""

    sync_hook_class = DataflowHook

    async def initialize_client(self, client_class):
        """
        Initialize object of the given class.

        Method is used to initialize asynchronous client. Because of the big amount of the classes which are
        used for Dataflow service it was decided to initialize them the same way with credentials which are
        received from the method of the GoogleBaseHook class.
        :param client_class: Class of the Google cloud SDK
        """
        credentials = (await self.get_sync_hook()).get_credentials()
        return client_class(
            credentials=credentials,
        )

    async def get_project_id(self) -> str:
        project_id = (await self.get_sync_hook()).project_id
        return project_id

    async def get_job(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        job_view: int = JobView.JOB_VIEW_SUMMARY,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> Job:
        """
        Get the job with the specified Job ID.

        :param job_id: Job ID to get.
        :param project_id: the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param job_view: Optional. JobView object which determines representation of the returned data
        :param location: Optional. The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        """
        project_id = project_id or (await self.get_project_id())
        client = await self.initialize_client(JobsV1Beta3AsyncClient)

        request = GetJobRequest(
            {
                "project_id": project_id,
                "job_id": job_id,
                "view": job_view,
                "location": location,
            }
        )

        job = await client.get_job(
            request=request,
        )

        return job

    async def get_job_status(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        job_view: int = JobView.JOB_VIEW_SUMMARY,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> JobState:
        """
        Get the job status with the specified Job ID.

        :param job_id: Job ID to get.
        :param project_id: the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param job_view: Optional. JobView object which determines representation of the returned data
        :param location: Optional. The location of the Dataflow job (for example europe-west1). See:
            https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
        """
        job = await self.get_job(
            project_id=project_id,
            job_id=job_id,
            job_view=job_view,
            location=location,
        )
        state = job.current_state
        return state

    async def list_jobs(
        self,
        jobs_filter: int | None = None,
        project_id: str | None = PROVIDE_PROJECT_ID,
        location: str | None = DEFAULT_DATAFLOW_LOCATION,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> ListJobsAsyncPager:
        """
        List jobs.

        For detail see:
        https://cloud.google.com/python/docs/reference/dataflow/latest/google.cloud.dataflow_v1beta3.types.ListJobsRequest

        :param jobs_filter: Optional. This field filters out and returns jobs in the specified job state.
        :param project_id: Optional. The Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: Optional. The location of the Dataflow job (for example europe-west1).
        :param page_size: Optional. If there are many jobs, limit response to at most this many.
        :param page_token: Optional. Set this to the 'next_page_token' field of a previous response to request
            additional results in a long list.
        """
        project_id = project_id or (await self.get_project_id())
        client = await self.initialize_client(JobsV1Beta3AsyncClient)
        request: ListJobsRequest = ListJobsRequest(
            {
                "project_id": project_id,
                "location": location,
                "filter": jobs_filter,
                "page_size": page_size,
                "page_token": page_token,
            }
        )
        page_result: ListJobsAsyncPager = await client.list_jobs(request=request)
        return page_result

    async def list_job_messages(
        self,
        job_id: str,
        project_id: str | None = PROVIDE_PROJECT_ID,
        minimum_importance: int = JobMessageImportance.JOB_MESSAGE_BASIC,
        page_size: int | None = None,
        page_token: str | None = None,
        start_time: Timestamp | None = None,
        end_time: Timestamp | None = None,
        location: str | None = DEFAULT_DATAFLOW_LOCATION,
    ) -> ListJobMessagesAsyncPager:
        """
        Return ListJobMessagesAsyncPager object from MessagesV1Beta3AsyncClient.

        This method wraps around a similar method of MessagesV1Beta3AsyncClient. ListJobMessagesAsyncPager can be iterated
        over to extract messages associated with a specific Job ID.

        For more details see the MessagesV1Beta3AsyncClient method description at:
        https://cloud.google.com/python/docs/reference/dataflow/latest/google.cloud.dataflow_v1beta3.services.messages_v1_beta3.MessagesV1Beta3AsyncClient

        :param job_id: ID of the Dataflow job to get messages about.
        :param project_id: Optional. The Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param minimum_importance: Optional. Filter to only get messages with importance >= level.
            For more details see the description at:
            https://cloud.google.com/python/docs/reference/dataflow/latest/google.cloud.dataflow_v1beta3.types.JobMessageImportance
        :param page_size: Optional. If specified, determines the maximum number of messages to return.
            If unspecified, the service may choose an appropriate default, or may return an arbitrarily large number of results.
        :param page_token: Optional. If supplied, this should be the value of next_page_token returned by an earlier call.
            This will cause the next page of results to be returned.
        :param start_time: Optional. If specified, return only messages with timestamps >= start_time.
            The default is the job creation time (i.e. beginning of messages).
        :param end_time: Optional. If specified, return only messages with timestamps < end_time. The default is the current time.
        :param location: Optional. The [regional endpoint] (https://cloud.google.com/dataflow/docs/concepts/regional-endpoints) that contains
            the job specified by job_id.
        """
        project_id = project_id or (await self.get_project_id())
        client = await self.initialize_client(MessagesV1Beta3AsyncClient)
        request = ListJobMessagesRequest(
            {
                "project_id": project_id,
                "job_id": job_id,
                "minimum_importance": minimum_importance,
                "page_size": page_size,
                "page_token": page_token,
                "start_time": start_time,
                "end_time": end_time,
                "location": location,
            }
        )
        page_results: ListJobMessagesAsyncPager = await client.list_job_messages(request=request)
        return page_results

    async def get_job_metrics(
        self,
        job_id: str,
        project_id: str | None = PROVIDE_PROJECT_ID,
        start_time: Timestamp | None = None,
        location: str | None = DEFAULT_DATAFLOW_LOCATION,
    ) -> JobMetrics:
        """
        Return JobMetrics object from MetricsV1Beta3AsyncClient.

        This method wraps around a similar method of MetricsV1Beta3AsyncClient.

        For more details see the MetricsV1Beta3AsyncClient method description at:
        https://cloud.google.com/python/docs/reference/dataflow/latest/google.cloud.dataflow_v1beta3.services.metrics_v1_beta3.MetricsV1Beta3AsyncClient

        :param job_id: ID of the Dataflow job to get metrics for.
        :param project_id: Optional. The Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param start_time: Optional. Return only metric data that has changed since this time.
            Default is to return all information about all metrics for the job.
        :param location: Optional. The [regional endpoint] (https://cloud.google.com/dataflow/docs/concepts/regional-endpoints) that contains
            the job specified by job_id.
        """
        project_id = project_id or (await self.get_project_id())
        client: MetricsV1Beta3AsyncClient = await self.initialize_client(MetricsV1Beta3AsyncClient)
        request = GetJobMetricsRequest(
            {
                "project_id": project_id,
                "job_id": job_id,
                "start_time": start_time,
                "location": location,
            }
        )
        job_metrics: JobMetrics = await client.get_job_metrics(request=request)
        return job_metrics
