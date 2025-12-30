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
"""This module contains Google Dataflow operators."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError

from airflow.configuration import conf
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    DataflowHook,
)
from airflow.providers.google.cloud.links.dataflow import DataflowJobLink, DataflowPipelineLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.dataflow import (
    DataflowStartYamlJobTrigger,
    TemplateJobStartTrigger,
)
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class CheckJobRunning(Enum):
    """
    Helper enum for choosing what to do if job is already running.

    IgnoreJob - do not check if running
    FinishIfRunning - finish current dag run with no action
    WaitForRun - wait for job to finish and then continue with new job
    """

    IgnoreJob = 1
    FinishIfRunning = 2
    WaitForRun = 3


class DataflowConfiguration:
    """
    Dataflow configuration for BeamRunJavaPipelineOperator and BeamRunPythonPipelineOperator.

    .. seealso::
        :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`
        and :class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`.

    :param job_name: The 'jobName' to use when executing the Dataflow job
        (templated). This ends up being set in the pipeline options, so any entry
        with key ``'jobName'`` or  ``'job_name'``in ``options`` will be overwritten.
    :param append_job_name: True if unique suffix has to be appended to job name.
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: Job location.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

        .. warning::
            This option requires Apache Beam 2.39.0 or newer.

    :param drain_pipeline: Optional, set to True if want to stop streaming job by draining it
        instead of canceling during killing task instance. See:
        https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline
    :param cancel_timeout: How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed. (optional) default to 300s
    :param wait_until_finished: (Optional)
        If True, wait for the end of pipeline execution before exiting.
        If False, only submits job.
        If None, default behavior.

        The default behavior depends on the type of pipeline:

        * for the streaming pipeline, wait for jobs to start,
        * for the batch pipeline, wait for the jobs to complete.

        .. warning::
            You cannot call ``PipelineResult.wait_until_finish`` method in your pipeline code for the operator
            to work properly. i. e. you must use asynchronous execution. Otherwise, your pipeline will
            always wait until finished. For more information, look at:
            `Asynchronous execution
            <https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#python_10>`__

        The process of starting the Dataflow job in Airflow consists of two steps:
        * running a subprocess and reading the stderr/stderr log for the job id.
        * loop waiting for the end of the job ID from the previous step by checking its status.

        Step two is started just after step one has finished, so if you have wait_until_finished in your
        pipeline code, step two will not start until the process stops. When this process stops,
        steps two will run, but it will only execute one iteration as the job will be in a terminal state.

        If you in your pipeline do not call the wait_for_pipeline method but pass wait_until_finish=True
        to the operator, the second loop will wait for the job's terminal state.

        If you in your pipeline do not call the wait_for_pipeline method, and pass wait_until_finish=False
        to the operator, the second loop will check once is job not in terminal state and exit the loop.
    :param multiple_jobs: If pipeline creates multiple jobs then monitor all jobs. Supported only by
        :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`.
    :param check_if_running: Before running job, validate that a previous run is not in process.
        Supported only by:
        :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator`.
    :param service_account: Run the job as a specific service account, instead of the default GCE robot.
    :param max_num_workers: Maximum amount of workers that will be used for Dataflow job execution.
    """

    template_fields: Sequence[str] = ("job_name", "location")

    def __init__(
        self,
        *,
        job_name: str | None = None,
        append_job_name: bool = True,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        drain_pipeline: bool = False,
        cancel_timeout: int | None = 5 * 60,
        wait_until_finished: bool | None = None,
        multiple_jobs: bool | None = None,
        check_if_running: CheckJobRunning = CheckJobRunning.WaitForRun,
        service_account: str | None = None,
        max_num_workers: int | None = None,
    ) -> None:
        self.job_name = job_name
        self.append_job_name = append_job_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.multiple_jobs = multiple_jobs
        self.check_if_running = check_if_running
        self.service_account = service_account
        self.max_num_workers = max_num_workers


class DataflowTemplatedJobStartOperator(GoogleCloudBaseOperator):
    """
    Start a Dataflow job with a classic template; the parameters of the operation will be passed to the job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowTemplatedJobStartOperator`

    :param template: The reference to the Dataflow template.
    :param job_name: The 'jobName' to use when executing the Dataflow template
        (templated).
    :param options: Map of job runtime environment options.
        It will update environment argument if passed.

        .. seealso::
            For more information on possible configurations, look at the API documentation
            `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
            <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

    :param dataflow_default_options: Map of default job environment options.
    :param parameters: Map of job specific parameters for the template.
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: Job location.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param environment: Optional, Map of job runtime environment options.

        .. seealso::
            For more information on possible configurations, look at the API documentation
            `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
            <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__
    :param cancel_timeout: How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed.
    :param append_job_name: True if unique suffix has to be appended to job name.
    :param wait_until_finished: (Optional)
        If True, wait for the end of pipeline execution before exiting.
        If False, only submits job.
        If None, default behavior.

        The default behavior depends on the type of pipeline:

        * for the streaming pipeline, wait for jobs to start,
        * for the batch pipeline, wait for the jobs to complete.

        .. warning::

            You cannot call ``PipelineResult.wait_until_finish`` method in your pipeline code for the operator
            to work properly. i. e. you must use asynchronous execution. Otherwise, your pipeline will
            always wait until finished. For more information, look at:
            `Asynchronous execution
            <https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#python_10>`__

        The process of starting the Dataflow job in Airflow consists of two steps:

        * running a subprocess and reading the stderr/stderr log for the job id.
        * loop waiting for the end of the job ID from the previous step.
          This loop checks the status of the job.

        Step two is started just after step one has finished, so if you have wait_until_finished in your
        pipeline code, step two will not start until the process stops. When this process stops,
        steps two will run, but it will only execute one iteration as the job will be in a terminal state.

        If you in your pipeline do not call the wait_for_pipeline method but pass wait_until_finish=True
        to the operator, the second loop will wait for the job's terminal state.

        If you in your pipeline do not call the wait_for_pipeline method, and pass wait_until_finish=False
        to the operator, the second loop will check once is job not in terminal state and exit the loop.
    :param expected_terminal_state: The expected terminal state of the operator on which the corresponding
        Airflow task succeeds. When not specified, it will be determined by the hook.

    It's a good practice to define dataflow_* parameters in the default_args of the dag
    like the project, zone and staging location.

    .. seealso::
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/LaunchTemplateParameters
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment

    .. code-block:: python

       default_args = {
           "dataflow_default_options": {
               "zone": "europe-west1-d",
               "tempLocation": "gs://my-staging-bucket/staging/",
           }
       }

    You need to pass the path to your dataflow template as a file reference with the
    ``template`` parameter. Use ``parameters`` to pass on parameters to your job.
    Use ``environment`` to pass on runtime environment variables to your job.

    .. code-block:: python

       t1 = DataflowTemplatedJobStartOperator(
           task_id="dataflow_example",
           template="{{var.value.gcp_dataflow_base}}",
           parameters={
               "inputFile": "gs://bucket/input/my_input.txt",
               "outputFile": "gs://bucket/output/my_output.txt",
           },
           gcp_conn_id="airflow-conn-id",
           dag=my_dag,
       )

    ``template``, ``dataflow_default_options``, ``parameters``, and ``job_name`` are
    templated, so you can use variables in them.

    Note that ``dataflow_default_options`` is expected to save high-level options
    for project information, which apply to all dataflow operators in the DAG.

        .. seealso::
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3
            /LaunchTemplateParameters
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
            For more detail on job template execution have a look at the reference:
            https://cloud.google.com/dataflow/docs/templates/executing-templates

    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields: Sequence[str] = (
        "template",
        "job_name",
        "options",
        "parameters",
        "project_id",
        "location",
        "gcp_conn_id",
        "impersonation_chain",
        "environment",
        "dataflow_default_options",
    )
    ui_color = "#0273d4"
    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        *,
        template: str,
        project_id: str = PROVIDE_PROJECT_ID,
        job_name: str = "{{task.task_id}}",
        options: dict[str, Any] | None = None,
        dataflow_default_options: dict[str, Any] | None = None,
        parameters: dict[str, str] | None = None,
        location: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        environment: dict | None = None,
        cancel_timeout: int | None = 10 * 60,
        wait_until_finished: bool | None = None,
        append_job_name: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        expected_terminal_state: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.template = template
        self.job_name = job_name
        self.options = options or {}
        self.dataflow_default_options = dataflow_default_options or {}
        self.parameters = parameters or {}
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.environment = environment
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.append_job_name = append_job_name
        self.deferrable = deferrable
        self.expected_terminal_state = expected_terminal_state

        self.job: dict[str, str] | None = None

        self._validate_deferrable_params()

    def _validate_deferrable_params(self):
        if self.deferrable and self.wait_until_finished:
            raise ValueError(
                "Conflict between deferrable and wait_until_finished parameters "
                "because it makes operator as blocking when it requires to be deferred. "
                "It should be True as deferrable parameter or True as wait_until_finished."
            )

        if self.deferrable and self.wait_until_finished is None:
            self.wait_until_finished = False

    @cached_property
    def hook(self) -> DataflowHook:
        hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
            expected_terminal_state=self.expected_terminal_state,
        )
        return hook

    def execute(self, context: Context):
        def set_current_job(current_job):
            self.job = current_job
            DataflowJobLink.persist(
                context=context,
                project_id=self.project_id,
                region=self.location,
                job_id=self.job.get("id"),
            )

        options = self.dataflow_default_options
        options.update(self.options)

        if not self.location:
            self.location = DEFAULT_DATAFLOW_LOCATION

        if not self.deferrable:
            self.job = self.hook.start_template_dataflow(
                job_name=self.job_name,
                variables=options,
                parameters=self.parameters,
                dataflow_template=self.template,
                on_new_job_callback=set_current_job,
                project_id=self.project_id,
                location=self.location,
                environment=self.environment,
                append_job_name=self.append_job_name,
            )
            job_id = self.hook.extract_job_id(self.job)
            context["task_instance"].xcom_push(key="job_id", value=job_id)
            return job_id

        self.job = self.hook.launch_job_with_template(
            job_name=self.job_name,
            variables=options,
            parameters=self.parameters,
            dataflow_template=self.template,
            project_id=self.project_id,
            append_job_name=self.append_job_name,
            location=self.location,
            environment=self.environment,
        )
        job_id = self.hook.extract_job_id(self.job)
        DataflowJobLink.persist(
            context=context, project_id=self.project_id, region=self.location, job_id=job_id
        )
        self.defer(
            trigger=TemplateJobStartTrigger(
                project_id=self.project_id,
                job_id=job_id,
                location=self.location,
                gcp_conn_id=self.gcp_conn_id,
                poll_sleep=self.poll_sleep,
                impersonation_chain=self.impersonation_chain,
                cancel_timeout=self.cancel_timeout,
            ),
            method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> str:
        """Execute after trigger finishes its work."""
        if event["status"] in ("error", "stopped"):
            self.log.info("status: %s, msg: %s", event["status"], event["message"])
            raise AirflowException(event["message"])

        job_id = event["job_id"]
        context["task_instance"].xcom_push(key="job_id", value=job_id)
        self.log.info("Task %s completed with response %s", self.task_id, event["message"])
        return job_id

    def on_kill(self) -> None:
        self.log.info("On kill.")
        if self.job is not None:
            self.log.info("Cancelling job %s", self.job_name)
            self.hook.cancel_job(
                job_name=self.job_name,
                job_id=self.job.get("id"),
                project_id=self.job.get("projectId"),
                location=self.job.get("location"),
            )


class DataflowStartFlexTemplateOperator(GoogleCloudBaseOperator):
    """
    Starts a Dataflow Job with a Flex Template.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowStartFlexTemplateOperator`

    :param body: The request body. See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#request-body
    :param location: The location of the Dataflow job (for example europe-west1)
    :param project_id: The ID of the GCP project that owns the job.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
    :param drain_pipeline: Optional, set to True if want to stop streaming job by draining it
        instead of canceling during killing task instance. See:
        https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline
    :param cancel_timeout: How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed.
    :param wait_until_finished: (Optional)
        If True, wait for the end of pipeline execution before exiting.
        If False, only submits job.
        If None, default behavior.

        The default behavior depends on the type of pipeline:

        * for the streaming pipeline, wait for jobs to start,
        * for the batch pipeline, wait for the jobs to complete.

        .. warning::

            You cannot call ``PipelineResult.wait_until_finish`` method in your pipeline code for the operator
            to work properly. i. e. you must use asynchronous execution. Otherwise, your pipeline will
            always wait until finished. For more information, look at:
            `Asynchronous execution
            <https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#python_10>`__

        The process of starting the Dataflow job in Airflow consists of two steps:

        * running a subprocess and reading the stderr/stderr log for the job id.
        * loop waiting for the end of the job ID from the previous step.
          This loop checks the status of the job.

        Step two is started just after step one has finished, so if you have wait_until_finished in your
        pipeline code, step two will not start until the process stops. When this process stops,
        steps two will run, but it will only execute one iteration as the job will be in a terminal state.

        If you in your pipeline do not call the wait_for_pipeline method but pass wait_until_finished=True
        to the operator, the second loop will wait for the job's terminal state.

        If you in your pipeline do not call the wait_for_pipeline method, and pass wait_until_finished=False
        to the operator, the second loop will check once is job not in terminal state and exit the loop.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param expected_terminal_state: The expected final status of the operator on which the corresponding
        Airflow task succeeds. When not specified, it will be determined by the hook.
    :param append_job_name: True if unique suffix has to be appended to job name.
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    """

    template_fields: Sequence[str] = ("body", "location", "project_id", "gcp_conn_id")
    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        body: dict,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        drain_pipeline: bool = False,
        cancel_timeout: int | None = 10 * 60,
        wait_until_finished: bool | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        append_job_name: bool = True,
        expected_terminal_state: str | None = None,
        poll_sleep: int = 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.body = body
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.drain_pipeline = drain_pipeline
        self.cancel_timeout = cancel_timeout
        self.wait_until_finished = wait_until_finished
        self.job: dict[str, str] | None = None
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.expected_terminal_state = expected_terminal_state
        self.append_job_name = append_job_name
        self.poll_sleep = poll_sleep

        self._validate_deferrable_params()

    def _validate_deferrable_params(self):
        if self.deferrable and self.wait_until_finished:
            raise ValueError(
                "Conflict between deferrable and wait_until_finished parameters "
                "because it makes operator as blocking when it requires to be deferred. "
                "It should be True as deferrable parameter or True as wait_until_finished."
            )

        if self.deferrable and self.wait_until_finished is None:
            self.wait_until_finished = False

    @cached_property
    def hook(self) -> DataflowHook:
        hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            wait_until_finished=self.wait_until_finished,
            impersonation_chain=self.impersonation_chain,
            expected_terminal_state=self.expected_terminal_state,
        )
        return hook

    def execute(self, context: Context):
        if self.append_job_name:
            self._append_uuid_to_job_name()

        def set_current_job(current_job):
            self.job = current_job
            DataflowJobLink.persist(
                context=context, project_id=self.project_id, region=self.location, job_id=self.job.get("id")
            )

        if not self.deferrable:
            self.job = self.hook.start_flex_template(
                body=self.body,
                location=self.location,
                project_id=self.project_id,
                on_new_job_callback=set_current_job,
            )
            job_id = self.hook.extract_job_id(self.job)
            context["task_instance"].xcom_push(key="job_id", value=job_id)
            return self.job

        self.job = self.hook.launch_job_with_flex_template(
            body=self.body,
            location=self.location,
            project_id=self.project_id,
        )
        job_id = self.hook.extract_job_id(self.job)
        DataflowJobLink.persist(
            context=context, project_id=self.project_id, region=self.location, job_id=job_id
        )
        self.defer(
            trigger=TemplateJobStartTrigger(
                project_id=self.project_id,
                job_id=job_id,
                location=self.location,
                gcp_conn_id=self.gcp_conn_id,
                poll_sleep=self.poll_sleep,
                impersonation_chain=self.impersonation_chain,
                cancel_timeout=self.cancel_timeout,
            ),
            method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
        )

    def _append_uuid_to_job_name(self):
        job_body = self.body.get("launch_parameter") or self.body.get("launchParameter")
        job_name = job_body.get("jobName")
        if job_name:
            job_name += f"-{uuid.uuid4()!s:.8}"
            job_body["jobName"] = job_name
            self.log.info("Job name was changed to %s", job_name)

    def execute_complete(self, context: Context, event: dict) -> dict[str, str]:
        """Execute after trigger finishes its work."""
        if event["status"] in ("error", "stopped"):
            self.log.info("status: %s, msg: %s", event["status"], event["message"])
            raise AirflowException(event["message"])

        job_id = event["job_id"]
        self.log.info("Task %s completed with response %s", job_id, event["message"])
        context["task_instance"].xcom_push(key="job_id", value=job_id)
        job = self.hook.get_job(job_id=job_id, project_id=self.project_id, location=self.location)
        return job

    def on_kill(self) -> None:
        self.log.info("On kill.")
        if self.job is not None:
            self.hook.cancel_job(
                job_id=self.job.get("id"),
                project_id=self.job.get("projectId"),
                location=self.job.get("location"),
            )


class DataflowStartYamlJobOperator(GoogleCloudBaseOperator):
    """
    Launch a Dataflow YAML job and return the result.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowStartYamlJobOperator`

    .. warning::
        This operator requires ``gcloud`` command (Google Cloud SDK) must be installed on the Airflow worker
        <https://cloud.google.com/sdk/docs/install>`__

    :param job_name: Required. The unique name to assign to the Cloud Dataflow job.
    :param yaml_pipeline_file: Required. Path to a file defining the YAML pipeline to run.
        Must be a local file or a URL beginning with 'gs://'.
    :param region: Optional. Region ID of the job's regional endpoint. Defaults to 'us-central1'.
    :param project_id: Required. The ID of the GCP project that owns the job.
        If set to ``None`` or missing, the default project_id from the GCP connection is used.
    :param gcp_conn_id: Optional. The connection ID used to connect to GCP.
    :param append_job_name: Optional. Set to True if a unique suffix has to be appended to the `job_name`.
        Defaults to True.
    :param drain_pipeline: Optional. Set to True if you want to stop a streaming pipeline job by draining it
        instead of canceling when killing the task instance. Note that this does not work for batch pipeline jobs
        or in the deferrable mode. Defaults to False.
        For more info see: https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline
    :param deferrable: Optional. Run operator in the deferrable mode.
    :param expected_terminal_state: Optional. The expected terminal state of the Dataflow job at which the
        operator task is set to succeed. Defaults to 'JOB_STATE_DONE' for the batch jobs and 'JOB_STATE_RUNNING'
        for the streaming jobs.
    :param poll_sleep: Optional. The time in seconds to sleep between polling Google Cloud Platform for the Dataflow job status.
        Used both for the sync and deferrable mode.
    :param cancel_timeout: Optional. How long (in seconds) operator should wait for the pipeline to be
        successfully canceled when the task is being killed.
    :param jinja_variables: Optional. A dictionary of Jinja2 variables to be used in reifying the yaml pipeline file.
    :param options: Optional. Additional gcloud or Beam job parameters.
        It must be a dictionary with the keys matching the optional flag names in gcloud.
        The list of supported flags can be found at: `https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml/run`.
        Note that if a flag does not require a value, then its dictionary value must be either True or None.
        For example, the `--log-http` flag can be passed as {'log-http': True}.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :return: Dictionary containing the job's data.
    """

    template_fields: Sequence[str] = (
        "job_name",
        "yaml_pipeline_file",
        "jinja_variables",
        "options",
        "region",
        "project_id",
        "gcp_conn_id",
    )
    template_fields_renderers = {
        "jinja_variables": "json",
    }
    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        *,
        job_name: str,
        yaml_pipeline_file: str,
        region: str = DEFAULT_DATAFLOW_LOCATION,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        append_job_name: bool = True,
        drain_pipeline: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_sleep: int = 10,
        cancel_timeout: int | None = 5 * 60,
        expected_terminal_state: str | None = None,
        jinja_variables: dict[str, str] | None = None,
        options: dict[str, Any] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.yaml_pipeline_file = yaml_pipeline_file
        self.region = region
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.append_job_name = append_job_name
        self.drain_pipeline = drain_pipeline
        self.deferrable = deferrable
        self.poll_sleep = poll_sleep
        self.cancel_timeout = cancel_timeout
        self.expected_terminal_state = expected_terminal_state
        self.options = options
        self.jinja_variables = jinja_variables
        self.impersonation_chain = impersonation_chain
        self.job_id: str | None = None

    def execute(self, context: Context) -> dict[str, Any]:
        self.job_id = self.hook.launch_beam_yaml_job(
            job_name=self.job_name,
            yaml_pipeline_file=self.yaml_pipeline_file,
            append_job_name=self.append_job_name,
            options=self.options,
            jinja_variables=self.jinja_variables,
            project_id=self.project_id,
            location=self.region,
        )

        DataflowJobLink.persist(
            context=context, project_id=self.project_id, region=self.region, job_id=self.job_id
        )

        if self.deferrable:
            self.defer(
                trigger=DataflowStartYamlJobTrigger(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    poll_sleep=self.poll_sleep,
                    cancel_timeout=self.cancel_timeout,
                    expected_terminal_state=self.expected_terminal_state,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )

        self.hook.wait_for_done(
            job_name=self.job_name, location=self.region, project_id=self.project_id, job_id=self.job_id
        )
        job = self.hook.get_job(job_id=self.job_id, location=self.region, project_id=self.project_id)
        return job

    def execute_complete(self, context: Context, event: dict) -> dict[str, Any]:
        """Execute after the trigger returns an event."""
        if event["status"] in ("error", "stopped"):
            self.log.info("status: %s, msg: %s", event["status"], event["message"])
            raise AirflowException(event["message"])
        job = event["job"]
        self.log.info("Job %s completed with response %s", job["id"], event["message"])
        context["task_instance"].xcom_push(key="job_id", value=job["id"])

        return job

    def on_kill(self):
        """
        Cancel the dataflow job if a task instance gets killed.

        This method will not be called if a task instance is killed in a deferred
        state.
        """
        self.log.info("On kill called.")
        if self.job_id:
            self.hook.cancel_job(
                job_id=self.job_id,
                project_id=self.project_id,
                location=self.region,
            )

    @cached_property
    def hook(self) -> DataflowHook:
        return DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            drain_pipeline=self.drain_pipeline,
            cancel_timeout=self.cancel_timeout,
            expected_terminal_state=self.expected_terminal_state,
        )


class DataflowStopJobOperator(GoogleCloudBaseOperator):
    """
    Stops the job with the specified name prefix or Job ID.

    All jobs with provided name prefix will be stopped.
    Streaming jobs are drained by default.

    Parameter ``job_name_prefix`` and ``job_id`` are mutually exclusive.

    .. seealso::
        For more details on stopping a pipeline see:
        https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowStopJobOperator`

    :param job_name_prefix: Name prefix specifying which jobs are to be stopped.
    :param job_id: Job ID specifying which jobs are to be stopped.
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: Optional, Job location. If set to None or missing, "us-central1" will be used.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status to confirm it's stopped.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param drain_pipeline: Optional, set to False if want to stop streaming job by canceling it
        instead of draining. See: https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline
    :param stop_timeout: wait time in seconds for successful job canceling/draining
    """

    template_fields = [
        "job_id",
        "project_id",
        "impersonation_chain",
    ]

    def __init__(
        self,
        job_name_prefix: str | None = None,
        job_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        stop_timeout: int | None = 10 * 60,
        drain_pipeline: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.poll_sleep = poll_sleep
        self.stop_timeout = stop_timeout
        self.job_name = job_name_prefix
        self.job_id = job_id
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.hook: DataflowHook | None = None
        self.drain_pipeline = drain_pipeline

    def execute(self, context: Context) -> None:
        self.dataflow_hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.stop_timeout,
            drain_pipeline=self.drain_pipeline,
        )
        if self.job_id or self.dataflow_hook.is_job_dataflow_running(
            name=self.job_name,
            project_id=self.project_id,
            location=self.location,
        ):
            self.dataflow_hook.cancel_job(
                job_name=self.job_name,
                project_id=self.project_id,
                location=self.location,
                job_id=self.job_id,
            )
        else:
            self.log.info("No jobs to stop")

        return None


class DataflowCreatePipelineOperator(GoogleCloudBaseOperator):
    """
    Creates a new Dataflow Data Pipeline instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowCreatePipelineOperator`

    :param body: The request body (contains instance of Pipeline). See:
        https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/projects.locations.pipelines/create#request-body
    :param project_id: The ID of the GCP project that owns the job.
    :param location: The location to direct the Data Pipelines instance to (for example us-central1).
    :param gcp_conn_id: The connection ID to connect to the Google Cloud
        Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

        .. warning::
            This option requires Apache Beam 2.39.0 or newer.

    Returns the created Dataflow Data Pipeline instance in JSON representation.
    """

    operator_extra_links = (DataflowPipelineLink(),)

    def __init__(
        self,
        *,
        body: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.body = body
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.dataflow_hook: DataflowHook | None = None

        self.pipeline_name = self.body["name"].split("/")[-1] if self.body else None

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "location": self.location,
            "pipeline_name": self.pipeline_name,
        }

    def execute(self, context: Context):
        if self.body is None:
            raise AirflowException(
                "Request Body not given; cannot create a Data Pipeline without the Request Body."
            )
        if self.project_id is None:
            raise AirflowException(
                "Project ID not given; cannot create a Data Pipeline without the Project ID."
            )
        if self.location is None:
            raise AirflowException("location not given; cannot create a Data Pipeline without the location.")

        self.dataflow_hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.body["pipelineSources"] = {"airflow": "airflow"}
        try:
            self.pipeline = self.dataflow_hook.create_data_pipeline(
                project_id=self.project_id,
                body=self.body,
                location=self.location,
            )
        except HttpError as e:
            if e.resp.status == 409:
                # If the pipeline already exists, retrieve it
                self.log.info("Pipeline with given name already exists.")
                self.pipeline = self.dataflow_hook.get_data_pipeline(
                    project_id=self.project_id,
                    pipeline_name=self.pipeline_name,
                    location=self.location,
                )
        DataflowPipelineLink.persist(context=context)
        context["task_instance"].xcom_push(key="pipeline_name", value=self.pipeline_name)
        if self.pipeline:
            if "error" in self.pipeline:
                raise AirflowException(self.pipeline.get("error").get("message"))

        return self.pipeline


class DataflowRunPipelineOperator(GoogleCloudBaseOperator):
    """
    Runs a Dataflow Data Pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowRunPipelineOperator`

    :param pipeline_name:  The display name of the pipeline. In example
        projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
    :param project_id: The ID of the GCP project that owns the job.
    :param location: The location to direct the Data Pipelines instance to (for example us-central1).
    :param gcp_conn_id: The connection ID to connect to the Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    Returns the created Job in JSON representation.
    """

    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        pipeline_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.pipeline_name = pipeline_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.dataflow_hook: DataflowHook | None = None

    def execute(self, context: Context):
        self.dataflow_hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        if self.pipeline_name is None:
            raise AirflowException("Data Pipeline name not given; cannot run unspecified pipeline.")
        if self.project_id is None:
            raise AirflowException("Data Pipeline Project ID not given; cannot run pipeline.")
        if self.location is None:
            raise AirflowException("Data Pipeline location not given; cannot run pipeline.")
        try:
            self.job = self.dataflow_hook.run_data_pipeline(
                pipeline_name=self.pipeline_name,
                project_id=self.project_id,
                location=self.location,
            )["job"]
            job_id = self.dataflow_hook.extract_job_id(self.job)
            context["task_instance"].xcom_push(key="job_id", value=job_id)
            DataflowJobLink.persist(
                context=context, project_id=self.project_id, region=self.location, job_id=job_id
            )
        except HttpError as e:
            if e.resp.status == 404:
                raise AirflowException("Pipeline with given name was not found.")
        except Exception as exc:
            raise AirflowException("Error occurred when running Pipeline: %s", exc)

        return self.job


class DataflowDeletePipelineOperator(GoogleCloudBaseOperator):
    """
    Deletes a Dataflow Data Pipeline.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowDeletePipelineOperator`

    :param pipeline_name: The display name of the pipeline. In example
        projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
    :param project_id: The ID of the GCP project that owns the job.
    :param location: The location to direct the Data Pipelines instance to (for example us-central1).
    :param gcp_conn_id: The connection ID to connect to the Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        pipeline_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.pipeline_name = pipeline_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.dataflow_hook: DataflowHook | None = None
        self.response: dict | None = None

    def execute(self, context: Context):
        self.dataflow_hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        if self.pipeline_name is None:
            raise AirflowException("Data Pipeline name not given; cannot run unspecified pipeline.")
        if self.project_id is None:
            raise AirflowException("Data Pipeline Project ID not given; cannot run pipeline.")
        if self.location is None:
            raise AirflowException("Data Pipeline location not given; cannot run pipeline.")

        self.response = self.dataflow_hook.delete_data_pipeline(
            pipeline_name=self.pipeline_name,
            project_id=self.project_id,
            location=self.location,
        )

        if self.response:
            raise AirflowException(self.response)

        return None
