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

import asyncio
import contextlib
from typing import IO, Any, AsyncIterator, Sequence

from deprecated import deprecated
from google.cloud.dataflow_v1beta3 import ListJobsRequest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.apache.beam.hooks.beam import BeamAsyncHook
from airflow.providers.google.cloud.hooks.dataflow import AsyncDataflowHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BeamPipelineBaseTrigger(BaseTrigger):
    """Base class for Beam Pipeline Triggers."""

    @staticmethod
    def _get_async_hook(*args, **kwargs) -> BeamAsyncHook:
        return BeamAsyncHook(*args, **kwargs)

    @staticmethod
    def _get_sync_dataflow_hook(**kwargs) -> AsyncDataflowHook:
        return AsyncDataflowHook(**kwargs)


class BeamPythonPipelineTrigger(BeamPipelineBaseTrigger):
    """
    Trigger to perform checking the Python pipeline status until it reaches terminate state.

    :param variables: Variables passed to the pipeline.
    :param py_file: Path to the python file to execute.
    :param py_options: Additional options.
    :param py_interpreter: Python version of the Apache Beam pipeline. If `None`, this defaults to the
        python3. To track python versions supported by beam and related issues
        check: https://issues.apache.org/jira/browse/BEAM-1251
    :param py_requirements: Additional python package(s) to install.
        If a value is passed to this parameter, a new virtual environment has been created with
        additional packages installed.
        You could also install the apache-beam package if it is not installed on your system, or you want
        to use a different version.
    :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
        This option is only relevant if the ``py_requirements`` parameter is not None.
    :param runner: Runner on which pipeline will be run. By default, "DirectRunner" is being used.
        Other possible options: DataflowRunner, SparkRunner, FlinkRunner, PortableRunner.
        See: :class:`~providers.apache.beam.hooks.beam.BeamRunnerType`
        See: https://beam.apache.org/documentation/runners/capability-matrix/
    """

    def __init__(
        self,
        variables: dict,
        py_file: str,
        py_options: list[str] | None = None,
        py_interpreter: str = "python3",
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        runner: str = "DirectRunner",
    ):
        super().__init__()
        self.variables = variables
        self.py_file = py_file
        self.py_options = py_options
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages
        self.runner = runner

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BeamPythonPipelineTrigger arguments and classpath."""
        return (
            "airflow.providers.apache.beam.triggers.beam.BeamPythonPipelineTrigger",
            {
                "variables": self.variables,
                "py_file": self.py_file,
                "py_options": self.py_options,
                "py_interpreter": self.py_interpreter,
                "py_requirements": self.py_requirements,
                "py_system_site_packages": self.py_system_site_packages,
                "runner": self.runner,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current pipeline status and yields a TriggerEvent."""
        hook = self._get_async_hook(runner=self.runner)
        try:
            return_code = await hook.start_python_pipeline_async(
                variables=self.variables,
                py_file=self.py_file,
                py_options=self.py_options,
                py_interpreter=self.py_interpreter,
                py_requirements=self.py_requirements,
                py_system_site_packages=self.py_system_site_packages,
            )
        except Exception as e:
            self.log.exception("Exception occurred while checking for pipeline state")
            yield TriggerEvent({"status": "error", "message": str(e)})
        else:
            if return_code == 0:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": "Pipeline has finished SUCCESSFULLY",
                    }
                )
            else:
                yield TriggerEvent({"status": "error", "message": "Operation failed"})
        return


class BeamJavaPipelineTrigger(BeamPipelineBaseTrigger):
    """
    Trigger to perform checking the Java pipeline status until it reaches terminate state.

    :param variables: Variables passed to the job.
    :param jar: Name of the jar for the pipeline.
    :param job_class: Optional. Name of the java class for the pipeline.
    :param runner: Runner on which pipeline will be run. By default, "DirectRunner" is being used.
        Other possible options: DataflowRunner, SparkRunner, FlinkRunner, PortableRunner.
        See: :class:`~providers.apache.beam.hooks.beam.BeamRunnerType`
        See: https://beam.apache.org/documentation/runners/capability-matrix/
    :param check_if_running: Optional. Before running job, validate that a previous run is not in process.
    :param project_id: Optional. The Google Cloud project ID in which to start a job.
    :param location: Optional. Job location.
    :param job_name: Optional. The 'jobName' to use when executing the Dataflow job.
    :param gcp_conn_id: Optional. The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional. GCP service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_sleep: Optional. The time in seconds to sleep between polling GCP for the dataflow job status.
        Default value is 10s.
    :param cancel_timeout: Optional. How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed. Default value is 300s.
    """

    def __init__(
        self,
        variables: dict,
        jar: str,
        job_class: str | None = None,
        runner: str = "DirectRunner",
        check_if_running: bool = False,
        project_id: str | None = None,
        location: str | None = None,
        job_name: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        poll_sleep: int = 10,
        cancel_timeout: int | None = None,
    ):
        super().__init__()
        self.variables = variables
        self.jar = jar
        self.job_class = job_class
        self.runner = runner
        self.check_if_running = check_if_running
        self.project_id = project_id
        self.location = location
        self.job_name = job_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_sleep = poll_sleep
        self.cancel_timeout = cancel_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BeamJavaPipelineTrigger arguments and classpath."""
        return (
            "airflow.providers.apache.beam.triggers.beam.BeamJavaPipelineTrigger",
            {
                "variables": self.variables,
                "jar": self.jar,
                "job_class": self.job_class,
                "runner": self.runner,
                "check_if_running": self.check_if_running,
                "project_id": self.project_id,
                "location": self.location,
                "job_name": self.job_name,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poll_sleep": self.poll_sleep,
                "cancel_timeout": self.cancel_timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current Java pipeline status and yields a TriggerEvent."""
        hook = self._get_async_hook(runner=self.runner)

        return_code = 0
        if self.check_if_running:
            dataflow_hook = self._get_sync_dataflow_hook(
                gcp_conn_id=self.gcp_conn_id,
                poll_sleep=self.poll_sleep,
                impersonation_chain=self.impersonation_chain,
                cancel_timeout=self.cancel_timeout,
            )
            is_running = True
            while is_running:
                try:
                    jobs = await dataflow_hook.list_jobs(
                        project_id=self.project_id,
                        location=self.location,
                        jobs_filter=ListJobsRequest.Filter.ACTIVE,
                    )
                    is_running = bool([job async for job in jobs if job.name == self.job_name])
                except Exception as e:
                    self.log.exception("Exception occurred while requesting jobs with name %s", self.job_name)
                    yield TriggerEvent({"status": "error", "message": str(e)})
                    return
                if is_running:
                    await asyncio.sleep(self.poll_sleep)
        try:
            # Get the current running event loop to manage I/O operations asynchronously
            loop = asyncio.get_running_loop()
            if self.jar.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id)
                # Running synchronous `enter_context()` method in a separate
                # thread using the default executor `None`. The `run_in_executor()` function returns the
                # file object, which is created using gcs function `provide_file()`, asynchronously.
                # This means we can perform asynchronous operations with this file.
                create_tmp_file_call = gcs_hook.provide_file(object_url=self.jar)
                tmp_gcs_file: IO[str] = await loop.run_in_executor(
                    None,
                    contextlib.ExitStack().enter_context,  # type: ignore[arg-type]
                    create_tmp_file_call,
                )
                self.jar = tmp_gcs_file.name

            return_code = await hook.start_java_pipeline_async(
                variables=self.variables, jar=self.jar, job_class=self.job_class
            )
        except Exception as e:
            self.log.exception("Exception occurred while starting the Java pipeline")
            yield TriggerEvent({"status": "error", "message": str(e)})

        if return_code == 0:
            yield TriggerEvent(
                {
                    "status": "success",
                    "message": "Pipeline has finished SUCCESSFULLY",
                }
            )
        else:
            yield TriggerEvent({"status": "error", "message": "Operation failed"})
        return


@deprecated(
    reason="`BeamPipelineTrigger` is deprecated. Please use `BeamPythonPipelineTrigger`.",
    category=AirflowProviderDeprecationWarning,
)
class BeamPipelineTrigger(BeamPythonPipelineTrigger):
    """
    Trigger to perform checking the Python pipeline status until it reaches terminate state.

    This class is deprecated. Please use
    :class:`airflow.providers.apache.beam.triggers.beam.BeamPythonPipelineTrigger`
    instead.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
