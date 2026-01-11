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
from collections.abc import AsyncIterator
from typing import IO, Any

from airflow.providers.apache.beam.hooks.beam import BeamAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BeamPipelineBaseTrigger(BaseTrigger):
    """Base class for Beam Pipeline Triggers."""

    @staticmethod
    def _get_async_hook(*args, **kwargs) -> BeamAsyncHook:
        return BeamAsyncHook(*args, **kwargs)

    @staticmethod
    def file_has_gcs_path(file_path: str):
        return file_path.lower().startswith("gs://")

    @staticmethod
    async def provide_gcs_tempfile(gcs_file, gcp_conn_id):
        try:
            from airflow.providers.google.cloud.hooks.gcs import GCSAsyncHook
        except ImportError:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "Failed to import GCSAsyncHook. To use the GCSAsyncHook functionality, please install the "
                "apache-airflow-google-provider."
            )

        async_gcs_hook = GCSAsyncHook(gcp_conn_id=gcp_conn_id)
        sync_gcs_hook = await async_gcs_hook.get_sync_hook()

        loop = asyncio.get_running_loop()

        # Running synchronous `enter_context()` method in a separate
        # thread using the default executor `None`. The `run_in_executor()` function returns the
        # file object, which is created using gcs function `provide_file()`, asynchronously.
        # This means we can perform asynchronous operations with this file.
        create_tmp_file_call = sync_gcs_hook.provide_file(object_url=gcs_file)
        tmp_gcs_file: IO[str] = await loop.run_in_executor(
            None,
            contextlib.ExitStack().enter_context,
            create_tmp_file_call,
        )
        return tmp_gcs_file


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
    :param gcp_conn_id: Optional. The connection ID to use connecting to Google Cloud.
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
        gcp_conn_id: str = "google_cloud_default",
    ):
        super().__init__()
        self.variables = variables
        self.py_file = py_file
        self.py_options = py_options
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages
        self.runner = runner
        self.gcp_conn_id = gcp_conn_id

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
                "gcp_conn_id": self.gcp_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current pipeline status and yields a TriggerEvent."""
        hook = self._get_async_hook(runner=self.runner)

        try:
            if self.file_has_gcs_path(self.py_file):
                tmp_gcs_file = await self.provide_gcs_tempfile(self.py_file, self.gcp_conn_id)
                self.py_file = tmp_gcs_file.name

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
    :param gcp_conn_id: Optional. The connection ID to use connecting to Google Cloud.
    """

    def __init__(
        self,
        variables: dict,
        jar: str,
        job_class: str | None = None,
        runner: str = "DirectRunner",
        gcp_conn_id: str = "google_cloud_default",
    ):
        super().__init__()
        self.variables = variables
        self.jar = jar
        self.job_class = job_class
        self.runner = runner
        self.gcp_conn_id = gcp_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BeamJavaPipelineTrigger arguments and classpath."""
        return (
            "airflow.providers.apache.beam.triggers.beam.BeamJavaPipelineTrigger",
            {
                "variables": self.variables,
                "jar": self.jar,
                "job_class": self.job_class,
                "runner": self.runner,
                "gcp_conn_id": self.gcp_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current Java pipeline status and yields a TriggerEvent."""
        hook = self._get_async_hook(runner=self.runner)
        return_code = 0
        try:
            if self.file_has_gcs_path(self.jar):
                tmp_gcs_file = await self.provide_gcs_tempfile(self.jar, self.gcp_conn_id)
                self.jar = tmp_gcs_file.name

            return_code = await hook.start_java_pipeline_async(
                variables=self.variables,
                jar=self.jar,
                job_class=self.job_class,
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
