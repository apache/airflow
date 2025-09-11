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
"""This module contains Apache Beam operators."""

from __future__ import annotations

import copy
import os
import stat
import tempfile
from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from functools import partial
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType
from airflow.providers.apache.beam.triggers.beam import BeamJavaPipelineTrigger, BeamPythonPipelineTrigger
from airflow.providers.apache.beam.version_compat import BaseOperator
from airflow.providers_manager import ProvidersManager
from airflow.utils.helpers import convert_camel_to_snake, exactly_one
from airflow.version import version

if TYPE_CHECKING:
    from airflow.utils.context import Context

GOOGLE_PROVIDER = ProvidersManager().providers.get("apache-airflow-providers-google")


if GOOGLE_PROVIDER:
    from airflow.providers.google.cloud.hooks.dataflow import (
        DEFAULT_DATAFLOW_LOCATION,
        DataflowHook,
        process_line_and_extract_dataflow_job_id_callback,
    )
    from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
    from airflow.providers.google.cloud.links.dataflow import DataflowJobLink
    from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning, DataflowConfiguration
    from airflow.providers.google.cloud.triggers.dataflow import (
        DataflowJobStatus,
        DataflowJobStatusTrigger,
    )

    try:
        from airflow.providers.google.cloud.triggers.dataflow import DataflowJobStateCompleteTrigger

        GOOGLE_PROVIDER_DATAFLOW_JOB_STATE_COMPLETE_TRIGGER_AVAILABLE = True
    except ImportError:
        GOOGLE_PROVIDER_DATAFLOW_JOB_STATE_COMPLETE_TRIGGER_AVAILABLE = False


class BeamDataflowMixin(metaclass=ABCMeta):
    """
    Helper class to store common, Dataflow specific logic for both.

    :class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`,
    :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator` and
    :class:`~airflow.providers.apache.beam.operators.beam.BeamRunGoPipelineOperator`.
    """

    dataflow_hook: DataflowHook | None
    dataflow_config: DataflowConfiguration
    gcp_conn_id: str
    dataflow_support_impersonation: bool = True

    def __init__(self):
        if not GOOGLE_PROVIDER:
            raise AirflowOptionalProviderFeatureException(
                "Failed to import apache-airflow-google-provider. To use the dataflow service please install "
                "the appropriate version of the google provider."
            )

    def _set_dataflow(
        self,
        pipeline_options: dict,
        job_name_variable_key: str | None = None,
    ) -> tuple[str, dict, Callable[[str], None], Callable[[], bool]]:
        self.dataflow_hook = self.__set_dataflow_hook()
        self.dataflow_config.project_id = self.dataflow_config.project_id or self.dataflow_hook.project_id
        dataflow_job_name = self.__get_dataflow_job_name()
        pipeline_options = self.__get_dataflow_pipeline_options(
            pipeline_options, dataflow_job_name, job_name_variable_key
        )
        process_line_callback = self.__get_dataflow_process_callback()
        is_dataflow_job_id_exist_callback = self.__is_dataflow_job_id_exist_callback()
        return dataflow_job_name, pipeline_options, process_line_callback, is_dataflow_job_id_exist_callback

    def __set_dataflow_hook(self) -> DataflowHook:
        self.dataflow_hook = DataflowHook(
            gcp_conn_id=self.dataflow_config.gcp_conn_id or self.gcp_conn_id,
            poll_sleep=self.dataflow_config.poll_sleep,
            impersonation_chain=self.dataflow_config.impersonation_chain,
            drain_pipeline=self.dataflow_config.drain_pipeline,
            cancel_timeout=self.dataflow_config.cancel_timeout,
            wait_until_finished=self.dataflow_config.wait_until_finished,
        )
        return self.dataflow_hook

    def __get_dataflow_job_name(self) -> str:
        return DataflowHook.build_dataflow_job_name(
            self.dataflow_config.job_name,  # type: ignore
            self.dataflow_config.append_job_name,
        )

    def __get_dataflow_pipeline_options(
        self, pipeline_options: dict, job_name: str, job_name_key: str | None = None
    ) -> dict:
        pipeline_options = copy.deepcopy(pipeline_options)
        if job_name_key is not None:
            pipeline_options[job_name_key] = job_name
        if self.dataflow_config.service_account:
            pipeline_options["serviceAccount"] = self.dataflow_config.service_account
        if self.dataflow_support_impersonation and self.dataflow_config.impersonation_chain:
            if isinstance(self.dataflow_config.impersonation_chain, list):
                pipeline_options["impersonateServiceAccount"] = ",".join(
                    self.dataflow_config.impersonation_chain
                )
            else:
                pipeline_options["impersonateServiceAccount"] = self.dataflow_config.impersonation_chain
        pipeline_options["project"] = self.dataflow_config.project_id
        pipeline_options["region"] = self.dataflow_config.location
        pipeline_options.setdefault("labels", {}).update(
            {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
        )
        return pipeline_options

    def __get_dataflow_process_callback(self) -> Callable[[str], None]:
        def set_current_dataflow_job_id(job_id):
            self.dataflow_job_id = job_id

        return process_line_and_extract_dataflow_job_id_callback(
            on_new_job_id_callback=set_current_dataflow_job_id
        )

    def __is_dataflow_job_id_exist_callback(self) -> Callable[[], bool]:
        def is_dataflow_job_id_exist() -> bool:
            return True if self.dataflow_job_id else False

        return is_dataflow_job_id_exist


class BeamBasePipelineOperator(BaseOperator, BeamDataflowMixin, ABC):
    """
    Abstract base class for Beam Pipeline Operators.

    :param runner: Runner on which pipeline will be run. By default "DirectRunner" is being used.
        Other possible options: DataflowRunner, SparkRunner, FlinkRunner, PortableRunner.
        See: :class:`~providers.apache.beam.hooks.beam.BeamRunnerType`
        See: https://beam.apache.org/documentation/runners/capability-matrix/

    :param default_pipeline_options: Map of default pipeline options.
    :param pipeline_options: Map of pipeline options.The key must be a dictionary.
        The value can contain different types:

        * If the value is None, the single option - ``--key`` (without value) will be added.
        * If the value is False, this option will be skipped
        * If the value is True, the single option - ``--key`` (without value) will be added.
        * If the value is list, the many options will be added for each key.
          If the value is ``['A', 'B']`` and the key is ``key`` then the ``--key=A --key=B`` options
          will be left
        * Other value types will be replaced with the Python textual representation.

        When defining labels (labels option), you can also provide a dictionary.
    :param gcp_conn_id: Optional.
        The connection ID to use connecting to Google Cloud Storage if python file is on GCS.
    :param dataflow_config: Dataflow's configuration, used when runner type is set to DataflowRunner,
        (optional) defaults to None.
    """

    def __init__(
        self,
        *,
        runner: str = "DirectRunner",
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        dataflow_config: DataflowConfiguration | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.runner = runner
        self.default_pipeline_options = default_pipeline_options or {}
        self.pipeline_options = pipeline_options or {}
        # ``dataflow_config`` type will resolve into the execute method
        self.dataflow_config = dataflow_config or {}  # type: ignore[assignment]
        self.gcp_conn_id = gcp_conn_id
        self.beam_hook: BeamHook
        self.dataflow_hook: DataflowHook | None = None
        self._dataflow_job_id: str | None = None
        self._execute_context: Context | None = None

    @property
    def dataflow_job_id(self):
        return self._dataflow_job_id

    @dataflow_job_id.setter
    def dataflow_job_id(self, new_value):
        if all([new_value, not self._dataflow_job_id, self._execute_context]):
            # push job_id as soon as it's ready, to let Sensors work before the job finished
            # and job_id pushed as returned value item.
            # Use task instance to push XCom (works for both Airflow 2.x and 3.x)
            self._execute_context["ti"].xcom_push(key="dataflow_job_id", value=new_value)
        self._dataflow_job_id = new_value

    def _cast_dataflow_config(self):
        if isinstance(self.dataflow_config, dict):
            self.dataflow_config = DataflowConfiguration(**self.dataflow_config)
        else:
            self.dataflow_config = self.dataflow_config or DataflowConfiguration()

        if not self.dataflow_config.job_name:
            self.dataflow_config.job_name = self.task_id

        if self.dataflow_config and self.runner.lower() != BeamRunnerType.DataflowRunner.lower():
            self.log.warning(
                "dataflow_config is defined but runner is different than DataflowRunner (%s)", self.runner
            )

    def _init_pipeline_options(
        self,
        format_pipeline_options: bool = False,
        job_name_variable_key: str | None = None,
    ) -> tuple[bool, str | None, dict, Callable[[str], None] | None, Callable[[], bool] | None]:
        self.beam_hook = BeamHook(runner=self.runner)
        pipeline_options = self.default_pipeline_options.copy()
        process_line_callback: Callable[[str], None] | None = None
        is_dataflow_job_id_exist_callback: Callable[[], bool] | None = None
        is_dataflow = self.runner.lower() == BeamRunnerType.DataflowRunner.lower()
        dataflow_job_name: str | None = None
        if is_dataflow:
            (
                dataflow_job_name,
                pipeline_options,
                process_line_callback,
                is_dataflow_job_id_exist_callback,
            ) = self._set_dataflow(
                pipeline_options=pipeline_options,
                job_name_variable_key=job_name_variable_key,
            )
            self.log.info(pipeline_options)

        pipeline_options.update(self.pipeline_options)

        if format_pipeline_options:
            snake_case_pipeline_options = {
                convert_camel_to_snake(key): pipeline_options[key] for key in pipeline_options
            }
            return (
                is_dataflow,
                dataflow_job_name,
                snake_case_pipeline_options,
                process_line_callback,
                is_dataflow_job_id_exist_callback,
            )

        return (
            is_dataflow,
            dataflow_job_name,
            pipeline_options,
            process_line_callback,
            is_dataflow_job_id_exist_callback,
        )

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.dataflow_config.project_id,
            "region": self.dataflow_config.location,
            "job_id": self.dataflow_job_id,
        }

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )


class BeamRunPythonPipelineOperator(BeamBasePipelineOperator):
    """
    Launch Apache Beam pipelines written in Python.

    Note that both ``default_pipeline_options`` and ``pipeline_options``
    will be merged to specify pipeline execution parameter, and
    ``default_pipeline_options`` is expected to save high-level options,
    for instances, project and zone information, which apply to all beam
    operators in the DAG.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BeamRunPythonPipelineOperator`

    .. seealso::
        For more detail on Apache Beam have a look at the reference:
        https://beam.apache.org/documentation/

    :param py_file: Reference to the python Apache Beam pipeline file.py, e.g.,
        /some/local/file/path/to/your/python/pipeline/file. (templated)
    :param py_options: Additional python options, e.g., ["-m", "-v"].
    :param py_interpreter: Python version of the beam pipeline.
        If None, this defaults to the python3.
        To track python versions supported by beam and related
        issues check: https://issues.apache.org/jira/browse/BEAM-1251
    :param py_requirements: Additional python package(s) to install.
        If a value is passed to this parameter, a new virtual environment has been created with
        additional packages installed.

        You could also install the apache_beam package if it is not installed on your system or you want
        to use a different version.
    :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.
        This option is only relevant if the ``py_requirements`` parameter is not None.
    :param deferrable: Run operator in the deferrable mode: checks for the state using asynchronous calls.
    """

    template_fields: Sequence[str] = (
        "py_file",
        "runner",
        "pipeline_options",
        "default_pipeline_options",
        "dataflow_config",
    )
    template_fields_renderers = {"dataflow_config": "json", "pipeline_options": "json"}
    operator_extra_links = (DataflowJobLink(),) if GOOGLE_PROVIDER else ()

    def __init__(
        self,
        *,
        py_file: str,
        runner: str = "DirectRunner",
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        py_interpreter: str = "python3",
        py_options: list[str] | None = None,
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        dataflow_config: DataflowConfiguration | dict | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(
            runner=runner,
            default_pipeline_options=default_pipeline_options,
            pipeline_options=pipeline_options,
            gcp_conn_id=gcp_conn_id,
            dataflow_config=dataflow_config,
            **kwargs,
        )

        self.py_file = py_file
        self.py_options = py_options or []
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages
        self.deferrable = deferrable

    def execute(self, context: Context):
        """Execute the Apache Beam Python Pipeline."""
        self._execute_context = context
        self._cast_dataflow_config()
        self.pipeline_options.setdefault("labels", {}).update(
            {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
        )
        (
            self.is_dataflow,
            self.dataflow_job_name,
            self.snake_case_pipeline_options,
            self.process_line_callback,
            self.is_dataflow_job_id_exist_callback,
        ) = self._init_pipeline_options(format_pipeline_options=True, job_name_variable_key="job_name")
        if not self.beam_hook:
            raise AirflowException("Beam hook is not defined.")

        if self.deferrable and not self.is_dataflow:
            self.defer(
                trigger=BeamPythonPipelineTrigger(
                    variables=self.snake_case_pipeline_options,
                    py_file=self.py_file,
                    py_options=self.py_options,
                    py_interpreter=self.py_interpreter,
                    py_requirements=self.py_requirements,
                    py_system_site_packages=self.py_system_site_packages,
                    runner=self.runner,
                    gcp_conn_id=self.gcp_conn_id,
                ),
                method_name="execute_complete",
            )

        with ExitStack() as exit_stack:
            if self.py_file.lower().startswith("gs://"):
                gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
                tmp_gcs_file = exit_stack.enter_context(gcs_hook.provide_file(object_url=self.py_file))
                self.py_file = tmp_gcs_file.name
            if self.snake_case_pipeline_options.get("requirements_file", "").startswith("gs://"):
                gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
                tmp_req_file = exit_stack.enter_context(
                    gcs_hook.provide_file(object_url=self.snake_case_pipeline_options["requirements_file"])
                )
                self.snake_case_pipeline_options["requirements_file"] = tmp_req_file.name

            if self.is_dataflow and self.dataflow_hook:
                return self.execute_on_dataflow(context)
            self.beam_hook.start_python_pipeline(
                variables=self.snake_case_pipeline_options,
                py_file=self.py_file,
                py_options=self.py_options,
                py_interpreter=self.py_interpreter,
                py_requirements=self.py_requirements,
                py_system_site_packages=self.py_system_site_packages,
            )

    def execute_on_dataflow(self, context: Context):
        """Execute the Apache Beam Pipeline on Dataflow runner."""
        if not self.dataflow_hook:
            self.dataflow_hook = self.__set_dataflow_hook()

        self.beam_hook.start_python_pipeline(
            variables=self.snake_case_pipeline_options,
            py_file=self.py_file,
            py_options=self.py_options,
            py_interpreter=self.py_interpreter,
            py_requirements=self.py_requirements,
            py_system_site_packages=self.py_system_site_packages,
            process_line_callback=self.process_line_callback,
            is_dataflow_job_id_exist_callback=self.is_dataflow_job_id_exist_callback,
        )

        location = self.dataflow_config.location or DEFAULT_DATAFLOW_LOCATION
        DataflowJobLink.persist(context=context, region=location)

        if self.deferrable:
            trigger_args = {
                "job_id": self.dataflow_job_id,
                "project_id": self.dataflow_config.project_id,
                "location": location,
                "gcp_conn_id": self.gcp_conn_id,
            }
            trigger: DataflowJobStatusTrigger | DataflowJobStateCompleteTrigger

            if GOOGLE_PROVIDER_DATAFLOW_JOB_STATE_COMPLETE_TRIGGER_AVAILABLE:
                trigger = DataflowJobStateCompleteTrigger(
                    wait_until_finished=self.dataflow_config.wait_until_finished,
                    **trigger_args,
                )
            else:
                trigger = DataflowJobStatusTrigger(
                    expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                    **trigger_args,
                )

            self.defer(
                trigger=trigger,
                method_name="execute_complete",
            )
        self.dataflow_hook.wait_for_done(
            job_name=self.dataflow_job_name,
            location=self.dataflow_config.location,
            job_id=self.dataflow_job_id,
            project_id=self.dataflow_config.project_id,
        )
        return {"dataflow_job_id": self.dataflow_job_id}

    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info("Dataflow job with id: `%s` was requested to be cancelled.", self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
                location=self.dataflow_config.location,
            )


class BeamRunJavaPipelineOperator(BeamBasePipelineOperator):
    """
    Launching Apache Beam pipelines written in Java.

    Note that both
    ``default_pipeline_options`` and ``pipeline_options`` will be merged to specify pipeline
    execution parameter, and ``default_pipeline_options`` is expected to save
    high-level pipeline_options, for instances, project and zone information, which
    apply to all Apache Beam operators in the DAG.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BeamRunJavaPipelineOperator`

    .. seealso::
        For more detail on Apache Beam have a look at the reference:
        https://beam.apache.org/documentation/

    You need to pass the path to your jar file as a file reference with the ``jar``
    parameter, the jar needs to be a self executing jar (see documentation here:
    https://beam.apache.org/documentation/runners/dataflow/#self-executing-jar).
    Use ``pipeline_options`` to pass on pipeline_options to your job.

    :param jar: The reference to a self executing Apache Beam jar (templated).
    :param job_class: The name of the Apache Beam pipeline class to be executed, it
        is often not the main class configured in the pipeline jar file.
    """

    template_fields: Sequence[str] = (
        "jar",
        "runner",
        "job_class",
        "pipeline_options",
        "default_pipeline_options",
        "dataflow_config",
    )
    template_fields_renderers = {"dataflow_config": "json", "pipeline_options": "json"}
    ui_color = "#0273d4"

    operator_extra_links = (DataflowJobLink(),) if GOOGLE_PROVIDER else ()

    def __init__(
        self,
        *,
        jar: str,
        runner: str = "DirectRunner",
        job_class: str | None = None,
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        dataflow_config: DataflowConfiguration | dict | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(
            runner=runner,
            default_pipeline_options=default_pipeline_options,
            pipeline_options=pipeline_options,
            gcp_conn_id=gcp_conn_id,
            dataflow_config=dataflow_config,
            **kwargs,
        )
        self.jar = jar
        self.job_class = job_class
        self.deferrable = deferrable

    def execute(self, context: Context):
        """Execute the Apache Beam Python Pipeline."""
        self._execute_context = context
        self._cast_dataflow_config()
        (
            self.is_dataflow,
            self.dataflow_job_name,
            self.pipeline_options,
            self.process_line_callback,
            self.is_dataflow_job_id_exist_callback,
        ) = self._init_pipeline_options()
        if not self.beam_hook:
            raise AirflowException("Beam hook is not defined.")

        if self.deferrable and not self.is_dataflow:
            self.defer(
                trigger=BeamJavaPipelineTrigger(
                    variables=self.pipeline_options,
                    jar=self.jar,
                    job_class=self.job_class,
                    runner=self.runner,
                    gcp_conn_id=self.gcp_conn_id,
                ),
                method_name="execute_complete",
            )

        with ExitStack() as exit_stack:
            if self.jar.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id)
                tmp_gcs_file = exit_stack.enter_context(gcs_hook.provide_file(object_url=self.jar))
                self.jar = tmp_gcs_file.name

            if self.is_dataflow and self.dataflow_hook:
                return self.execute_on_dataflow(context)
            self.beam_hook.start_java_pipeline(
                variables=self.pipeline_options,
                jar=self.jar,
                job_class=self.job_class,
            )

    def execute_on_dataflow(self, context: Context):
        """Execute the Apache Beam Pipeline on Dataflow runner."""
        if not self.dataflow_hook:
            self.dataflow_hook = self.__set_dataflow_hook()
        is_running = self.dataflow_config.check_if_running == CheckJobRunning.WaitForRun
        while is_running and self.dataflow_config.check_if_running == CheckJobRunning.WaitForRun:
            # The reason for disable=no-value-for-parameter is that project_id parameter is
            # required but here is not passed, moreover it cannot be passed here.
            # This method is wrapped by @_fallback_to_project_id_from_variables decorator which
            # fallback project_id value from variables and raise error if project_id is
            # defined both in variables and as parameter (here is already defined in variables)
            is_running = self.dataflow_hook.is_job_dataflow_running(
                name=self.dataflow_config.job_name,
                variables=self.pipeline_options,
                location=self.dataflow_config.location,
            )

        if not is_running:
            self.pipeline_options["jobName"] = self.dataflow_job_name
            self.beam_hook.start_java_pipeline(
                variables=self.pipeline_options,
                jar=self.jar,
                job_class=self.job_class,
                process_line_callback=self.process_line_callback,
                is_dataflow_job_id_exist_callback=self.is_dataflow_job_id_exist_callback,
            )
            if self.dataflow_job_name and self.dataflow_config.location:
                DataflowJobLink.persist(context=context)
                if self.deferrable:
                    trigger_args = {
                        "job_id": self.dataflow_job_id,
                        "project_id": self.dataflow_config.project_id,
                        "location": self.dataflow_config.location,
                        "gcp_conn_id": self.gcp_conn_id,
                    }
                    trigger: DataflowJobStatusTrigger | DataflowJobStateCompleteTrigger

                    if GOOGLE_PROVIDER_DATAFLOW_JOB_STATE_COMPLETE_TRIGGER_AVAILABLE:
                        trigger = DataflowJobStateCompleteTrigger(
                            wait_until_finished=self.dataflow_config.wait_until_finished,
                            **trigger_args,
                        )
                    else:
                        trigger = DataflowJobStatusTrigger(
                            expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
                            **trigger_args,
                        )

                    self.defer(
                        trigger=trigger,
                        method_name="execute_complete",
                    )

                multiple_jobs = self.dataflow_config.multiple_jobs or False
                self.dataflow_hook.wait_for_done(
                    job_name=self.dataflow_job_name,
                    location=self.dataflow_config.location,
                    job_id=self.dataflow_job_id,
                    multiple_jobs=multiple_jobs,
                    project_id=self.dataflow_config.project_id,
                )
        return {"dataflow_job_id": self.dataflow_job_id}

    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info("Dataflow job with id: `%s` was requested to be cancelled.", self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
                location=self.dataflow_config.location,
            )


class BeamRunGoPipelineOperator(BeamBasePipelineOperator):
    """
    Launch Apache Beam pipelines written in Go.

    Note that both ``default_pipeline_options`` and ``pipeline_options``
    will be merged to specify pipeline execution parameter, and
    ``default_pipeline_options`` is expected to save high-level options,
    for instances, project and zone information, which apply to all beam
    operators in the DAG.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BeamRunGoPipelineOperator`

    .. seealso::
        For more detail on Apache Beam have a look at the reference:
        https://beam.apache.org/documentation/

    :param go_file: Reference to the Apache Beam pipeline Go source file,
        e.g. /local/path/to/main.go or gs://bucket/path/to/main.go.
        Exactly one of go_file and launcher_binary must be provided.

    :param launcher_binary: Reference to the Apache Beam pipeline Go binary compiled for the launching
        platform, e.g. /local/path/to/launcher-main or gs://bucket/path/to/launcher-main.
        Exactly one of go_file and launcher_binary must be provided.

    :param worker_binary: Reference to the Apache Beam pipeline Go binary compiled for the worker platform,
        e.g. /local/path/to/worker-main or gs://bucket/path/to/worker-main.
        Needed if the OS or architecture of the workers running the pipeline is different from that
        of the platform launching the pipeline. For more information, see the Apache Beam documentation
        for Go cross compilation: https://beam.apache.org/documentation/sdks/go-cross-compilation/.
        If launcher_binary is not set, providing a worker_binary will have no effect. If launcher_binary is
        set and worker_binary is not, worker_binary will default to the value of launcher_binary.
    """

    template_fields = [
        "go_file",
        "launcher_binary",
        "worker_binary",
        "runner",
        "pipeline_options",
        "default_pipeline_options",
        "dataflow_config",
    ]
    template_fields_renderers = {"dataflow_config": "json", "pipeline_options": "json"}
    operator_extra_links = (DataflowJobLink(),) if GOOGLE_PROVIDER else ()

    def __init__(
        self,
        *,
        go_file: str = "",
        launcher_binary: str = "",
        worker_binary: str = "",
        runner: str = "DirectRunner",
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        dataflow_config: DataflowConfiguration | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            runner=runner,
            default_pipeline_options=default_pipeline_options,
            pipeline_options=pipeline_options,
            gcp_conn_id=gcp_conn_id,
            dataflow_config=dataflow_config,
            **kwargs,
        )
        self.go_file = go_file
        self.launcher_binary = launcher_binary
        self.worker_binary = worker_binary or launcher_binary

    def execute(self, context: Context):
        """Execute the Apache Beam Pipeline."""
        if not exactly_one(self.go_file, self.launcher_binary):
            raise ValueError("Exactly one of `go_file` and `launcher_binary` must be set")
        self._execute_context = context
        self._cast_dataflow_config()
        if self.dataflow_config.impersonation_chain:
            self.log.warning(
                "Impersonation chain parameter is not supported for Apache Beam GO SDK and will be skipped "
                "in the execution"
            )
        self.dataflow_support_impersonation = False
        self.pipeline_options.setdefault("labels", {}).update(
            {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
        )

        (
            is_dataflow,
            dataflow_job_name,
            snake_case_pipeline_options,
            process_line_callback,
            _,
        ) = self._init_pipeline_options(format_pipeline_options=True, job_name_variable_key="job_name")

        if not self.beam_hook:
            raise AirflowException("Beam hook is not defined.")

        go_artifact: _GoArtifact = (
            _GoFile(file=self.go_file)
            if self.go_file
            else _GoBinary(launcher=self.launcher_binary, worker=self.worker_binary)
        )

        with ExitStack() as exit_stack:
            if go_artifact.is_located_on_gcs():
                gcs_hook = GCSHook(self.gcp_conn_id)
                tmp_dir = exit_stack.enter_context(tempfile.TemporaryDirectory(prefix="apache-beam-go"))
                go_artifact.download_from_gcs(gcs_hook=gcs_hook, tmp_dir=tmp_dir)

            if is_dataflow and self.dataflow_hook:
                go_artifact.start_pipeline(
                    beam_hook=self.beam_hook,
                    variables=snake_case_pipeline_options,
                    process_line_callback=process_line_callback,
                )
                DataflowJobLink.persist(context=context)
                if dataflow_job_name and self.dataflow_config.location:
                    self.dataflow_hook.wait_for_done(
                        job_name=dataflow_job_name,
                        location=self.dataflow_config.location,
                        job_id=self.dataflow_job_id,
                        multiple_jobs=False,
                        project_id=self.dataflow_config.project_id,
                    )
                return {"dataflow_job_id": self.dataflow_job_id}
            go_artifact.start_pipeline(
                beam_hook=self.beam_hook,
                variables=snake_case_pipeline_options,
                process_line_callback=process_line_callback,
            )

    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info("Dataflow job with id: `%s` was requested to be cancelled.", self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
                location=self.dataflow_config.location,
            )


class _GoArtifact(ABC):
    @abstractmethod
    def is_located_on_gcs(self) -> bool: ...

    @abstractmethod
    def download_from_gcs(self, gcs_hook: GCSHook, tmp_dir: str) -> None: ...

    @abstractmethod
    def start_pipeline(
        self,
        beam_hook: BeamHook,
        variables: dict,
        process_line_callback: Callable[[str], None] | None = None,
    ) -> None: ...


class _GoFile(_GoArtifact):
    def __init__(self, file: str) -> None:
        self.file = file
        self.should_init_go_module = False

    def is_located_on_gcs(self) -> bool:
        return _object_is_located_on_gcs(self.file)

    def download_from_gcs(self, gcs_hook: GCSHook, tmp_dir: str) -> None:
        self.file = _download_object_from_gcs(gcs_hook=gcs_hook, uri=self.file, tmp_dir=tmp_dir)
        self.should_init_go_module = True

    def start_pipeline(
        self,
        beam_hook: BeamHook,
        variables: dict,
        process_line_callback: Callable[[str], None] | None = None,
    ) -> None:
        beam_hook.start_go_pipeline(
            variables=variables,
            go_file=self.file,
            process_line_callback=process_line_callback,
            should_init_module=self.should_init_go_module,
        )


class _GoBinary(_GoArtifact):
    def __init__(self, launcher: str, worker: str) -> None:
        self.launcher = launcher
        self.worker = worker

    def is_located_on_gcs(self) -> bool:
        return any(_object_is_located_on_gcs(path) for path in (self.launcher, self.worker))

    def download_from_gcs(self, gcs_hook: GCSHook, tmp_dir: str) -> None:
        binaries_are_equal = self.launcher == self.worker

        binaries_to_download = []

        if _object_is_located_on_gcs(self.launcher):
            binaries_to_download.append("launcher")

        if not binaries_are_equal and _object_is_located_on_gcs(self.worker):
            binaries_to_download.append("worker")

        download_fn = partial(_download_object_from_gcs, gcs_hook=gcs_hook, tmp_dir=tmp_dir)

        with ThreadPoolExecutor(max_workers=len(binaries_to_download)) as executor:
            futures = {
                executor.submit(download_fn, uri=getattr(self, binary), tmp_prefix=f"{binary}-"): binary
                for binary in binaries_to_download
            }

            for future in as_completed(futures):
                binary = futures[future]
                tmp_path = future.result()
                _make_executable(tmp_path)
                setattr(self, binary, tmp_path)

        if binaries_are_equal:
            self.worker = self.launcher

    def start_pipeline(
        self,
        beam_hook: BeamHook,
        variables: dict,
        process_line_callback: Callable[[str], None] | None = None,
    ) -> None:
        beam_hook.start_go_pipeline_with_binary(
            variables=variables,
            launcher_binary=self.launcher,
            worker_binary=self.worker,
            process_line_callback=process_line_callback,
        )


def _object_is_located_on_gcs(path: str) -> bool:
    return path.lower().startswith("gs://")


def _download_object_from_gcs(gcs_hook: GCSHook, uri: str, tmp_dir: str, tmp_prefix: str = "") -> str:
    tmp_name = f"{tmp_prefix}{os.path.basename(uri)}"
    tmp_path = os.path.join(tmp_dir, tmp_name)

    bucket, prefix = _parse_gcs_url(uri)
    gcs_hook.download(bucket_name=bucket, object_name=prefix, filename=tmp_path)

    return tmp_path


def _make_executable(path: str) -> None:
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)
