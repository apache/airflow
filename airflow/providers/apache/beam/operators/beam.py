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
import tempfile
from abc import ABC, ABCMeta
from contextlib import ExitStack
from typing import TYPE_CHECKING, Callable, Sequence

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType
from airflow.providers.google.cloud.hooks.dataflow import (
    DataflowHook,
    process_line_and_extract_dataflow_job_id_callback,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.links.dataflow import DataflowJobLink
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning, DataflowConfiguration
from airflow.utils.helpers import convert_camel_to_snake
from airflow.version import version

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BeamDataflowMixin(metaclass=ABCMeta):
    """
    Helper class to store common, Dataflow specific logic for both
    :class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`,
    :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator` and
    :class:`~airflow.providers.apache.beam.operators.beam.BeamRunGoPipelineOperator`.
    """

    dataflow_hook: DataflowHook | None
    dataflow_config: DataflowConfiguration
    gcp_conn_id: str
    delegate_to: str | None
    dataflow_support_impersonation: bool = True

    def _set_dataflow(
        self,
        pipeline_options: dict,
        job_name_variable_key: str | None = None,
    ) -> tuple[str, dict, Callable[[str], None]]:
        self.dataflow_hook = self.__set_dataflow_hook()
        self.dataflow_config.project_id = self.dataflow_config.project_id or self.dataflow_hook.project_id
        dataflow_job_name = self.__get_dataflow_job_name()
        pipeline_options = self.__get_dataflow_pipeline_options(
            pipeline_options, dataflow_job_name, job_name_variable_key
        )
        process_line_callback = self.__get_dataflow_process_callback()
        return dataflow_job_name, pipeline_options, process_line_callback

    def __set_dataflow_hook(self) -> DataflowHook:
        self.dataflow_hook = DataflowHook(
            gcp_conn_id=self.dataflow_config.gcp_conn_id or self.gcp_conn_id,
            delegate_to=self.dataflow_config.delegate_to or self.delegate_to,
            poll_sleep=self.dataflow_config.poll_sleep,
            impersonation_chain=self.dataflow_config.impersonation_chain,
            drain_pipeline=self.dataflow_config.drain_pipeline,
            cancel_timeout=self.dataflow_config.cancel_timeout,
            wait_until_finished=self.dataflow_config.wait_until_finished,
        )
        return self.dataflow_hook

    def __get_dataflow_job_name(self) -> str:
        return DataflowHook.build_dataflow_job_name(
            self.dataflow_config.job_name, self.dataflow_config.append_job_name
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
    :param delegate_to:  Optional.
        The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param dataflow_config: Dataflow configuration, used when runner type is set to DataflowRunner,
        (optional) defaults to None.
    """

    def __init__(
        self,
        *,
        runner: str = "DirectRunner",
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        dataflow_config: DataflowConfiguration | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.runner = runner
        self.default_pipeline_options = default_pipeline_options or {}
        self.pipeline_options = pipeline_options or {}
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        if isinstance(dataflow_config, dict):
            self.dataflow_config = DataflowConfiguration(**dataflow_config)
        else:
            self.dataflow_config = dataflow_config or DataflowConfiguration()
        self.beam_hook: BeamHook | None = None
        self.dataflow_hook: DataflowHook | None = None
        self.dataflow_job_id: str | None = None

        if self.dataflow_config and self.runner.lower() != BeamRunnerType.DataflowRunner.lower():
            self.log.warning(
                "dataflow_config is defined but runner is different than DataflowRunner (%s)", self.runner
            )

    def _init_pipeline_options(
        self,
        format_pipeline_options: bool = False,
        job_name_variable_key: str | None = None,
    ) -> tuple[bool, str | None, dict, Callable[[str], None] | None]:
        self.beam_hook = BeamHook(runner=self.runner)
        pipeline_options = self.default_pipeline_options.copy()
        process_line_callback: Callable[[str], None] | None = None
        is_dataflow = self.runner.lower() == BeamRunnerType.DataflowRunner.lower()
        dataflow_job_name: str | None = None
        if is_dataflow:
            dataflow_job_name, pipeline_options, process_line_callback = self._set_dataflow(
                pipeline_options=pipeline_options,
                job_name_variable_key=job_name_variable_key,
            )
            self.log.info(pipeline_options)

        pipeline_options.update(self.pipeline_options)

        if format_pipeline_options:
            snake_case_pipeline_options = {
                convert_camel_to_snake(key): pipeline_options[key] for key in pipeline_options
            }
            return is_dataflow, dataflow_job_name, snake_case_pipeline_options, process_line_callback

        return is_dataflow, dataflow_job_name, pipeline_options, process_line_callback


class BeamRunPythonPipelineOperator(BeamBasePipelineOperator):
    """
    Launching Apache Beam pipelines written in Python. Note that both
    ``default_pipeline_options`` and ``pipeline_options`` will be merged to specify pipeline
    execution parameter, and ``default_pipeline_options`` is expected to save
    high-level options, for instances, project and zone information, which
    apply to all beam operators in the DAG.

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
    """

    template_fields: Sequence[str] = (
        "py_file",
        "runner",
        "pipeline_options",
        "default_pipeline_options",
        "dataflow_config",
    )
    template_fields_renderers = {"dataflow_config": "json", "pipeline_options": "json"}
    operator_extra_links = (DataflowJobLink(),)

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
        delegate_to: str | None = None,
        dataflow_config: DataflowConfiguration | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            runner=runner,
            default_pipeline_options=default_pipeline_options,
            pipeline_options=pipeline_options,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            dataflow_config=dataflow_config,
            **kwargs,
        )

        self.py_file = py_file
        self.py_options = py_options or []
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages
        self.pipeline_options.setdefault("labels", {}).update(
            {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
        )

    def execute(self, context: Context):
        """Execute the Apache Beam Pipeline."""
        (
            is_dataflow,
            dataflow_job_name,
            snake_case_pipeline_options,
            process_line_callback,
        ) = self._init_pipeline_options(format_pipeline_options=True, job_name_variable_key="job_name")

        if not self.beam_hook:
            raise AirflowException("Beam hook is not defined.")

        with ExitStack() as exit_stack:
            if self.py_file.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id, self.delegate_to)
                tmp_gcs_file = exit_stack.enter_context(gcs_hook.provide_file(object_url=self.py_file))
                self.py_file = tmp_gcs_file.name

            if is_dataflow and self.dataflow_hook:
                with self.dataflow_hook.provide_authorized_gcloud():
                    self.beam_hook.start_python_pipeline(
                        variables=snake_case_pipeline_options,
                        py_file=self.py_file,
                        py_options=self.py_options,
                        py_interpreter=self.py_interpreter,
                        py_requirements=self.py_requirements,
                        py_system_site_packages=self.py_system_site_packages,
                        process_line_callback=process_line_callback,
                    )
                DataflowJobLink.persist(
                    self,
                    context,
                    self.dataflow_config.project_id,
                    self.dataflow_config.location,
                    self.dataflow_job_id,
                )
                if dataflow_job_name and self.dataflow_config.location:
                    self.dataflow_hook.wait_for_done(
                        job_name=dataflow_job_name,
                        location=self.dataflow_config.location,
                        job_id=self.dataflow_job_id,
                        multiple_jobs=False,
                        project_id=self.dataflow_config.project_id,
                    )
                return {"dataflow_job_id": self.dataflow_job_id}
            else:
                self.beam_hook.start_python_pipeline(
                    variables=snake_case_pipeline_options,
                    py_file=self.py_file,
                    py_options=self.py_options,
                    py_interpreter=self.py_interpreter,
                    py_requirements=self.py_requirements,
                    py_system_site_packages=self.py_system_site_packages,
                    process_line_callback=process_line_callback,
                )

    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info("Dataflow job with id: `%s` was requested to be cancelled.", self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
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

    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        *,
        jar: str,
        runner: str = "DirectRunner",
        job_class: str | None = None,
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        dataflow_config: DataflowConfiguration | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            runner=runner,
            default_pipeline_options=default_pipeline_options,
            pipeline_options=pipeline_options,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            dataflow_config=dataflow_config,
            **kwargs,
        )
        self.jar = jar
        self.job_class = job_class

    def execute(self, context: Context):
        """Execute the Apache Beam Pipeline."""
        (
            is_dataflow,
            dataflow_job_name,
            pipeline_options,
            process_line_callback,
        ) = self._init_pipeline_options()

        if not self.beam_hook:
            raise AirflowException("Beam hook is not defined.")

        with ExitStack() as exit_stack:
            if self.jar.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id, self.delegate_to)
                tmp_gcs_file = exit_stack.enter_context(gcs_hook.provide_file(object_url=self.jar))
                self.jar = tmp_gcs_file.name

            if is_dataflow and self.dataflow_hook:
                is_running = False
                if self.dataflow_config.check_if_running != CheckJobRunning.IgnoreJob:
                    is_running = (
                        # The reason for disable=no-value-for-parameter is that project_id parameter is
                        # required but here is not passed, moreover it cannot be passed here.
                        # This method is wrapped by @_fallback_to_project_id_from_variables decorator which
                        # fallback project_id value from variables and raise error if project_id is
                        # defined both in variables and as parameter (here is already defined in variables)
                        self.dataflow_hook.is_job_dataflow_running(
                            name=self.dataflow_config.job_name,
                            variables=pipeline_options,
                        )
                    )
                    while is_running and self.dataflow_config.check_if_running == CheckJobRunning.WaitForRun:
                        # The reason for disable=no-value-for-parameter is that project_id parameter is
                        # required but here is not passed, moreover it cannot be passed here.
                        # This method is wrapped by @_fallback_to_project_id_from_variables decorator which
                        # fallback project_id value from variables and raise error if project_id is
                        # defined both in variables and as parameter (here is already defined in variables)

                        is_running = self.dataflow_hook.is_job_dataflow_running(
                            name=self.dataflow_config.job_name,
                            variables=pipeline_options,
                        )
                if not is_running:
                    pipeline_options["jobName"] = dataflow_job_name
                    with self.dataflow_hook.provide_authorized_gcloud():
                        self.beam_hook.start_java_pipeline(
                            variables=pipeline_options,
                            jar=self.jar,
                            job_class=self.job_class,
                            process_line_callback=process_line_callback,
                        )
                    if dataflow_job_name and self.dataflow_config.location:
                        multiple_jobs = self.dataflow_config.multiple_jobs or False
                        DataflowJobLink.persist(
                            self,
                            context,
                            self.dataflow_config.project_id,
                            self.dataflow_config.location,
                            self.dataflow_job_id,
                        )
                        self.dataflow_hook.wait_for_done(
                            job_name=dataflow_job_name,
                            location=self.dataflow_config.location,
                            job_id=self.dataflow_job_id,
                            multiple_jobs=multiple_jobs,
                            project_id=self.dataflow_config.project_id,
                        )
                return {"dataflow_job_id": self.dataflow_job_id}
            else:
                self.beam_hook.start_java_pipeline(
                    variables=pipeline_options,
                    jar=self.jar,
                    job_class=self.job_class,
                    process_line_callback=process_line_callback,
                )

    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info("Dataflow job with id: `%s` was requested to be cancelled.", self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
            )


class BeamRunGoPipelineOperator(BeamBasePipelineOperator):
    """
    Launching Apache Beam pipelines written in Go. Note that both
    ``default_pipeline_options`` and ``pipeline_options`` will be merged to specify pipeline
    execution parameter, and ``default_pipeline_options`` is expected to save
    high-level options, for instances, project and zone information, which
    apply to all beam operators in the DAG.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BeamRunGoPipelineOperator`

    .. seealso::
        For more detail on Apache Beam have a look at the reference:
        https://beam.apache.org/documentation/

    :param go_file: Reference to the Go Apache Beam pipeline e.g.,
        /some/local/file/path/to/your/go/pipeline/file.go
    """

    template_fields = [
        "go_file",
        "runner",
        "pipeline_options",
        "default_pipeline_options",
        "dataflow_config",
    ]
    template_fields_renderers = {"dataflow_config": "json", "pipeline_options": "json"}
    operator_extra_links = (DataflowJobLink(),)

    def __init__(
        self,
        *,
        go_file: str,
        runner: str = "DirectRunner",
        default_pipeline_options: dict | None = None,
        pipeline_options: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        dataflow_config: DataflowConfiguration | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            runner=runner,
            default_pipeline_options=default_pipeline_options,
            pipeline_options=pipeline_options,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            dataflow_config=dataflow_config,
            **kwargs,
        )

        if self.dataflow_config.impersonation_chain:
            self.log.info(
                "Impersonation chain parameter is not supported for Apache Beam GO SDK and will be skipped "
                "in the execution"
            )
        self.dataflow_support_impersonation = False

        self.go_file = go_file
        self.should_init_go_module = False
        self.pipeline_options.setdefault("labels", {}).update(
            {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
        )

    def execute(self, context: Context):
        """Execute the Apache Beam Pipeline."""
        (
            is_dataflow,
            dataflow_job_name,
            snake_case_pipeline_options,
            process_line_callback,
        ) = self._init_pipeline_options(format_pipeline_options=True, job_name_variable_key="job_name")

        if not self.beam_hook:
            raise AirflowException("Beam hook is not defined.")

        with ExitStack() as exit_stack:
            if self.go_file.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id, self.delegate_to)

                tmp_dir = exit_stack.enter_context(tempfile.TemporaryDirectory(prefix="apache-beam-go"))
                tmp_gcs_file = exit_stack.enter_context(
                    gcs_hook.provide_file(object_url=self.go_file, dir=tmp_dir)
                )
                self.go_file = tmp_gcs_file.name
                self.should_init_go_module = True

            if is_dataflow and self.dataflow_hook:
                with self.dataflow_hook.provide_authorized_gcloud():
                    self.beam_hook.start_go_pipeline(
                        variables=snake_case_pipeline_options,
                        go_file=self.go_file,
                        process_line_callback=process_line_callback,
                        should_init_module=self.should_init_go_module,
                    )

                DataflowJobLink.persist(
                    self,
                    context,
                    self.dataflow_config.project_id,
                    self.dataflow_config.location,
                    self.dataflow_job_id,
                )
                if dataflow_job_name and self.dataflow_config.location:
                    self.dataflow_hook.wait_for_done(
                        job_name=dataflow_job_name,
                        location=self.dataflow_config.location,
                        job_id=self.dataflow_job_id,
                        multiple_jobs=False,
                        project_id=self.dataflow_config.project_id,
                    )
                return {"dataflow_job_id": self.dataflow_job_id}
            else:
                self.beam_hook.start_go_pipeline(
                    variables=snake_case_pipeline_options,
                    go_file=self.go_file,
                    process_line_callback=process_line_callback,
                    should_init_module=self.should_init_go_module,
                )

    def on_kill(self) -> None:
        if self.dataflow_hook and self.dataflow_job_id:
            self.log.info("Dataflow job with id: `%s` was requested to be cancelled.", self.dataflow_job_id)
            self.dataflow_hook.cancel_job(
                job_id=self.dataflow_job_id,
                project_id=self.dataflow_config.project_id,
            )
