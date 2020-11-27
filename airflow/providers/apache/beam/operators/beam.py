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
import copy
import re
from contextlib import ExitStack
from typing import List, Optional

from airflow.models import BaseOperator
from airflow.providers.apache.beam.hooks.beam import BeamHook
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults
from airflow.version import version


class BeamRunPythonPipelineOperator(BaseOperator):
    """
    Launching Apache Beam pipelines written in Python. Note that both
    ``default_pipeline_options`` and ``pipeline_options`` will be merged to specify pipeline
    execution parameter, and ``default_pipeline_options`` is expected to save
    high-level options, for instances, project and zone information, which
    apply to all beam operators in the DAG.

    .. code-block:: python

        default_args = {
            'default_pipeline_options':
                {
                    'labels': 'example-label'
                }
        }

        with models.DAG(
            "example_beam_native_python",
            default_args=default_args,
            start_date=days_ago(1),
            schedule_interval=None,
            tags=['example'],
        ) as dag_native_python:

            start_python_job_local_direct_runner = BeamRunPythonPipelineOperator(
                task_id="start_python_job_local_direct_runner",
                runner="DirectRunner",
                py_file='apache_beam.examples.wordcount',
                py_options=['-m'],
                py_requirements=['apache-beam[gcp]==2.21.0'],
                py_interpreter='python3',
                py_system_site_packages=False,
            )

    .. seealso::
        For more detail on Apache Beam have a look at the reference:
        https://beam.apache.org/documentation/

    :param py_file: Reference to the python Apache Beam pipeline file.py, e.g.,
        /some/local/file/path/to/your/python/pipeline/file. (templated)
    :type py_file: str
    :param runner: Runner on which pipeline will be run. By default "DirectRunner" is being used.
        See:
        https://beam.apache.org/documentation/runners/capability-matrix/
        If you use Dataflow runner check dedicated operator:
        :class:`~providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`
    :type runner: str
    :param py_options: Additional python options, e.g., ["-m", "-v"].
    :type py_options: list[str]
    :param default_pipeline_options: Map of default pipeline options.
    :type default_pipeline_options: dict
    :param pipeline_options: Map of pipeline options.The key must be a dictionary.
        The value can contain different types:

        * If the value is None, the single option - ``--key`` (without value) will be added.
        * If the value is False, this option will be skipped
        * If the value is True, the single option - ``--key`` (without value) will be added.
        * If the value is list, the many options will be added for each key.
          If the value is ``['A', 'B']`` and the key is ``key`` then the ``--key=A --key-B`` options
          will be left
        * Other value types will be replaced with the Python textual representation.

        When defining labels (``labels`` option), you can also provide a dictionary.
    :type pipeline_options: dict
    :param py_interpreter: Python version of the beam pipeline.
        If None, this defaults to the python3.
        To track python versions supported by beam and related
        issues check: https://issues.apache.org/jira/browse/BEAM-1251
    :type py_interpreter: str
    :param py_requirements: Additional python package(s) to install.
        If a value is passed to this parameter, a new virtual environment has been created with
        additional packages installed.

        You could also install the apache_beam package if it is not installed on your system or you want
        to use a different version.
    :type py_requirements: List[str]
    :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.

        This option is only relevant if the ``py_requirements`` parameter is not None.
    :param project_id: (DataflowRunner) Optional,
        the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :type project_id: str
    :param gcp_conn_id: (DataflowRunner) Optional.
        The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param job_name: (DataflowRunner) Optional.
        The 'job_name' to use when executing the DataFlow job (templated).
        This ends up being set in the pipeline options, so any entry
        with key ``'jobName'`` or ``'job_name'`` in ``options`` will be overwritten.
    :type job_name: str
    :param delegate_to: (DataflowRunner) Optional.
        The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ["py_file", "runner", "pipeline_options", "default_pipeline_options", "job_name"]

    @apply_defaults
    def __init__(
        self,
        *,
        py_file: str,
        runner: str = "DirectRunner",
        default_pipeline_options: Optional[dict] = None,
        pipeline_options: Optional[dict] = None,
        py_interpreter: str = "python3",
        py_options: Optional[List[str]] = None,
        py_requirements: Optional[List[str]] = None,
        py_system_site_packages: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        project_id: Optional[str] = None,
        job_name: str = "{{task.task_id}}",
        delegate_to: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.py_file = py_file
        self.runner = runner
        self.py_options = py_options or []
        self.default_pipeline_options = default_pipeline_options or {}
        self.pipeline_options = pipeline_options or {}
        self.pipeline_options.setdefault("labels", {}).update(
            {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
        )
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages
        self.gcp_conn_id = gcp_conn_id
        self.job_name = job_name
        self.project_id = project_id
        self.job_id = None
        self.delegate_to = delegate_to
        self.hook: Optional[DataflowHook] = None

    def execute(self, context):
        """Execute the Apache Beam Pipeline."""
        pipeline_options = self.default_pipeline_options.copy()
        pipeline_options.update(self.pipeline_options)
        # Convert argument names from lowerCamelCase to snake case.
        camel_to_snake = lambda name: re.sub(r"[A-Z]", lambda x: "_" + x.group(0).lower(), name)
        formatted_pipeline_options = {camel_to_snake(key): pipeline_options[key] for key in pipeline_options}

        with ExitStack() as exit_stack:
            if self.py_file.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id, self.delegate_to)
                tmp_gcs_file = exit_stack.enter_context(  # pylint: disable=no-member
                    gcs_hook.provide_file(object_url=self.py_file)
                )
                self.py_file = tmp_gcs_file.name

            if self.runner == "DataflowRunner":
                self.log.warning(
                    "For more advanced option of DataflowRunner use: "
                    "providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator"
                )
                self.hook = DataflowHook(
                    gcp_conn_id=self.gcp_conn_id,
                )

                def set_current_job_id(job_id):
                    self.job_id = job_id

                self.hook.start_python_dataflow(
                    project_id=self.project_id,
                    job_name=self.job_name,
                    variables=formatted_pipeline_options,
                    dataflow=self.py_file,
                    py_options=self.py_options,
                    py_interpreter=self.py_interpreter,
                    py_requirements=self.py_requirements,
                    py_system_site_packages=self.py_system_site_packages,
                    on_new_job_id_callback=set_current_job_id,
                )
            else:
                self.hook = BeamHook(runner=self.runner)
                self.hook.start_python_pipeline(
                    variables=formatted_pipeline_options,
                    py_file=self.py_file,
                    py_options=self.py_options,
                    py_interpreter=self.py_interpreter,
                    py_requirements=self.py_requirements,
                    py_system_site_packages=self.py_system_site_packages,
                )

    def on_kill(self) -> None:
        self.log.info("On kill.")
        if self.runner == "DataflowRunner" and self.job_id:
            self.hook.cancel_job(job_id=self.job_id, project_id=self.project_id)


# pylint: disable=too-many-instance-attributes
class BeamRunJavaPipelineOperator(BaseOperator):
    """
    Launching Apache Beam pipelines written in Java.

    Note that both
    ``default_pipeline_options`` and ``pipeline_options`` will be merged to specify pipeline
    execution parameter, and ``default_pipeline_options`` is expected to save
    high-level pipeline_options, for instances, project and zone information, which
    apply to all Apache Beam operators in the DAG.

    It's a good practice to define parameters in the default_args of the dag
    like the project, zone and staging location.

    .. code-block:: python

       default_args = {
            'default_pipeline_options':
                {
                    'labels': 'example-label'
                }
        }

    You need to pass the path to your jar file as a file reference with the ``jar``
    parameter, the jar needs to be a self executing jar (see documentation here:
    https://beam.apache.org/documentation/runners/dataflow/#self-executing-jar).
    Use ``pipeline_options`` to pass on pipeline_options to your job.

    .. code-block:: python

       t1 = BeamRunJavaPipelineOperator(
           task_id='start_java_job_spark_runner',
           jar='{{var.value.spark_runner_jar_base}}pipeline/build/libs/pipeline-example-1.0.jar',
           pipeline_options={
               'output': '/tmp/start_java_job_spark_runner',
               'inputFile': 'gs://apache-beam-samples/shakespeare/kinglear.txt,
           },
           dag=my-dag)

    .. seealso::
        For more detail on Apache Beam have a look at the reference:
        https://beam.apache.org/documentation/

    :param jar: The reference to a self executing Apache Beam jar (templated).
    :type jar: str
    :param runner: Runner on which pipeline will be run. By default "DirectRunner" is being used.
        See:
        https://beam.apache.org/documentation/runners/capability-matrix/
        If you use Dataflow runner check dedicated operator:
        :class:`~providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`
    :type runner: str
    :param job_class: The name of the Apache Beam pipeline class to be executed, it
        is often not the main class configured in the pipeline jar file.
    :type job_class: str
    :param default_pipeline_options: Map of default job pipeline_options.
    :type default_pipeline_options: dict
    :param pipeline_options: Map of job specific pipeline_options.The key must be a dictionary.
        The value can contain different types:

        * If the value is None, the single option - ``--key`` (without value) will be added.
        * If the value is False, this option will be skipped
        * If the value is True, the single option - ``--key`` (without value) will be added.
        * If the value is list, the many pipeline_options will be added for each key.
          If the value is ``['A', 'B']`` and the key is ``key`` then the ``--key=A --key-B`` pipeline_options
          will be left
        * Other value types will be replaced with the Python textual representation.

        When defining labels (``labels`` option), you can also provide a dictionary.
    :type pipeline_options: dict
    :param job_name: (DataflowRunner) The 'jobName' to use when executing the DataFlow job
        (templated). This ends up being set in the pipeline pipeline_options, so any entry
        with key ``'jobName'`` in ``pipeline_options`` will be overwritten.
    :type job_name: str
    :param project_id: (DataflowRunner) Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :type project_id: str
    :param gcp_conn_id: (DataflowRunner) The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: (DataflowRunner) The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ["jar", "runner", "job_class", "pipeline_options", "job_name"]
    ui_color = "#0273d4"

    @apply_defaults
    def __init__(
        self,
        *,
        jar: str,
        runner: str = "DirectRunner",
        job_class: Optional[str] = None,
        default_pipeline_options: Optional[dict] = None,
        pipeline_options: Optional[dict] = None,
        job_name: str = "{{task.task_id}}",
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.jar = jar
        self.runner = runner
        self.default_pipeline_options = default_pipeline_options or {}
        self.pipeline_options = pipeline_options or {}
        self.job_class = job_class
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.job_name = job_name
        self.delegate_to = delegate_to
        self.job_id = None
        self.hook = None

    def execute(self, context):
        """Execute the Apache Beam Pipeline."""
        pipeline_options = copy.copy(self.default_pipeline_options)
        pipeline_options.update(self.pipeline_options)

        with ExitStack() as exit_stack:
            if self.jar.lower().startswith("gs://"):
                gcs_hook = GCSHook(self.gcp_conn_id, self.delegate_to)
                tmp_gcs_file = exit_stack.enter_context(  # pylint: disable=no-member
                    gcs_hook.provide_file(object_url=self.jar)
                )
                self.jar = tmp_gcs_file.name

            if self.runner == "DataflowRunner":
                pipeline_options.setdefault("labels", {}).update(
                    {"airflow-version": "v" + version.replace(".", "-").replace("+", "-")}
                )

                self.log.warning(
                    "For more advanced option of DataflowRunner use: "
                    "providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator"
                )
                self.hook = DataflowHook(
                    gcp_conn_id=self.gcp_conn_id,
                    delegate_to=self.delegate_to,
                )

                is_running = self.hook.is_job_dataflow_running(  # type: ignore[attr-defined]
                    name=self.job_name,
                    variables=pipeline_options,
                    project_id=self.project_id,
                )
                while is_running:
                    is_running = self.hook.is_job_dataflow_running(  # type: ignore[attr-defined]
                        name=self.job_name,
                        variables=pipeline_options,
                        project_id=self.project_id,
                    )

                if not is_running:

                    def set_current_job_id(job_id):
                        self.job_id = job_id

                    self.hook.start_java_dataflow(  # type: ignore[attr-defined]
                        job_name=self.job_name,
                        variables=pipeline_options,
                        jar=self.jar,
                        job_class=self.job_class,
                        on_new_job_id_callback=set_current_job_id,
                        project_id=self.project_id,
                    )
            else:
                self.hook = BeamHook(runner=self.runner)
                self.hook.start_java_pipeline(  # type: ignore[attr-defined]
                    variables=pipeline_options,
                    jar=self.jar,
                    job_class=self.job_class,
                )

    def on_kill(self) -> None:
        self.log.info("On kill.")
        if self.runner == "DataflowRunner" and self.job_id:
            self.hook.cancel_job(job_id=self.job_id, project_id=self.project_id)
