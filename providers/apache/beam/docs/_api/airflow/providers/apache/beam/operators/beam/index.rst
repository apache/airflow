 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.apache.beam.operators.beam`
======================================================

.. py:module:: airflow.providers.apache.beam.operators.beam

.. autoapi-nested-parse::

   This module contains Apache Beam operators.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.apache.beam.operators.beam.BeamDataflowMixin
   airflow.providers.apache.beam.operators.beam.BeamBasePipelineOperator
   airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator
   airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator
   airflow.providers.apache.beam.operators.beam.BeamRunGoPipelineOperator




.. py:class:: BeamDataflowMixin


   Helper class to store common, Dataflow specific logic for both.

   :class:`~airflow.providers.apache.beam.operators.beam.BeamRunPythonPipelineOperator`,
   :class:`~airflow.providers.apache.beam.operators.beam.BeamRunJavaPipelineOperator` and
   :class:`~airflow.providers.apache.beam.operators.beam.BeamRunGoPipelineOperator`.

   .. py:attribute:: dataflow_hook
      :type: airflow.providers.google.cloud.hooks.dataflow.DataflowHook | None



   .. py:attribute:: dataflow_config
      :type: airflow.providers.google.cloud.operators.dataflow.DataflowConfiguration



   .. py:attribute:: gcp_conn_id
      :type: str



   .. py:attribute:: dataflow_support_impersonation
      :type: bool
      :value: True




.. py:class:: BeamBasePipelineOperator(*, runner = 'DirectRunner', default_pipeline_options = None, pipeline_options = None, gcp_conn_id = 'google_cloud_default', dataflow_config = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`, :py:obj:`BeamDataflowMixin`, :py:obj:`abc.ABC`

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

   .. py:property:: dataflow_job_id


   .. py:method:: execute_complete(context, event)

      Execute when the trigger fires - returns immediately.

      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: BeamRunPythonPipelineOperator(*, py_file, runner = 'DirectRunner', default_pipeline_options = None, pipeline_options = None, py_interpreter = 'python3', py_options = None, py_requirements = None, py_system_site_packages = False, gcp_conn_id = 'google_cloud_default', dataflow_config = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`BeamBasePipelineOperator`

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

   .. py:attribute:: template_fields
      :type: collections.abc.Sequence[str]
      :value: ('py_file', 'runner', 'pipeline_options', 'default_pipeline_options', 'dataflow_config')



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Execute the Apache Beam Python Pipeline.


   .. py:method:: execute_sync(context)


   .. py:method:: execute_async(context)


   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.



.. py:class:: BeamRunJavaPipelineOperator(*, jar, runner = 'DirectRunner', job_class = None, default_pipeline_options = None, pipeline_options = None, gcp_conn_id = 'google_cloud_default', dataflow_config = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`BeamBasePipelineOperator`

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

   .. py:attribute:: template_fields
      :type: collections.abc.Sequence[str]
      :value: ('jar', 'runner', 'job_class', 'pipeline_options', 'default_pipeline_options', 'dataflow_config')



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#0273d4'



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Execute the Apache Beam Python Pipeline.


   .. py:method:: execute_sync(context)

      Execute the Apache Beam Pipeline.


   .. py:method:: execute_async(context)


   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.



.. py:class:: BeamRunGoPipelineOperator(*, go_file = '', launcher_binary = '', worker_binary = '', runner = 'DirectRunner', default_pipeline_options = None, pipeline_options = None, gcp_conn_id = 'google_cloud_default', dataflow_config = None, **kwargs)


   Bases: :py:obj:`BeamBasePipelineOperator`

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

   .. py:attribute:: template_fields
      :value: ['go_file', 'launcher_binary', 'worker_binary', 'runner', 'pipeline_options',...



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: operator_extra_links
      :value: ()



   .. py:method:: execute(context)

      Execute the Apache Beam Pipeline.


   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.
