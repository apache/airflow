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

:py:mod:`airflow.providers.apache.beam.hooks.beam`
==================================================

.. py:module:: airflow.providers.apache.beam.hooks.beam

.. autoapi-nested-parse::

   This module contains a Apache Beam Hook.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.apache.beam.hooks.beam.BeamRunnerType
   airflow.providers.apache.beam.hooks.beam.BeamHook
   airflow.providers.apache.beam.hooks.beam.BeamAsyncHook



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.apache.beam.hooks.beam.beam_options_to_args
   airflow.providers.apache.beam.hooks.beam.process_fd
   airflow.providers.apache.beam.hooks.beam.run_beam_command



.. py:class:: BeamRunnerType


   Helper class for listing runner types.

   For more information about runners see: https://beam.apache.org/documentation/

   .. py:attribute:: DataflowRunner
      :value: 'DataflowRunner'



   .. py:attribute:: DirectRunner
      :value: 'DirectRunner'



   .. py:attribute:: SparkRunner
      :value: 'SparkRunner'



   .. py:attribute:: FlinkRunner
      :value: 'FlinkRunner'



   .. py:attribute:: SamzaRunner
      :value: 'SamzaRunner'



   .. py:attribute:: NemoRunner
      :value: 'NemoRunner'



   .. py:attribute:: JetRunner
      :value: 'JetRunner'



   .. py:attribute:: Twister2Runner
      :value: 'Twister2Runner'




.. py:function:: beam_options_to_args(options)

   Return a formatted pipeline options from a dictionary of arguments.

   The logic of this method should be compatible with Apache Beam:
   https://github.com/apache/beam/blob/b56740f0e8cd80c2873412847d0b336837429fb9/sdks/python/
   apache_beam/options/pipeline_options.py#L230-L251

   :param options: Dictionary with options
   :return: List of arguments


.. py:function:: process_fd(proc, fd, log, process_line_callback = None, check_job_status_callback = None)

   Print output to logs.

   :param proc: subprocess.
   :param fd: File descriptor.
   :param process_line_callback: Optional callback which can be used to process
       stdout and stderr to detect job id.
   :param log: logger.


.. py:function:: run_beam_command(cmd, log, process_line_callback = None, working_directory = None, check_job_status_callback = None)

   Run pipeline command in subprocess.

   :param cmd: Parts of the command to be run in subprocess
   :param process_line_callback: Optional callback which can be used to process
       stdout and stderr to detect job id
   :param working_directory: Working directory
   :param log: logger.


.. py:class:: BeamHook(runner)


   Bases: :py:obj:`airflow.hooks.base.BaseHook`

   Hook for Apache Beam.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   :param runner: Runner type

   .. py:method:: start_python_pipeline(variables, py_file, py_options, py_interpreter = 'python3', py_requirements = None, py_system_site_packages = False, process_line_callback = None, check_job_status_callback = None)

      Start Apache Beam python pipeline.

      :param variables: Variables passed to the pipeline.
      :param py_file: Path to the python file to execute.
      :param py_options: Additional options.
      :param py_interpreter: Python version of the Apache Beam pipeline.
          If None, this defaults to the python3.
          To track python versions supported by beam and related
          issues check: https://issues.apache.org/jira/browse/BEAM-1251
      :param py_requirements: Additional python package(s) to install.
          If a value is passed to this parameter, a new virtual environment has been created with
          additional packages installed.

          You could also install the apache-beam package if it is not installed on your system, or you want
          to use a different version.
      :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
          See virtualenv documentation for more information.

          This option is only relevant if the ``py_requirements`` parameter is not None.
      :param process_line_callback: (optional) Callback that can be used to process each line of
          the stdout and stderr file descriptors.


   .. py:method:: start_java_pipeline(variables, jar, job_class = None, process_line_callback = None)

      Start Apache Beam Java pipeline.

      :param variables: Variables passed to the job.
      :param jar: Name of the jar for the pipeline
      :param job_class: Name of the java class for the pipeline.
      :param process_line_callback: (optional) Callback that can be used to process each line of
          the stdout and stderr file descriptors.


   .. py:method:: start_go_pipeline(variables, go_file, process_line_callback = None, should_init_module = False)

      Start Apache Beam Go pipeline with a source file.

      :param variables: Variables passed to the job.
      :param go_file: Path to the Go file with your beam pipeline.
      :param process_line_callback: (optional) Callback that can be used to process each line of
          the stdout and stderr file descriptors.
      :param should_init_module: If False (default), will just execute a `go run` command. If True, will
          init a module and dependencies with a ``go mod init`` and ``go mod tidy``, useful when pulling
          source with GCSHook.
      :return:


   .. py:method:: start_go_pipeline_with_binary(variables, launcher_binary, worker_binary, process_line_callback = None)

      Start Apache Beam Go pipeline with an executable binary.

      :param variables: Variables passed to the job.
      :param launcher_binary: Path to the binary compiled for the launching platform.
      :param worker_binary: Path to the binary compiled for the worker platform.
      :param process_line_callback: (optional) Callback that can be used to process each line of
          the stdout and stderr file descriptors.



.. py:class:: BeamAsyncHook(runner)


   Bases: :py:obj:`BeamHook`

   Asynchronous hook for Apache Beam.

   :param runner: Runner type.

   .. py:method:: start_python_pipeline_async(variables, py_file, py_options = None, py_interpreter = 'python3', py_requirements = None, py_system_site_packages = False)
      :async:

      Start Apache Beam python pipeline.

      :param variables: Variables passed to the pipeline.
      :param py_file: Path to the python file to execute.
      :param py_options: Additional options.
      :param py_interpreter: Python version of the Apache Beam pipeline.
          If None, this defaults to the python3.
          To track python versions supported by beam and related
          issues check: https://issues.apache.org/jira/browse/BEAM-1251
      :param py_requirements: Additional python package(s) to install.
          If a value is passed to this parameter, a new virtual environment has been created with
          additional packages installed.
          You could also install the apache-beam package if it is not installed on your system, or you want
          to use a different version.
      :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
          See virtualenv documentation for more information.
          This option is only relevant if the ``py_requirements`` parameter is not None.


   .. py:method:: start_java_pipeline_async(variables, jar, job_class = None)
      :async:

      Start Apache Beam Java pipeline.

      :param variables: Variables passed to the job.
      :param jar: Name of the jar for the pipeline.
      :param job_class: Name of the java class for the pipeline.
      :return: Beam command execution return code.


   .. py:method:: start_pipeline_async(variables, command_prefix, working_directory = None)
      :async:


   .. py:method:: run_beam_command_async(cmd, log, working_directory = None)
      :async:

      Run pipeline command in subprocess.

      :param cmd: Parts of the command to be run in subprocess
      :param working_directory: Working directory
      :param log: logger.


   .. py:method:: read_logs(stream_reader)
      :async:
