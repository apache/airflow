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

:py:mod:`airflow.providers.apache.beam.triggers.beam`
=====================================================

.. py:module:: airflow.providers.apache.beam.triggers.beam


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.apache.beam.triggers.beam.BeamPipelineBaseTrigger
   airflow.providers.apache.beam.triggers.beam.BeamPythonPipelineTrigger
   airflow.providers.apache.beam.triggers.beam.BeamJavaPipelineTrigger




.. py:class:: BeamPipelineBaseTrigger(**kwargs)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for Beam Pipeline Triggers.


.. py:class:: BeamPythonPipelineTrigger(variables, py_file, py_options = None, py_interpreter = 'python3', py_requirements = None, py_system_site_packages = False, runner = 'DirectRunner', gcp_conn_id = 'google_cloud_default')


   Bases: :py:obj:`BeamPipelineBaseTrigger`

   Trigger to perform checking the Python pipeline status until it reaches terminate state.

   :param variables: Variables passed to the pipeline.
   :param py_file: Path to the python file to execute.
   :param py_options: Additional options.
   :param py_interpreter: Python version of the Apache Beam pipeline. If ``None``, this defaults to the
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

   .. py:method:: serialize()

      Serialize BeamPythonPipelineTrigger arguments and classpath.


   .. py:method:: run()
      :async:

      Get current pipeline status and yields a TriggerEvent.



.. py:class:: BeamJavaPipelineTrigger(variables, jar, job_class = None, runner = 'DirectRunner', check_if_running = False, project_id = None, location = None, job_name = None, gcp_conn_id = 'google_cloud_default', impersonation_chain = None, poll_sleep = 10, cancel_timeout = None)


   Bases: :py:obj:`BeamPipelineBaseTrigger`

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

   .. py:method:: serialize()

      Serialize BeamJavaPipelineTrigger arguments and classpath.


   .. py:method:: run()
      :async:

      Get current Java pipeline status and yields a TriggerEvent.
