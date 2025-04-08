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

.. _write-logs-azure:

Writing logs to Azure Blob Storage
----------------------------------

Airflow can be configured to read and write task logs in Azure Blob Storage. It uses an existing
Airflow connection to read or write logs. If you don't have a connection properly setup,
this process will fail.

Follow the steps below to enable Azure Blob Storage logging:

To enable this feature, ``airflow.cfg`` must be configured as in this
example:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True
    remote_base_log_folder = wasb-base-folder/path/to/logs

    [azure_remote_logging]
    remote_wasb_log_container = my-container

#. Install the provider package with ``pip install apache-airflow-providers-microsoft-azure``
#. Ensure :ref:`connection <howto/connection:wasb>` is already setup with read and write access to Azure Blob Storage in the ``remote_wasb_log_container`` container and path ``remote_base_log_folder``.
#. Setup the above configuration values. Please note that the ``remote_base_log_folder`` should start with ``wasb`` to select the correct handler as shown above and the container should already exist.
#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the container at the specified base path you have defined.
#. Verify that the Azure Blob Storage viewer is working in the UI. Pull up a newly executed task, and verify that you see something like:

.. code-block:: none

    *** Found remote logs:
    ***   * https://my-container.blob.core.windows.net/wasb-base-folder/path/to/logs/dag_id=tutorial_dag/run_id=manual__2023-07-22T22:22:25.891267+00:00/task_id=load/attempt=1.log
    [2023-07-23, 03:52:47] {taskinstance.py:1144} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: tutorial_dag.load manual__2023-07-22T22:22:25.891267+00:00 [queued]>
    [2023-07-23, 03:52:47] {taskinstance.py:1144} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: tutorial_dag.load manual__2023-07-22T22:22:25.891267+00:00 [queued]>
    [2023-07-23, 03:52:47] {taskinstance.py:1346} INFO - Starting attempt 1 of 3

**Note** that the path to the remote log file is listed in the second line.
