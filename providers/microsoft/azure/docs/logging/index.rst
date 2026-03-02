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
    # Airflow can store logs remotely in AWS S3, Azure Blob Storage, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True
    remote_base_log_folder = wasb://path/to/logs

    [azure_remote_logging]
    remote_wasb_log_container = my-container

.. note::
    If you are using environment variables, the equivalent configuration is:

    .. code-block:: bash

        export AIRFLOW__LOGGING__REMOTE_LOGGING=True
        export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=<your_wasb_connection_id>
        export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=wasb://path/to/logs
        export AIRFLOW__AZURE_REMOTE_LOGGING__REMOTE_WASB_LOG_CONTAINER=<your_container_name>

    The ``remote_base_log_folder`` must be prefixed with ``wasb://`` for Airflow to use the correct log handler. An incorrect format can cause a misleading ``ResourceNotFoundError``, even if the container exists.

Setup Steps:
''''''''''''''

#. Install the provider package with ``pip install apache-airflow-providers-microsoft-azure``.
#. Ensure :ref:`connection <howto/connection:wasb>` is already setup with read and write access to Azure Blob Storage in the ``remote_wasb_log_container`` container and path ``remote_base_log_folder``. The connection should be configured with appropriate authentication credentials (such as account key, shared access key, or managed identity). For account key authentication, you can add ``account_key`` to the connection's extra fields as a JSON dictionary: ``{"account_key": "your_account_key"}``.
#. Setup the above configuration values. Please note that the container should already exist.
#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the container at the specified base path you have defined.
#. Verify that the Azure Blob Storage viewer is working in the UI. Pull up a newly executed task, and verify that you see something like:

.. code-block:: none

    *** Found remote logs:
    ***   * https://my-container.blob.core.windows.net/path/to/logs/dag_id=tutorial_dag/run_id=manual.../task_id=load/attempt=1.log
    [2023-07-23, 03:52:47] {taskinstance.py:1144} INFO - Dependencies all met...

**Note** that the path to the remote log file is listed in the second line.
