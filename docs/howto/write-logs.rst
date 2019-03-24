..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Writing Logs
============

Writing Logs Locally
--------------------

Users can specify the directory to place log files in ``airflow.cfg`` using
``base_log_folder``. By default, logs are placed in the ``AIRFLOW_HOME``
directory.

The following convention is followed while naming logs: ``{dag_id}/{task_id}/{execution_date}/{try_number}.log``

In addition, users can supply a remote location to store current logs and backups.

In the Airflow Web UI, local logs take precedence over remote logs. If local logs
can not be found or accessed, the remote logs will be displayed. Note that logs
are only sent to remote storage once a task is complete (including failure); In other words, remote logs for
running tasks are unavailable.

Before you begin
''''''''''''''''

Remote logging uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.

.. _write-logs-amazon:

Writing Logs to Amazon S3
-------------------------


Enabling remote logging
'''''''''''''''''''''''

To enable this feature, ``airflow.cfg`` must be configured as follows:

.. code-block:: bash

    [core]
    # Airflow can store logs remotely in AWS S3. Users must supply a remote
    # location URL (starting with either 's3://...') and an Airflow connection
    # id that provides access to the storage location.
    remote_logging = True
    remote_base_log_folder = s3://my-bucket/path/to/logs
    remote_log_conn_id = MyS3Conn
    # Use server-side encryption for logs stored in S3
    encrypt_s3_logs = False

In the above example, Airflow will try to use ``S3Hook('MyS3Conn')``.

.. _write-logs-azure:

Writing Logs to Azure Blob Storage
----------------------------------

Airflow can be configured to read and write task logs in Azure Blob Storage.

Follow the steps below to enable Azure Blob Storage logging:

#. Airflow's logging system requires a custom `.py` file to be located in the ``PYTHONPATH``, so that it's importable from Airflow. Start by creating a directory to store the config file, ``$AIRFLOW_HOME/config`` is recommended.
#. Create empty files called ``$AIRFLOW_HOME/config/log_config.py`` and ``$AIRFLOW_HOME/config/__init__.py``.
#. Copy the contents of ``airflow/config_templates/airflow_local_settings.py`` into the ``log_config.py`` file created in `Step 2`.
#. Customize the following portions of the template:

    .. code-block:: bash

        # wasb buckets should start with "wasb" just to help Airflow select correct handler
        REMOTE_BASE_LOG_FOLDER = 'wasb-<whatever you want here>'

        # Rename DEFAULT_LOGGING_CONFIG to LOGGING CONFIG
        LOGGING_CONFIG = ...


#. Make sure a Azure Blob Storage (Wasb) connection hook has been defined in Airflow. The hook should have read and write access to the Azure Blob Storage bucket defined above in ``REMOTE_BASE_LOG_FOLDER``.

#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: bash

        remote_logging = True
        logging_config_class = log_config.LOGGING_CONFIG
        remote_log_conn_id = <name of the Azure Blob Storage connection>

#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you've defined.

.. _write-logs-gcp:

Writing Logs to Google Cloud Storage
------------------------------------

Follow the steps below to enable Google Cloud Storage logging.

To enable this feature, ``airflow.cfg`` must be configured as in this
example:

.. code-block:: bash

    [core]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True
    remote_base_log_folder = gs://my-bucket/path/to/logs
    remote_log_conn_id = MyGCSConn

#. Install the ``gcp`` package first, like so: ``pip install apache-airflow[gcp]``.
#. Make sure a Google Cloud Platform connection hook has been defined in Airflow. The hook should have read and write access to the Google Cloud Storage bucket defined above in ``remote_base_log_folder``.
#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you've defined.
#. Verify that the Google Cloud Storage viewer is working in the UI. Pull up a newly executed task, and verify that you see something like:

.. code-block:: bash

  *** Reading remote log from gs://<bucket where logs should be persisted>/example_bash_operator/run_this_last/2017-10-03T00:00:00/16.log.
  [2017-10-03 21:57:50,056] {cli.py:377} INFO - Running on host chrisr-00532
  [2017-10-03 21:57:50,093] {base_task_runner.py:115} INFO - Running: ['bash', '-c', u'airflow run example_bash_operator run_this_last 2017-10-03T00:00:00 --job_id 47 --raw -sd DAGS_FOLDER/example_dags/example_bash_operator.py']
  [2017-10-03 21:57:51,264] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,263] {__init__.py:45} INFO - Using executor SequentialExecutor
  [2017-10-03 21:57:51,306] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,306] {models.py:186} INFO - Filling up the DagBag from /airflow/dags/example_dags/example_bash_operator.py

**Note** that the path to the remote log file is listed on the first line.
