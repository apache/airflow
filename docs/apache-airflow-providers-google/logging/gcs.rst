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

.. _write-logs-gcp:

Writing Logs to Google Cloud Storage
------------------------------------

Remote logging to Google Cloud Storage uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.

Follow the steps below to enable Google Cloud Storage logging.

To enable this feature, ``airflow.cfg`` must be configured as in this
example:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True
    remote_base_log_folder = gs://my-bucket/path/to/logs

#. By default Application Default Credentials are used to obtain credentials. You can also
   set ``google_key_path`` option in ``[logging]`` section, if you want to use your own service account.
#. Make sure a Google Cloud account have read and write access to the Google Cloud Storage bucket defined above in ``remote_base_log_folder``.
#. Install the ``google`` package, like so: ``pip install 'apache-airflow[google]'``.
#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you have defined.
#. Verify that the Google Cloud Storage viewer is working in the UI. Pull up a newly executed task, and verify that you see something like:

.. code-block:: none

  *** Reading remote log from gs://<bucket where logs should be persisted>/example_bash_operator/run_this_last/2017-10-03T00:00:00/16.log.
  [2017-10-03 21:57:50,056] {cli.py:377} INFO - Running on host chrisr-00532
  [2017-10-03 21:57:50,093] {base_task_runner.py:115} INFO - Running: ['bash', '-c', 'airflow tasks run example_bash_operator run_this_last 2017-10-03T00:00:00 --job-id 47 --raw -S DAGS_FOLDER/example_dags/example_bash_operator.py']
  [2017-10-03 21:57:51,264] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,263] {__init__.py:45} INFO - Using executor SequentialExecutor
  [2017-10-03 21:57:51,306] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,306] {models.py:186} INFO - Filling up the DagBag from /airflow/dags/example_dags/example_bash_operator.py

**Note** that the path to the remote log file is listed on the first line.
