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

.. _write-logs-stackdriver:

Writing Logs to Google Stackdriver
----------------------------------

Airflow can be configured to read and write task logs in `Google Stackdriver Logging <https://cloud.google.com/logging/>`__.

To enable this feature, ``airflow.cfg`` must be configured as in this
example:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True
    remote_base_log_folder = stackdriver://logs-name

All configuration options are in the ``[logging]`` section.

The value of field ``remote_logging`` must always be set to ``True`` for this feature to work.
Turning this option off will result in data not being sent to Stackdriver.
The ``remote_base_log_folder`` option contains the URL that specifies the type of handler to be used.
For integration with Stackdriver, this option should start with ``stackdriver:///``.
The path section of the URL specifies the name of the log e.g. ``stackdriver://airflow-tasks`` writes
logs under the name ``airflow-tasks``.

You can set ``google_key_path`` option in the ``[logging]`` section to specify the path to `the service
account key file <https://cloud.google.com/iam/docs/service-accounts>`__.
If omitted, authorization based on `the Application Default Credentials
<https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
be used.

By using the ``logging_config_class`` option you can get :ref:`advanced features <write-logs-advanced>` of
this handler. Details are available in the handler's documentation -
:class:`~airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverTaskHandler`.


.. _log-link-stackdriver:

Google Stackdriver External Link
''''''''''''''''''''''''''''''''

Airflow automatically shows a link to Google Stackdriver when configured to use it as the remote logging system.
