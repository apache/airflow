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



Logging for Tasks
=================

Writing Logs Locally
--------------------

Users can specify the directory to place log files in ``airflow.cfg`` using
``base_log_folder``. By default, logs are placed in the ``AIRFLOW_HOME``
directory.

.. note::
    For more information on setting the configuration, see :doc:`/howto/set-config`

The following convention is followed while naming logs: ``{dag_id}/{task_id}/{execution_date}/{try_number}.log``

In addition, users can supply a remote location to store current logs and backups.

In the Airflow Web UI, remote logs take precedence over local logs when remote logging is enabled. If remote logs
can not be found or accessed, local logs will be displayed. Note that logs
are only sent to remote storage once a task is complete (including failure); In other words, remote logs for
running tasks are unavailable (but local logs are available).

Troubleshooting
---------------

If you want to check which task handler is currently set, you can use ``airflow info`` command as in
the example below.

.. code-block:: bash

    $ airflow info
    ...
    airflow on PATH: [True]

    Executor: [SequentialExecutor]
    Task Logging Handlers: [StackdriverTaskHandler]
    SQL Alchemy Conn: [sqlite://///root/airflow/airflow.db]
    DAGS Folder: [/root/airflow/dags]
    Plugins Folder: [/root/airflow/plugins]
    Base Log Folder: [/root/airflow/logs]

You can also use ``airflow config list`` to check that the logging configuration options have valid values.

.. _write-logs-advanced:

Advanced configuration
----------------------

Not all configuration options are available from the ``airflow.cfg`` file. Some configuration options require
that the logging config class be overwritten. This can be done by ``logging_config_class`` option
in ``airflow.cfg`` file. This option should specify the import path indicating to a configuration compatible with
:func:`logging.config.dictConfig`. If your file is a standard import location, then you should set a :envvar:`PYTHONPATH` environment.

Follow the steps below to enable custom logging config class:

#. Start by setting environment variable to known directory e.g. ``~/airflow/``

    .. code-block:: bash

        export PYTHON_PATH=~/airflow/

#. Create a directory to store the config file e.g. ``~/airflow/config``
#. Create file called ``~/airflow/config/log_config.py`` with following content:

    .. code-block:: python

      form copy import deepcopy
      from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

      LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

#.  At the end of the file, add code to modify the default dictionary configuration.
#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: ini

        [logging]
        remote_logging = True
        logging_config_class = log_config.LOGGING_CONFIG

#. Restart the application.

See :doc:`../modules_management` for details on how Python and Airflow manage modules.

.. _write-logs-amazon-s3:

Writing Logs to Amazon S3
-------------------------

Remote logging to Amazon S3 uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.


Enabling remote logging
'''''''''''''''''''''''

To enable this feature, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3. Users must supply a remote
    # location URL (starting with either 's3://...') and an Airflow connection
    # id that provides access to the storage location.
    remote_logging = True
    remote_base_log_folder = s3://my-bucket/path/to/logs
    remote_log_conn_id = MyS3Conn
    # Use server-side encryption for logs stored in S3
    encrypt_s3_logs = False

In the above example, Airflow will try to use ``S3Hook('MyS3Conn')``.

You can also use `LocalStack <https://localstack.cloud/>`_ to emulate Amazon S3 locally.
To configure it, you must additionally set the endpoint url to point to your local stack.
You can do this via the Connection Extra ``host`` field.
For example, ``{"host": "http://localstack:4572"}``

.. _write-logs-amazon-cloudwatch:

Writing Logs to Amazon Cloudwatch
---------------------------------

Remote logging to Amazon Cloudwatch uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.

To enable this feature, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS Cloudwatch. Users must supply a log group
    # ARN (starting with 'cloudwatch://...') and an Airflow connection
    # id that provides write and read access to the log location.
    remote_logging = True
    remote_base_log_folder = cloudwatch://arn:aws:logs:<region name>:<account id>:log-group:<group name>
    remote_log_conn_id = MyCloudwatchConn

In the above example, Airflow will try to use ``AwsLogsHook('MyCloudwatchConn')``.

.. _write-logs-azure:

Writing Logs to Azure Blob Storage
----------------------------------

Airflow can be configured to read and write task logs in Azure Blob Storage. It uses an existing
Airflow connection to read or write logs. If you don't have a connection properly setup,
this process will fail.

Follow the steps below to enable Azure Blob Storage logging:

#. Airflow's logging system requires a custom ``.py`` file to be located in the :envvar:`PYTHONPATH`, so that it's importable from Airflow. Start by creating a directory to store the config file, ``$AIRFLOW_HOME/config`` is recommended.
#. Create empty files called ``$AIRFLOW_HOME/config/log_config.py`` and ``$AIRFLOW_HOME/config/__init__.py``.
#. Copy the contents of ``airflow/config_templates/airflow_local_settings.py`` into the ``log_config.py`` file created in ``Step 2``.
#. Customize the following portions of the template:

    .. code-block:: ini

        # wasb buckets should start with "wasb" just to help Airflow select correct handler
        REMOTE_BASE_LOG_FOLDER = 'wasb-<whatever you want here>'

        # Rename DEFAULT_LOGGING_CONFIG to LOGGING CONFIG
        LOGGING_CONFIG = ...


#. Make sure a Azure Blob Storage (Wasb) connection hook has been defined in Airflow. The hook should have read and write access to the Azure Blob Storage bucket defined above in ``REMOTE_BASE_LOG_FOLDER``.

#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: ini

        [logging]
        remote_logging = True
        logging_config_class = log_config.LOGGING_CONFIG
        remote_log_conn_id = <name of the Azure Blob Storage connection>

#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks in the bucket you have defined.

.. _write-logs-elasticsearch:

Writing Logs to Elasticsearch
-----------------------------

Airflow can be configured to read task logs from Elasticsearch and optionally write logs to stdout in standard or json format. These logs can later be collected and forwarded to the Elasticsearch cluster using tools like fluentd, logstash or others.

You can choose to have all task logs from workers output to the highest parent level process, instead of the standard file locations. This allows for some additional flexibility in container environments like Kubernetes, where container stdout is already being logged to the host nodes. From there a log shipping tool can be used to forward them along to Elasticsearch. To use this feature, set the ``write_stdout`` option in ``airflow.cfg``.
You can also choose to have the logs output in a JSON format, using the ``json_format`` option. Airflow uses the standard Python logging module and JSON fields are directly extracted from the LogRecord object. To use this feature, set the ``json_fields`` option in ``airflow.cfg``. Add the fields to the comma-delimited string that you want collected for the logs. These fields are from the LogRecord object in the ``logging`` module. `Documentation on different attributes can be found here <https://docs.python.org/3/library/logging.html#logrecord-objects/>`_.

First, to use the handler, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True

    [elasticsearch]
    host = <host>:<port>
    log_id_template = {{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}
    end_of_log_mark = end_of_log
    write_stdout =
    json_fields =

To output task logs to stdout in JSON format, the following config could be used:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True

    [elasticsearch]
    host = <host>:<port>
    log_id_template = {{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}
    end_of_log_mark = end_of_log
    write_stdout = True
    json_format = True
    json_fields = asctime, filename, lineno, levelname, message

.. _write-logs-elasticsearch-tls:

Writing Logs to Elasticsearch over TLS
''''''''''''''''''''''''''''''''''''''

To add custom configurations to ElasticSearch (e.g. turning on ``ssl_verify``, adding a custom self-signed
cert, etc.) use the ``elasticsearch_configs`` setting in your ``airflow.cfg``

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True

    [elasticsearch_configs]
    use_ssl=True
    verify_certs=True
    ca_certs=/path/to/CA_certs


External Links
--------------

When using remote logging, users can configure Airflow to show a link to an external UI within the Airflow Web UI. Clicking the link redirects a user to the external UI.

Some external systems require specific configuration in Airflow for redirection to work but others do not.

.. _log-link-elasticsearch:

Elasticsearch External Link
'''''''''''''''''''''''''''

A user can configure Airflow to show a link to an Elasticsearch log viewing system (e.g. Kibana).

To enable it, ``airflow.cfg`` must be configured as in the example below. Note the required ``{log_id}`` in the URL, when constructing the external link, Airflow replaces this parameter with the same ``log_id_template`` used for writing logs (see `Writing Logs to Elasticsearch`_).

.. code-block:: ini

    [elasticsearch]
    # Qualified URL for an elasticsearch frontend (like Kibana) with a template argument for log_id
    # Code will construct log_id using the log_id template from the argument above.
    # NOTE: The code will prefix the https:// automatically, don't include that here.
    frontend = <host_port>/{log_id}
