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

      from copy import deepcopy
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

External Links
--------------

When using remote logging, users can configure Airflow to show a link to an external UI within the Airflow Web UI. Clicking the link redirects a user to the external UI.

Some external systems require specific configuration in Airflow for redirection to work but others do not.
