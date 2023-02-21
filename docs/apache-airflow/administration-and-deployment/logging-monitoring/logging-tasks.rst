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

Airflow writes logs for tasks in a way that allows you to see the logs for each task separately in the Airflow UI.
Core Airflow provides an interface FileTaskHandler, which writes task logs to file, and includes a mechanism to serve them from workers while tasks are running. The Apache Airflow Community also releases providers for many
services (:doc:`apache-airflow-providers:index`) and some of them provide handlers that extend the logging
capability of Apache Airflow. You can see all of these providers in :doc:`apache-airflow-providers:core-extensions/logging`.

Configuring logging
-------------------

For the default handler, FileTaskHandler, you can specify the directory to place log files in ``airflow.cfg`` using
``base_log_folder``. By default, logs are placed in the ``AIRFLOW_HOME``
directory.

.. note::
    For more information on setting the configuration, see :doc:`/howto/set-config`

The default pattern is followed while naming log files for tasks:

- For normal tasks: ``dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log``.
- For dynamically mapped tasks: ``dag_id={dag_id}/run_id={run_id}/task_id={task_id}/map_index={map_index}/attempt={try_number}.log``.

These patterns can be adjusted by :ref:`config:logging__log_filename_template`.

In addition, you can supply a remote location to store current logs and backups.

Interleaving of logs
--------------------

Airflow's remote task logging handlers can broadly be separated into two categories: streaming handlers (such as ElasticSearch, AWS Cloudwatch, and GCP operations logging, formerly stackdriver) and blob storage handlers (e.g. S3, GCS, WASB).

For blob storage handlers, depending on the state of the task, logs could be in a lot of different places and in multiple different files.  For this reason, we need to check all locations and interleave what we find.  To do this we need to be able to parse the timestamp for each line.  If you are using a custom formatter you may need to override the default parser by providing a callable name at Airflow setting ``[logging] interleave_timestamp_parser``.

For streaming handlers, no matter the task phase or location of execution, all log messages can be sent to the logging service with the same identifier so generally speaking there isn't a need to check multiple sources and interleave.

Troubleshooting
---------------

If you want to check which task handler is currently set, you can use the ``airflow info`` command as in
the example below.

.. code-block:: bash

    $ airflow info
    ...
    airflow on PATH: [True]

    Executor: [SequentialExecutor]
    Task Logging Handlers: [StackdriverTaskHandler]
    SQL Alchemy Conn: [sqlite://///root/airflow/airflow.db]
    DAGs Folder: [/root/airflow/dags]
    Plugins Folder: [/root/airflow/plugins]
    Base Log Folder: [/root/airflow/logs]

You can also run ``airflow config list`` to check that the logging configuration options have valid values.

.. _write-logs-advanced:

Advanced configuration
----------------------

Not all configuration options are available from the ``airflow.cfg`` file. Some configuration options require
that the logging config class be overwritten. This can be done via the ``logging_config_class`` option
in ``airflow.cfg`` file. This option should specify the import path to a configuration compatible with
:func:`logging.config.dictConfig`. If your file is a standard import location, then you should set a :envvar:`PYTHONPATH` environment variable.

Follow the steps below to enable custom logging config class:

#. Start by setting environment variable to known directory e.g. ``~/airflow/``

    .. code-block:: bash

        export PYTHONPATH=~/airflow/

#. Create a directory to store the config file e.g. ``~/airflow/config``
#. Create file called ``~/airflow/config/log_config.py`` with following the contents:

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

When using remote logging, you can configure Airflow to show a link to an external UI within the Airflow Web UI. Clicking the link redirects you to the external UI.

Some external systems require specific configuration in Airflow for redirection to work but others do not.

Serving logs from workers and triggerer
---------------------------------------

Most task handlers send logs upon completion of a task. In order to view logs in real time, Airflow starts an HTTP server to serve the logs in the following cases:

- If ``SequentialExecutor`` or ``LocalExecutor`` is used, then when ``airflow scheduler`` is running.
- If ``CeleryExecutor`` is used, then when ``airflow worker`` is running.

In triggerer, logs are served unless the service is started with option ``--skip-serve-logs``.

The server is running on the port specified by ``worker_log_server_port`` option in ``[logging]`` section, and option ``triggerer_log_server_port`` for triggerer.  Defaults are 8793 and 8794, respectively.
Communication between the webserver and the worker is signed with the key specified by ``secret_key`` option  in ``[webserver]`` section. You must ensure that the key matches so that communication can take place without problems.

We are using `Gunicorn <https://gunicorn.org/>`__ as a WSGI server. Its configuration options can be overridden with the ``GUNICORN_CMD_ARGS`` env variable. For details, see `Gunicorn settings <https://docs.gunicorn.org/en/latest/settings.html#settings>`__.

Implementing a custom file task handler
---------------------------------------

.. note:: This is an advanced topic and most users should be able to just use an existing handler from :doc:`apache-airflow-providers:core-extensions/logging`.

In our providers we have a healthy variety of options with all the major cloud providers.  But should you need to implement logging with a different service, and should you then decide to implement a custom FileTaskHandler, there are a few settings to be aware of, particularly in the context of trigger logging.

Triggers require a shift in the way that logging is set up.  In contrast with tasks, many triggers run in the same process, and with triggers, since they run in asyncio, we have to be mindful of not introducing blocking calls through the logging handler.  And because of the variation in handler behavior (some write to file, some upload to blob storage, some send messages over network as they arrive, some do so in thread), we need to have some way to let triggerer know how to use them.

To accomplish this we have a few attributes that may be set on the handler, either the instance or the class.  Inheritance is not respected for these parameters, because subclasses of FileTaskHandler may differ from it in the relevant characteristics.  These params are described below:

- ``trigger_should_wrap``: Controls whether this handler should be wrapped by TriggerHandlerWrapper.  This is necessary when each instance of handler creates a file handler that it writes all messages to.
- ``trigger_should_queue``: Controls whether the triggerer should put a QueueListener between the event loop and the handler, to ensure blocking IO in the handler does not disrupt the event loop.
- ``trigger_send_end_marker``: Controls whether an END signal should be sent to the logger when trigger completes. It is used to tell the wrapper to close and remove the individual file handler specific to the trigger that just completed.
- ``trigger_supported``: If ``trigger_should_wrap`` and ``trigger_should_queue`` are not True, we generally assume that the handler does not support triggers.  But if in this case the handler has ``trigger_supported`` set to True, then we'll still move the handler to root at triggerer start so that it will process trigger messages.  Essentially, this should be true for handlers that "natively" support triggers. One such example of this is the StackdriverTaskHandler.
