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



Setting Configuration Options
=============================

The first time you run Airflow, it will create a file called ``airflow.cfg`` in
your ``$AIRFLOW_HOME`` directory (``~/airflow`` by default). This is in order to make it easy to
"play" with airflow configuration.

However, for production case you are advised to generate the configuration using command line:

.. code-block:: bash

    airflow config list --defaults

This command will produce the output that you can copy to your configuration file and edit.

It will contain all the default configuration options, with examples, nicely commented out
so you need only un-comment and modify those that you want to change.
This way you can easily keep track of all the configuration options that you changed from default
and you can also easily upgrade your installation to new versions of Airflow when they come out and
automatically use the defaults for existing options if they changed there.

You can redirect it to your configuration file and edit it:

.. code-block:: bash

    airflow config list --defaults > "${AIRFLOW_HOME}/airflow.cfg"


You can also set options with environment variables by using this format:
:envvar:`AIRFLOW__{SECTION}__{KEY}` (note the double underscores).

For example, the metadata database connection string can either be set in ``airflow.cfg`` like this:

.. code-block:: ini

    [database]
    sql_alchemy_conn = my_conn_string

or by creating a corresponding environment variable:

.. code-block:: bash

    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=my_conn_string

Note that when the section name has a dot in it, you must replace it with an underscore when setting the env var.
For example consider the pretend section ``providers.some_provider``:

.. code-block:: ini

    [providers.some_provider]
    this_param = true

.. code-block:: bash

    export AIRFLOW__PROVIDERS_SOME_PROVIDER__THIS_PARAM=true


You can also derive the connection string at run time by appending ``_cmd`` to
the key like this:

.. code-block:: ini

    [database]
    sql_alchemy_conn_cmd = bash_command_to_run

You can also derive the connection string at run time by appending ``_secret`` to
the key like this:

.. code-block:: ini

    [database]
    sql_alchemy_conn_secret = sql_alchemy_conn
    # You can also add a nested path
    # example:
    # sql_alchemy_conn_secret = database/sql_alchemy_conn

This will retrieve config option from Secret Backends e.g Hashicorp Vault. See
:ref:`Secrets Backends<secrets_backend_configuration>` for more details.

The following config options support this ``_cmd`` and ``_secret`` version:

* ``sql_alchemy_conn`` in ``[database]`` section
* ``fernet_key`` in ``[core]`` section
* ``broker_url`` in ``[celery]`` section
* ``flower_basic_auth`` in ``[celery]`` section
* ``result_backend`` in ``[celery]`` section
* ``password`` in ``[atlas]`` section
* ``smtp_password`` in ``[smtp]`` section
* ``secret_key`` in ``[webserver]`` section

The ``_cmd`` config options can also be set using a corresponding environment variable
the same way the usual config options can. For example:

.. code-block:: bash

    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD=bash_command_to_run

Similarly, ``_secret`` config options can also be set using a corresponding environment variable.
For example:

.. code-block:: bash

    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_SECRET=sql_alchemy_conn

.. note::
    The config options must follow the config prefix naming convention defined within the secrets backend. This means that ``sql_alchemy_conn`` is not defined with a connection prefix, but with config prefix. For example it should be named as ``airflow/config/sql_alchemy_conn``

The idea behind this is to not store passwords on boxes in plain text files.

The universal order of precedence for all configuration options is as follows:

#. set as an environment variable (``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``)
#. set as a command environment variable (``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD``)
#. set as a secret environment variable (``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_SECRET``)
#. set in ``airflow.cfg``
#. command in ``airflow.cfg``
#. secret key in ``airflow.cfg``
#. Airflow's built in defaults

.. note::
    For Airflow versions >= 2.2.1, < 2.3.0 Airflow's built in defaults took precedence
    over command and secret key in ``airflow.cfg`` in some circumstances.

You can check the current configuration with the ``airflow config list`` command.

If you only want to see the value for one option, you can use ``airflow config get-value`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value core executor
    SequentialExecutor

.. note::
    For more information on configuration options, see :doc:`../configurations-ref`

.. note::
    See :doc:`/administration-and-deployment/modules_management` for details on how Python and Airflow manage modules.

.. note::
    Use the same configuration across all the Airflow components. While each component
    does not require all, some configurations need to be same otherwise they would not
    work as expected. A good example for that is :ref:`secret_key<config:webserver__secret_key>` which
    should be same on the Webserver and Worker to allow Webserver to fetch logs from Worker.

    The webserver key is also used to authorize requests to Celery workers when logs are retrieved. The token
    generated using the secret key has a short expiry time though - make sure that time on ALL the machines
    that you run airflow components on is synchronized (for example using ntpd) otherwise you might get
    "forbidden" errors when the logs are accessed.

.. _set-config:configuring-local-settings:

Configuring local settings
==========================

Some Airflow configuration is configured via local setting, because they require changes in the
code that is executed when Airflow is initialized. Usually it is mentioned in the detailed documentation
where you can configure such local settings - This is usually done in the ``airflow_local_settings.py`` file.

You should create an airflow_local_settings.py file and place it in a directory that is part of sys.path, such as the $AIRFLOW_HOME/config folder (which Airflow automatically adds to sys.path during initialization). 
Starting from Airflow 2.10.1, the $AIRFLOW_HOME/dags folder is no longer included in sys.path at initialization, so any local settings in that folder will not be imported. Ensure that airflow_local_settings.py is located in a path that is part of sys.path during initialization, like $AIRFLOW_HOME/config.


You can see the example of such local settings here:

.. py:module:: airflow.config_templates.airflow_local_settings

Example settings you can configure this way:

* :ref:`Cluster Policies <administration-and-deployment:cluster-policies-define>`
* :ref:`Advanced logging configuration <write-logs-advanced>`
* :ref:`Dag serialization <dag-serialization>`
* :ref:`Pod mutation hook in Kubernetes Executor<kubernetes:pod_mutation_hook>`
* :ref:`Control DAG parsing time <faq:how-to-control-dag-file-parsing-timeout>`
* :ref:`Customize your UI <customizing-the-ui>`
* :ref:`Configure more variables to export <export_dynamic_environment_variables>`
* :ref:`Customize your DB configuration <set-up-database-backend>`


Configuring Flask Application for Airflow Webserver
===================================================

Airflow uses Flask to render the web UI. When you initialize the Airflow webserver, predefined configuration
is used, based on the ``webserver`` section of the ``airflow.cfg`` file. You can override these settings
and add any extra settings however by adding flask configuration to ``webserver_config.py`` file in your
``$AIRFLOW_HOME`` directory. This file is automatically loaded by the webserver.

For example if you would like to change rate limit strategy to "moving window", you can set the
``RATELIMIT_STRATEGY`` to ``moving-window``.

You could also enhance / modify the underlying flask app directly,
as the `app context <https://flask.palletsprojects.com/en/2.3.x/appcontext/>`_ is pushed to ``webserver_config.py``:

.. code-block:: python

    from flask import current_app as app


    @app.before_request
    def print_custom_message() -> None:
        print("Executing before every request")
