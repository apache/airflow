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

Setting Configuration Options
=============================

The first time you run Airflow, it will create a file called ``airflow.cfg`` in
your ``$AIRFLOW_HOME`` directory (``~/airflow`` by default). This file contains Airflow's configuration and you
can edit it to change any of the settings. You can also set options with environment variables by using this format:
``$AIRFLOW__{SECTION}__{KEY}`` (note the double underscores).

For example, the
metadata database connection string can either be set in ``airflow.cfg`` like this:

.. code-block:: bash

    [core]
    sql_alchemy_conn = my_conn_string

or by creating a corresponding environment variable:

.. code-block:: bash

    AIRFLOW__CORE__SQL_ALCHEMY_CONN=my_conn_string

You can also derive the connection string at run time by appending ``_cmd`` to
the key like this:

.. code-block:: bash

    [core]
    sql_alchemy_conn_cmd = bash_command_to_run

The following config options support this ``_cmd`` version:

* ``sql_alchemy_conn`` in ``[core]`` section
* ``fernet_key`` in ``[core]`` section
* ``broker_url`` in ``[celery]`` section
* ``result_backend`` in ``[celery]`` section
* ``password`` in ``[atlas]`` section
* ``smtp_password`` in ``[smtp]`` section
* ``bind_password`` in ``[ldap]`` section
* ``git_password`` in ``[kubernetes]`` section

The idea behind this is to not store passwords on boxes in plain text files.

The order of precedence for all connfig options is as follows -

1. environment variable
2. configuration in airflow.cfg
3. command in airflow.cfg
4. Airflow's built in defaults
