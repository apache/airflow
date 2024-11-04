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

.. _cli:

Command Line Interface and Environment Variables Reference
==========================================================

Command Line Interface
''''''''''''''''''''''

Airflow has a very rich command line interface that allows for
many types of operation on a DAG, starting services, and supporting
development and testing.

.. note::
    For more information on usage CLI, see :doc:`/howto/usage-cli`

.. contents:: Content
    :local:
    :depth: 2

Providers that implement executors might contribute additional commands to the CLI. Here are the commands
contributed by the community providers:

* Celery Executor and related CLI commands: :doc:`apache-airflow-providers-celery:cli-ref`
* Kubernetes Executor and related CLI commands: :doc:`apache-airflow-providers-cncf-kubernetes:cli-ref`
* Edge Executor and related CLI commands: :doc:`apache-airflow-providers-edge:cli-ref`
* AWS and related CLI commands: :doc:`apache-airflow-providers-amazon:cli-ref`
* The ``users`` and ``roles`` CLI commands are described in FAB provider documentation :doc:`apache-airflow-providers-fab:cli-ref`

.. argparse::
   :module: airflow.cli.cli_parser
   :func: get_parser
   :prog: airflow

Environment Variables
'''''''''''''''''''''

.. envvar:: AIRFLOW__{SECTION}__{KEY}

  Sets options in the Airflow configuration. This takes priority over the value in the ``airflow.cfg`` file.

  Replace the ``{SECTION}`` placeholder with any section
  and the ``{KEY}`` placeholder with any key in that specified section.

  For example, if you want to set the ``dags_folder`` options in ``[core]`` section,
  then you should set the ``AIRFLOW__CORE__DAGS_FOLDER`` environment variable.

  For more information, see: :doc:`/howto/set-config`.

.. envvar:: AIRFLOW__{SECTION}__{KEY}_CMD

  For any specific key in a section in Airflow, execute the command the key is pointing to.
  The result of the command is used as a value of the ``AIRFLOW__{SECTION}__{KEY}`` environment variable.

  This is only supported by the following config options:

* ``sql_alchemy_conn`` in ``[database]`` section
* ``fernet_key`` in ``[core]`` section
* ``broker_url`` in ``[celery]`` section
* ``flower_basic_auth`` in ``[celery]`` section
* ``result_backend`` in ``[celery]`` section
* ``password`` in ``[atlas]`` section
* ``smtp_password`` in ``[smtp]`` section
* ``secret_key`` in ``[webserver]`` section

.. envvar:: AIRFLOW__{SECTION}__{KEY}_SECRET

  For any specific key in a section in Airflow, retrieve the secret from the configured secrets backend.
  The returned value will be used as the value of the ``AIRFLOW__{SECTION}__{KEY}`` environment variable.

  See :ref:`Secrets Backends<secrets_backend_configuration>` for more information on available secrets backends.

  This form of environment variable configuration is only supported for the same subset of config options as ``AIRFLOW__{SECTION}__{KEY}_CMD``

.. envvar:: AIRFLOW_CONFIG

  The path to the Airflow configuration file.

.. envvar:: AIRFLOW_CONN_{CONN_ID}

  Defines a new connection with the name ``{CONN_ID}`` using the URI value.

  For example, if you want to create a connection named ``PROXY_POSTGRES_TCP``, you can create
  a key ``AIRFLOW_CONN_PROXY_POSTGRES_TCP`` with the connection URI as the value.

  For more information, see: :ref:`environment_variables_connections`.

.. envvar:: AIRFLOW_HOME

  The root directory for the Airflow content.
  This is the default parent directory for Airflow assets such as DAGs and logs.

.. envvar:: AIRFLOW_VAR_{KEY}

  Defines an Airflow variable.
  Replace the ``{KEY}`` placeholder with the variable name.

  For more information, see: :ref:`managing_variables`.
