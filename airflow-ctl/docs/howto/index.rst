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



How-to Guides
=============

Setting up the sandbox in the :doc:`/start` section was easy;
building a production-grade environment requires a bit more work!

These how-to guides will step you through common tasks in using and
configuring an Airflow CTL environment.


How to use Airflow CTL
----------------------

**Important Note**
''''''''''''''''''
Airflow CTL needs the Airflow API running to be able to work. Please, see the login section below before use.
Otherwise, you may get errors.

Login
'''''
Airflow CTL needs to be able to connect to the Airflow API. You should pass API URL as a parameter to the command
``--api-url``. The URL should be in the form of ``http(s)://<host>:<port>``.
You can also set the environment variable ``AIRFLOW_CLI_TOKEN`` to the token to use for authentication.

There are two ways to authenticate with the Airflow API:
1. Using a token acquired from the Airflow API

.. code-block:: bash

  airflowctl auth login --api-url <api_url> --api-token <token> --env <env_name:production>

2. Using a username and password


.. code-block:: bash

  airflowctl auth login --api-url <api_url> --username <username> --password <password> --env <env_name:production>

3. (optional) Using a token acquired from the Airflow API and username and password

.. code-block:: bash

  export AIRFLOW_CLI_TOKEN=<token>
  airflowctl auth login --api-url <api_url> --env <env_name>

In both cases token is securely stored in the keyring backend. Only configuration persisted in ``~/.config/airflow`` file
is the API URL and the environment name. The token is stored in the keyring backend and is not persisted in the
configuration file. The keyring backend is used to securely store the token and is not accessible to the user.


For more information use

.. code-block:: bash

  airflowctl auth login --help

.. image:: ../images/output_auth_login.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_auth_login.svg
  :width: 60%
  :alt: Airflow CTL Auth Login Help

You are ready to use Airflow CTL now.
Please, also see :doc:`/cli-and-env-variables-ref` for the list of available commands and options.

You can use the command ``airflowctl --help`` to see the list of available commands.

.. image:: ../images/output_main.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_main.svg
  :width: 60%
  :alt: Airflow CTL Help


All Available Group Command References
--------------------------------------

Below are the command reference diagrams for all available commands in Airflow CTL.
These visual references show the full command syntax, options, and parameters for each command.

**Assets**
''''''''''
.. image:: ../images/output_assets.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_assets.svg
  :width: 60%
  :alt: Airflow CTL Assets Command

**Auth**
''''''''
.. image:: ../images/output_auth.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_auth.svg
  :width: 60%
  :alt: Airflow CTL Auth Command

**Backfills**
'''''''''''''
.. image:: ../images/output_backfills.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_backfills.svg
  :width: 60%
  :alt: Airflow CTL Backfills Command

**Config**
''''''''''
.. image:: ../images/output_config.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_config.svg
  :width: 60%
  :alt: Airflow CTL Config Command

**Connections**
'''''''''''''''
.. image:: ../images/output_connections.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_connections.svg
  :width: 60%
  :alt: Airflow CTL Connections Command

**DAGs**
''''''''
.. image:: ../images/output_dag.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_dag.svg
  :width: 60%
  :alt: Airflow CTL DAG Command

**DAG Runs**
''''''''''''
.. image:: ../images/output_dagrun.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_dagrun.svg
  :width: 60%
  :alt: Airflow CTL DAG Run Command

**Jobs**
''''''''
.. image:: ../images/output_jobs.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_jobs.svg
  :width: 60%
  :alt: Airflow CTL Jobs Command

**Pools**
'''''''''
.. image:: ../images/output_pools.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_pools.svg
  :width: 60%
  :alt: Airflow CTL Pools Command

**Providers**
'''''''''''''
.. image:: ../images/output_providers.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_providers.svg
  :width: 60%
  :alt: Airflow CTL Providers Command

**Variables**
'''''''''''''
.. image:: ../images/output_variables.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_variables.svg
  :width: 60%
  :alt: Airflow CTL Variables Command

**Version**
'''''''''''
.. image:: ../images/output_version.svg
  :target: https://raw.githubusercontent.com/apache/airflow/main/airflow-ctl/docs/images/output_version.svg
  :width: 60%
  :alt: Airflow CTL Version Command
