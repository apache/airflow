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

Quick Start
-----------

Let's first install ``airflowctl`` if you haven't already:

From PyPI: :doc:`/installation/installing-from-pypi`

From source: :doc:`/installation/installing-from-sources`

airflowctl is a command line tool that helps you manage your Airflow deployments.
It is designed to be easy to use and provides a simple interface for managing your Airflow environment.

To get started, you can use the following command to create a new airflowctl environment:

.. code-block:: bash

  airflowctl auth login --username <username> --password <password> --api-url <api_url> --env <env_name>

To persist the environment, you can set ``AIRFLOW_CLI_ENVIRONMENT``.
The environment variable should be the name of the environment you want to use.
This will allow users to switch environments easily.

OR

.. code-block:: bash

  export AIRFLOW_CLI_TOKEN=<token>
  airflowctl auth login --api-url <api_url> --env <env_name>

OR, when the auth manager of the API server offers browser login (for example to sign in through SSO):

.. code-block:: bash

  airflowctl auth login --sso --api-url <api_url>

This opens the login page of the API server in your browser and waits for the login to complete.
The browser redirects back to airflowctl on ``127.0.0.1``, so it must run on the same machine.
On a remote machine, pass ``--no-browser`` to only print the login URL, and forward the port
of the printed ``redirect_uri`` from your local machine, for example with ``ssh -L``.

Each of these commands creates a new airflowctl environment with the given credentials.
You can then use the following command to start the airflowctl environment:

.. code-block:: bash

  airflowctl --help
