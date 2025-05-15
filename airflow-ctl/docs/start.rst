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

Airflow CTL is a command line tool that helps you manage your Airflow deployments.
It is designed to be easy to use and provides a simple interface for managing your Airflow environment.

To get started, you can use the following command to create a new Airflow CTL environment:

.. code-block:: bash

  airflowctl auth login --username <username> --password <password> --api-url <api_url> --env <env_name>

OR

.. code-block:: bash

  export AIRFLOW_CLI_TOKEN=<token>
  airflowctl auth login --api-url <api_url> --env <env_name>

This command will create a new Airflow CTL environment with the specified username and password.
You can then use the following command to start the Airflow CTL environment:

.. code-block:: bash

  airflowctl --help
