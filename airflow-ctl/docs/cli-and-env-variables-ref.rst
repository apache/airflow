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

Command Line Interface and Environment Variables Reference
==========================================================

CLI
'''

airflowctl has a very rich command line interface that allows for
many types of operation on a Dag, starting services, and supporting
development and testing.

.. note::
    For more information on usage CLI, see :doc:`cli-and-env-variables-ref`

.. contents:: Content
    :local:
    :depth: 2

.. argparse::
   :module: airflowctl.ctl.cli_parser
   :func: get_parser
   :prog: airflowctl

Environment Variables
'''''''''''''''''''''

.. envvar:: AIRFLOW_CLI_TOKEN

    The token used to authenticate with the Airflow API. This is only
    required if you are using the Airflow API and have not set up
    authentication using a different method. If username and password hasn't been used.

.. envvar:: AIRFLOW_CLI_ENVIRONMENT

    Environment name to use for the CLI. This is used to determine
    which environment to use when running the CLI. This is only
    required if you have multiple environments set up and want to
    specify which one to use. If not set, the default environment
    will be used which is production.

.. envvar:: AIRFLOW_CLI_DEBUG_MODE

    This variable can be used to enable debug mode for the CLI.
    It disables some features such as keyring integration and save credentials to file.
    It is only meant to use if either you are developing airflowctl or running API integration tests.
    Please do not use this variable unless you know what you are doing.
