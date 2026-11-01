:tocdepth: 4

..
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
    authentication using a different method, such as a username and password.

.. envvar:: AIRFLOW_CLI_API_URL

    The URL of the Airflow API server, used when ``--api-url`` is not passed and no URL is
    stored in the credentials. Defaults to ``http://localhost:8080``. After ``auth login`` the stored
    URL takes precedence over this variable; re-run ``auth login`` to change it.

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

.. envvar:: AIRFLOW_CLI_API_RETRIES

    The number of times to retry an API call if it fails. This is
    only used if you are using the Airflow API and have not set up
    authentication using a different method. The default value is 3.

    Only read-only calls (``GET``, ``HEAD`` and ``OPTIONS``) are retried after a server error or
    a lost response. Calls that change state, such as ``POST``, ``PUT``, ``PATCH`` and ``DELETE``,
    are retried only when the connection to the server could not be established, so that a
    change the server may already have applied is not sent twice.

.. envvar:: AIRFLOW_CLI_API_RETRY_WAIT_MIN

    The minimum amount of time to wait between API retries in seconds.
    This is only used if you are using the Airflow API and have not set up
    authentication using a different method. The default value is 1 second.

.. envvar:: AIRFLOW_CLI_API_RETRY_WAIT_MAX

    The maximum amount of time to wait between API retries in seconds.
    This is only used if you are using the Airflow API and have not set up
    authentication using a different method. The default value is 10 seconds.

.. envvar:: AIRFLOW_CLI_API_TIMEOUT

    How long, in seconds, to wait for the API server to read or write a request, or for a free
    connection, before the request fails. Connecting is limited to 5 seconds. Read-only requests
    that time out are retried (see :envvar:`AIRFLOW_CLI_API_RETRIES`), so the total wait can be up
    to that many times this value; other requests are not retried. The default value is 30 seconds.

.. note::

    A value for any of the retry variables above or for :envvar:`AIRFLOW_CLI_API_TIMEOUT` that is
    not a whole number, or that is out of range (below 1 for :envvar:`AIRFLOW_CLI_API_RETRIES` and
    :envvar:`AIRFLOW_CLI_API_TIMEOUT`, negative for the two wait variables), is reported on
    standard error and the default is used instead. Setting a variable to an empty string is
    treated the same as leaving it unset.
