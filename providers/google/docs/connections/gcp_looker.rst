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

Google Cloud Platform Looker Connection
=======================================

Communication between Airflow and Looker is done via `Looker API <https://docs.looker.com/reference/api-and-integration/api-reference/v4.0>`_.
To facilitate the API communication Looker operators use `Looker SDK <https://pypi.org/project/looker-sdk/>`_ as an API client.
Before calling API, Looker SDK needs to authenticate itself using your Looker API credentials.

* Obtain your Looker API credentials using instructions in the `Looker API authentication documentation <https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk>`_.

* Obtain your Looker API path and port as described in the `Looker API documentation <https://docs.looker.com/reference/api-and-integration/api-getting-started#looker_api_path_and_port>`_.

* Setup a Looker connection in Airflow.

The ``HTTP`` connection type provides connection to Looker API.

The :class:`~airflow.providers.google.cloud.hooks.looker.LookerHook` uses this connection to run
API requests on a Looker instance issued by :class:`~airflow.providers.google.cloud.operators.looker.LookerStartPdtBuildOperator` and :class:`~airflow.providers.google.cloud.sensors.looker.LookerCheckPdtBuildSensor`.


Configuring the Connection
--------------------------

Host (required)
    Base URL for Looker API. Do not include /api/* in the URL.

Login (required)
    Looker API client id.

Password (required)
    Looker API client secret.

Port (optional)
    Port for Looker API. If hosted on GCP, don't specify the port leaving just the host.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Looker
    connection. The following parameters are supported:

    * ``verify_ssl`` - Set to false only if testing locally against self-signed certs. Defaults to true if not specified.
    * ``timeout`` - Timeout in seconds for HTTP requests. Defaults to 2 minutes (120) seconds if not specified.

    Example "extras" field:

    .. code-block:: json

       {
          "verify_ssl": true,
          "timeout": 120,
       }

Connection URI
--------------

A URL configuration example of a Looker connection:

.. code-block::

    AIRFLOW_CONN_YOUR_CONN_ID='http://YourClientID:YourClientSecret@https%3A%2F%2Fyour.looker.com:19999?verify_ssl=true&timeout=120'
