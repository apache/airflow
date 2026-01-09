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

Generate JWT token with Keycloak auth manager
=============================================

.. note::
    This guide only applies if your environment is configured with Keycloak auth manager.

In order to use the :doc:`Airflow public API <apache-airflow:stable-rest-api-ref>`, you need a JWT token for authentication.
You can then include this token in your Airflow public API requests.
To generate a JWT token, use the ``Create Token`` API in :doc:`/api-ref/token-api-ref`.

Several endpoints exist to create tokens depending on the authentication method you want to use.

If a user or service needs to interact with the Airflow public API, they can create a token using their credentials.

- ``/auth/token``: Create token using username and password or client credentials with a ``[config][api_auth]jwt_expiration_time`` expiration time.
- ``/auth/token/cli``: Create token for Airflow CLI using username and password with a ``[config][api_auth]jwt_cli_expiration_time`` expiration time.


Example
'''''''

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080"
    curl -X 'POST' \
        "${ENDPOINT_URL}/auth/token" \
        -H 'Content-Type: application/json' \
        -d '{
        "username": "<username>",
        "password": "<password>"
        }'

This process will return a token that you can use in the Airflow public API requests.
The body can also contain a ``grant_type`` field with value ``password`` but it is optional since it is the default value.

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080 "
    curl -X 'POST' \
        "${ENDPOINT_URL}/auth/token" \
        -H 'Content-Type: application/json' \
        -d '{
        "grant_type": "client_credentials",
        "client_id": "<client_id>",
        "client_secret": "<client_secret>"
        }'

If other services need to interact with the Airflow public API, they can create a token using the client credentials grant flow.
The client must live in the same realm the Auth Manager is configured to use. Its service account must have the appropriate roles / permissions to access the Airflow public API.
This process will return a token obtained using client credentials grant flow.
