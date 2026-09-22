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

- ``/auth/token``: Create token using username and password, client credentials, or a Keycloak-issued JWT assertion, with a ``[config][api_auth]jwt_expiration_time`` expiration time.
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

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080"
    curl -X 'POST' \
        "${ENDPOINT_URL}/auth/token" \
        -H 'Content-Type: application/json' \
        -d '{
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": "<keycloak_access_token>"
        }'

If a client already authenticated to Keycloak by some other means -- for example, a
"Signed JWT - Federated" client bound to an external OIDC identity provider such as a
Kubernetes ServiceAccount issuer or AWS IAM outbound identity federation -- it can
exchange the resulting Keycloak access token for an Airflow token without
re-authenticating to Keycloak through Airflow. Airflow does not obtain the assertion on
the client's behalf, it verifies the assertion and calls Keycloak's ``/userinfo``
endpoint to validate it and retrieve user information.

The assertion must be a valid, unexpired access token issued by this realm, and its
``aud`` claim must include Airflow's configured client id. The calling client (from the
token's ``azp`` claim) must also appear in the
``[keycloak_auth_manager]jwt_federated_client_ids`` allow-list -- an ``aud`` match alone
is not sufficient, since it only proves the token was meant for Airflow, not that the
issuing client has been vetted for machine authentication. An unset or empty allow-list
denies every caller.

If the assertion fails JWT validation, is not allow-listed, or Keycloak rejects it at
``/userinfo``, the endpoint returns ``403 Invalid Keycloak assertion``. This generic
response does not reveal which assertion validation check failed.

**Keycloak client requirements:** The federated client that obtains the assertion (not
the ``airflow`` client itself) must
be configured with:

- **Client authentication**: ON (confidential client), using whatever mechanism suits
  the caller -- a static client secret, or a "Signed JWT - Federated" authenticator bound
  to an external OIDC identity provider (e.g. a Kubernetes ServiceAccount issuer or AWS
  IAM outbound identity federation) for machine auth with no static secret.
- **Service accounts roles**: ON, so the client can obtain its own access token via the
  ``client_credentials`` grant.
- **Default client scopes** must include ``openid``. Keycloak's ``/userinfo`` endpoint
  rejects tokens whose ``scope`` claim omits ``openid`` with a bare ``403``, and Airflow
  calls ``/userinfo`` to build the resulting user -- a token issued with only, say,
  ``profile email`` in its ``scope`` claim will fail here even though the JWT itself is
  perfectly valid. This is easy to miss since it is a realm-wide default that some
  client scope configurations exclude for service accounts.
- Realm/client roles appropriate for whatever Airflow permissions the client needs
  (e.g. ``SuperAdmin``), assigned the same way as any other service-account client.

On the Airflow side, the client's id (from its token's ``azp`` claim) must be added to
``[keycloak_auth_manager]jwt_federated_client_ids``.

The response contains an Airflow-minted JWT, not the Keycloak assertion itself. Exchange
the assertion for it first, then use that JWT -- not ``$KEYCLOAK_TOKEN`` -- as the Bearer
token for subsequent public API calls:

.. code-block:: bash

    ENDPOINT_URL="http://airflow-api-server:8080"

    AIRFLOW_TOKEN=$(curl -s -X 'POST' \
        "${ENDPOINT_URL}/auth/token" \
        -H 'Content-Type: application/json' \
        -d "{\"grant_type\": \"urn:ietf:params:oauth:grant-type:jwt-bearer\", \"assertion\": \"${KEYCLOAK_TOKEN}\"}" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')

    curl -s "${ENDPOINT_URL}/api/v2/dags" \
        -H "Authorization: Bearer ${AIRFLOW_TOKEN}"
