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
- ``/auth/token/cli``: Create token for command-line clients such as ``airflowctl``, with a ``[config][api_auth]jwt_cli_expiration_time`` expiration time.
  Besides username and password, it supports token refresh and browser login, see :ref:`keycloak-cli-browser-login`.

Both endpoints return ``503`` when Keycloak cannot be reached or answers with a server error. The client can try again
later.


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


.. _keycloak-cli-browser-login:

Command-line clients
''''''''''''''''''''

Command-line clients such as ``airflowctl`` get their token from ``/auth/token/cli``. Besides username and password,
this endpoint offers two more ways to get a token:

- **Refresh**: get fresh Keycloak tokens without logging in again.
- **Browser login**: log in through Keycloak in the browser. It is turned off by default.

Clients find out which of these the server offers by calling ``GET /api/v2/auth/cli/metadata``.

.. note::
    The metadata route exists on Airflow 3.5 and later. On earlier versions, refresh and browser login still work
    when called directly, but clients such as ``airflowctl`` cannot discover them, so they cannot offer browser login.

Refresh
~~~~~~~

A CLI token contains a Keycloak access token, which usually expires after a few minutes. To keep calling the API, the
client exchanges its current CLI token for a new one:

.. code-block:: bash

    curl -X 'POST' \
        "${ENDPOINT_URL}/auth/token/cli" \
        -H 'Content-Type: application/json' \
        -d '{"grant_type": "refresh_token", "token": "<current CLI token>"}'

The new token:

- never expires later than the current one, so refreshing never extends a session.
- lasts at most ``[api_auth] jwt_cli_expiration_time``, so refreshing a longer-lived token from ``/auth/token``
  shortens it.
- can expire up to a minute early, because its lifetime is rounded down to the minute.

On Airflow 3.2 and later, the current token is revoked once the new one is issued. Airflow 3.0 and 3.1 cannot revoke
tokens, so there the current token keeps working until it expires, even after logging out.

A refresh fails with:

- ``403`` when the token is expired or revoked, or the Keycloak session has ended. The user has to log in again.
- ``400`` when Keycloak issued the token without a refresh token, which happens when "Use Refresh Tokens" is turned
  off in the Keycloak client. The user has to log in again.
- ``503`` when Keycloak cannot be reached or answers with a server error. The client can try again later. Before
  Airflow 3.1.7, a server error returns ``403`` instead.

Browser login
~~~~~~~~~~~~~

Browser login uses the OAuth 2.0 authorization code flow with PKCE. To turn it on:

1. Set ``[keycloak_auth_manager] cli_authorization_code_enabled = True``.
2. In the Keycloak client, add ``http://127.0.0.1/callback`` and ``http://[::1]/callback`` to the valid redirect
   URIs. Keycloak ignores the port of loopback redirect URIs, as RFC 8252 requires, so the client can listen on any
   port.
3. Leave PKCE turned off in the Keycloak client. Airflow requires it for command-line clients, and turning it on in
   Keycloak would break the web login, which does not use it.

A login then goes like this:

1. The client listens on ``http://127.0.0.1:<port>/callback`` (or ``[::1]``) and opens ``/auth/login/cli`` in the
   browser, with ``redirect_uri``, ``state``, ``code_challenge`` and ``code_challenge_method=S256``.
2. The user logs in to Keycloak, which redirects the browser to the client's callback with ``code`` and ``state``.
3. The client sends ``grant_type=authorization_code``, ``code``, ``code_verifier`` and ``redirect_uri`` to
   ``/auth/token/cli``, and gets its CLI token.

Airflow refuses any challenge method other than ``S256``, and any ``redirect_uri`` that is not a loopback callback
like the one above.
