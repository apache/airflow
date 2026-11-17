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

.. _howto/connection:polaris:

Apache Polaris Connection
=========================

The ``polaris`` connection type points at one Polaris deployment (one base URL) and says how to
authenticate to it. :class:`~airflow.providers.apache.polaris.hooks.polaris.PolarisHook` and its
subclasses turn it into authenticated clients of the official ``apache-polaris`` SDK, and can hand
the same settings to pyiceberg, Spark and Trino.

Default Connection IDs
----------------------

Polaris hooks and operators use ``polaris_default`` by default.

How Polaris authenticates
-------------------------

Every Polaris request carries ``Authorization: Bearer <token>`` and, in multi-realm deployments,
a realm header. Each realm is configured server-side with one of three authentication types,
and the connection's **Auth Type** must match how its tokens are obtained:

========================  =============================================  ================================
Server realm type         Who issues tokens                              Connection ``auth_type``
========================  =============================================  ================================
``internal`` (default)    Polaris, from its token endpoint               ``internal``
``external``              An OIDC identity provider (Keycloak, Okta,     ``oidc``
                          Entra ID, ...); Polaris's token endpoint
                          returns HTTP 501
``mixed``                 Either                                         ``internal`` or ``oidc``
any                       A token obtained outside Airflow               ``token``
========================  =============================================  ================================

Configuring the Connection
--------------------------

Polaris URL (host)
    Base URL of Polaris, for example ``https://polaris.example.com``. The scheme defaults to
    ``https``; a path prefix is kept (``https://gateway.example.com/polaris``). Port is optional.

Client ID (login)
    ``internal``: the Polaris principal's client ID. ``oidc``: the client ID registered with the
    identity provider. Unused for ``token``.

Client Secret or Token (password)
    ``internal`` / ``oidc``: the matching client secret. ``token``: the bearer token itself. This
    is the only field that holds a secret.

Extra (optional)
    A JSON dictionary. On Airflow 3.2 and later the connection form shows these as separate
    fields; on Airflow 3.1, enter them as JSON in **Extra**.

    * ``auth_type``: ``internal`` (default), ``oidc`` or ``token``.
    * ``realm``: realm to send in the realm header on every Polaris request, and on ``internal``
      token requests (Polaris tokens are bound to a realm). Leave empty for the server's default
      realm.
    * ``realm_header``: realm header name, default ``Polaris-Realm``. Change it only if the
      server sets ``polaris.realm-context.header-name``.
    * ``scope``:

      * ``internal``: the principal roles the token activates, as space-separated
        ``PRINCIPAL_ROLE:<name>`` entries, default ``PRINCIPAL_ROLE:ALL``. Polaris rejects token
        requests without a valid scope. Prefer naming roles explicitly over ``ALL`` (see
        `Least privilege`_).
      * ``oidc``: sent to the identity provider unchanged, for example
        ``api://polaris/.default`` for Entra ID. With an identity provider, the Polaris roles
        come from claims in the token, not from this scope.

    * ``oauth2_server_uri``: token endpoint. Required for ``oidc`` (the identity provider's
      endpoint). Optional for ``internal``, where it defaults to
      ``<host>/api/catalog/v1/oauth/tokens``; override it behind a proxy or if Polaris moves the
      endpoint.
    * ``audience``, ``resource`` (``oidc`` only): extra token-request parameters some identity
      providers require.
    * ``client_auth_method``: ``client_secret_basic`` (default, HTTP Basic header) or
      ``client_secret_post`` (in the request body). Polaris accepts both; identity providers
      may accept only one.
    * ``default_catalog``: catalog used when an operator or hook call does not name one. Also
      used by **Test Connection**.
    * ``verify``: ``true`` (default), ``false``, or the path to a CA bundle.
    * ``catalog_path``, ``management_path``: API paths under the host, default
      ``/api/catalog`` and ``/api/management/v1``. Change them only for non-standard gateways.
    * ``timeout``: HTTP timeout in seconds for token and API requests, default ``30``.

Token handling
--------------

For ``internal`` and ``oidc`` the hook runs the OAuth2 client-credentials grant, keeps the token
in memory for the lifetime of the hook (one task), refreshes it 60 seconds (at most half its
lifetime) before it expires, and retries a call once with a fresh token if Polaris answers HTTP
401. The lifetime comes from ``expires_in``, else the JWT ``exp`` claim, else 5 minutes. Tokens
are masked in task logs, and the hooks and operators never write them to the database or XCom.
``token`` connections are used as-is and cannot be refreshed.

Test Connection
---------------

A token alone proves little: Polaris only checks the requested principal roles when the token is
used. **Test Connection** therefore gets a token and then makes one API call:

* with ``default_catalog``, it reads that catalog's configuration;
* otherwise it lists catalogs. An HTTP 403 still counts as success: authentication worked, and
  the principal simply lacks the privilege to list catalogs.

Airflow 3 disables the **Test** button by default; enable it with ``[core] test_connection =
Enabled`` (``AIRFLOW__CORE__TEST_CONNECTION=Enabled``).

Troubleshooting
---------------

=================================================  ===================================================
Error                                              What to check
=================================================  ===================================================
Token endpoint ``400 invalid_scope``               ``scope``: ``PRINCIPAL_ROLE:<name>`` entries
Token endpoint ``401 unauthorized_client``         Client ID / secret; a credentials *reset* invalidates
                                                   the old secret immediately (a *rotation* keeps it
                                                   valid until the next rotation)
Token endpoint ``400 invalid_client``              Client ID and secret are both set; for an identity
                                                   provider, ``client_auth_method``
Token endpoint ``501``                             The realm uses external authentication: use ``oidc``
HTTP 404 ``MissingOrInvalidRealm``                 ``realm`` and ``realm_header``
HTTP 401 with a fresh token                        The principal exists in this realm and holds every
                                                   role named in ``scope``
HTTP 403 "must rotate credentials first"           The principal was created with
                                                   ``credentialRotationRequired``; rotate, then update
                                                   the connection
"No catalog given, and connection ... has no       Pass ``catalog``, or set ``default_catalog``
default_catalog"
=================================================  ===================================================

Least privilege
---------------

Create one connection per role set rather than one all-powerful connection: for example a
connection with ``scope`` ``PRINCIPAL_ROLE:catalog_admin`` for provisioning Dags and one with a
read-only role for maintenance Dags. ``PRINCIPAL_ROLE:ALL`` activates every role the principal
holds.

Using the connection with engines
---------------------------------

* :meth:`~airflow.providers.apache.polaris.hooks.polaris.PolarisCatalogHook.get_iceberg_catalog`
  returns a pyiceberg ``RestCatalog`` (``iceberg`` extra) with the same auth, realm and TLS
  settings.
* :meth:`~airflow.providers.apache.polaris.hooks.polaris.PolarisHook.get_iceberg_rest_properties`
  returns Iceberg REST catalog properties for Spark (``engine="spark"``) or Trino
  (``engine="trino"``). They carry a short-lived token, never the client secret. With
  ``internal`` auth the Spark properties include the connection's scope, so Spark's Iceberg
  client keeps the token fresh through Polaris token exchange. Otherwise the properties turn the
  engine's token refresh off, so the job must finish before the token expires: Polaris cannot
  exchange an identity provider's token (``oidc``) or a static one (``token``), and Trino accepts
  a scope only together with a credential, so its properties carry the token alone.
* With ``oidc``, set ``scope`` when the connection also sets ``audience``, ``resource``,
  ``client_secret_post`` or custom TLS verification: pyiceberg then sends the client secret in
  the request body and, without a scope, asks for ``catalog``, which most identity providers
  reject.

.. warning::

    ``get_iceberg_rest_properties()`` contains a bearer token, and
    ``PolarisCatalogHook.get_iceberg_catalog_properties()`` contains the client secret (or the
    token for ``auth_type=token``). Pass them straight to the engine or pyiceberg; do not log
    them or return them from a task, since return values go to XCom.

Extras that change where requests go
------------------------------------

Three extras are under the control of whoever can edit the connection, as for any Airflow
connection: ``verify`` (``false`` disables TLS verification, a path selects a CA bundle on the
worker), ``oauth2_server_uri`` (where the token request goes) and the base URL with
``catalog_path`` / ``management_path``. The token endpoint only ever receives this connection's
own credentials, and a warning is logged when ``verify`` is ``false``. Restrict who can edit
Polaris connections as you would for any connection that holds credentials.

Examples
--------

Internal authentication, JSON form (recommended, since the host is a full URL):

.. code-block:: bash

    export AIRFLOW_CONN_POLARIS_DEFAULT='{
        "conn_type": "polaris",
        "host": "https://polaris.example.com",
        "login": "etl-svc",
        "password": "<client secret>",
        "extra": {"realm": "PROD", "scope": "PRINCIPAL_ROLE:etl_writer", "default_catalog": "analytics"}
    }'

External identity provider:

.. code-block:: bash

    export AIRFLOW_CONN_POLARIS_DEFAULT='{
        "conn_type": "polaris",
        "host": "https://polaris.example.com",
        "login": "<IdP client id>",
        "password": "<IdP client secret>",
        "extra": {
            "auth_type": "oidc",
            "oauth2_server_uri": "https://keycloak.example.com/realms/polaris/protocol/openid-connect/token",
            "realm": "PROD"
        }
    }'

A token obtained outside Airflow:

.. code-block:: bash

    export AIRFLOW_CONN_POLARIS_DEFAULT='{
        "conn_type": "polaris",
        "host": "https://polaris.example.com",
        "password": "<bearer token>",
        "extra": {"auth_type": "token", "realm": "PROD"}
    }'

URI form:

.. code-block:: bash

    export AIRFLOW_CONN_POLARIS_DEFAULT='polaris://etl-svc:<client secret>@polaris.example.com/?realm=PROD&scope=PRINCIPAL_ROLE%3Aetl_writer'

In URI form the host carries no scheme, so the hook uses ``https``; use the JSON form for
``http://``. A path after the host (``polaris://...@gateway.example.com/polaris``) is the path
prefix. URL-encode reserved characters in the client secret (``/``, ``+``, ``@``, ``:``, ...), and
give extras such as ``verify=false`` as query parameters.
