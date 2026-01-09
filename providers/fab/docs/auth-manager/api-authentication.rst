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

FAB auth manager API authentication
===================================

.. note::
    This guide only applies to :doc:`FAB auth manager API </api-ref/fab-public-api-ref>`.

Authentication for the APIs is handled by what is called an authentication backend. The default is to check the user session:

.. code-block:: ini

    [fab]
    auth_backends = airflow.providers.fab.auth_manager.api.auth.backend.session

If you want to check which authentication backends are currently set, you can use ``airflow config get-value fab auth_backends``
command as in the example below.

.. code-block:: console

    $ airflow config get-value fab auth_backends
    airflow.providers.fab.auth_manager.api.auth.backend.basic_auth

.. versionchanged:: 3.0.0

    Airflow now uses token-based authentication for the public API.
    This mechanism is independent of the configured ``auth_backend``.
    Clients must first obtain a JWT token using :doc:`token`, then include that token in subsequent API requests.

Kerberos authentication
'''''''''''''''''''''''

Kerberos authentication is currently supported for the API, both experimental and stable.

To enable Kerberos authentication, set the following in the configuration:

.. code-block:: ini

    [fab]
    auth_backends = airflow.providers.fab.auth_manager.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Airflow Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists `in both the Kerberos database as well as in the keytab file </docs/apache-airflow/stable/security/kerberos.html#enabling-kerberos>`_.

You have to make sure to name your users with the kerberos full username/realm in order to make it
work. This means that your user name should be ``user_name@REALM``.

.. code-block:: bash

    kinit user_name@REALM
    ENDPOINT_URL="http://localhost:8080"
    curl -X GET  \
        --negotiate \  # enables Negotiate (SPNEGO) authentication
        --service airflow \  # matches the `airflow` service name in the `airflow/fully.qualified.domainname@REALM` principal
        --user : \
        "${ENDPOINT_URL}/api/v1/pools"


.. note::

    Remember that the APIs are secured by both authentication and `access control <./access-control.html>`_.
    This means that your user needs to have a Role with necessary associated permissions, otherwise you'll receive
    a 403 response.


Basic authentication
''''''''''''''''''''

`Basic username password authentication <https://en.wikipedia.org/wiki/Basic_access_authentication>`_ is currently
supported for the API. This works for users created through LDAP login or
within Airflow Metadata DB using password.

To enable basic authentication, set the following in the configuration:

.. code-block:: ini

    [fab]
    auth_backends = airflow.providers.fab.auth_manager.api.auth.backend.basic_auth

Username and password needs to be base64 encoded and send through the
``Authorization`` HTTP header in the following format:

.. code-block:: text

    Authorization: Basic Base64(username:password)

Here is a sample curl command you can use to validate the setup:

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080"
    curl -X GET  \
        --user "username:password" \
        "${ENDPOINT_URL}/api/v1/pools"

Note, you can still enable this setting to allow API access through username
password credential even though Airflow webserver might be using another
authentication method. Under this setup, only users created through LDAP or
``airflow users create`` command will be able to pass the API authentication.

Roll your own API authentication
''''''''''''''''''''''''''''''''

Each auth backend is defined as a new Python module. It must have 2 defined methods:

* ``init_app(app: Flask)`` - function invoked when creating a flask application, which allows you to add a new view.
* ``requires_authentication(fn: Callable)`` - a decorator that allows arbitrary code execution before and after or instead of a view function.

and may have one of the following to support API client authorizations used by :ref:`remote mode for CLI <cli-remote>`:

* function ``create_client_session() -> requests.Session``
* attribute ``CLIENT_AUTH: tuple[str, str] | requests.auth.AuthBase | None``

After writing your backend module, provide the fully qualified module name in the ``auth_backends`` key in the ``[fab]``
section of ``airflow.cfg``.

Additional options to your auth backend can be configured in ``airflow.cfg``, as a new option.

Example using Keycloak authentication
'''''''''''''''''''''''''''''''''''''

The following example add also ``PKCE`` authentication flow.

You must configure Keycloak accordingly, create ``clientID`` in Keycloak:

.. code-block:: text

    Client ID:                       <airflow-client-id> # Your choice
    Root URL:                        https://<your-airflow-url.fr>
    Home URL:
    Valid redirect URIs:             https://<your-airflow-url.fr>/auth/oauth-authorized/keycloak # with webserver (airflow <3.0) it was https://<your-airflow-url.fr>/oauth-authorized/keycloak

    Valid post logout redirect URIs: https://<your-airflow-url.fr>

    Web origins: +
    Admin URL: https://<your-airflow-url.fr>

In Capability config, you must select:

.. code-block:: text

    Client authentication   On
    Authorization           Off
    Authentication flow     [x] Standard flow                        [x] Direct access grants
                            [ ] Implicit flow                        [ ] Service accounts roles
                            [ ] OAuth 2.0 Device Authorization Grant
                            [ ] OIDC CIBA Grant

In Roles tab, you must create the following roles:

.. code-block:: text

    airflow_admin
    airflow_op
    airflow_public
    airflow_user
    airflow_viewer

In Advanced tab, section Advanced settings (needed for PKCE flow):

.. code-block:: text

    Proof Key for Code Exchange Code Challenge Method: S256

You must create the following environment variables in your Airflow deployment:

.. code-block:: bash

    export CLIENT_ID=<airflow-client-id>
    export CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    export OIDC_ISSUER=https://<your-keycloak-url.fr>/auth/realms/<REALM>
    export AIRFLOW__API__BASE_URL=https://<your-airflow-url.fr>

or create a Secret with these values for your Helm chart:

.. code-block:: bash

    kubectl -n airflow create secret generic airflow-api-keycloak \
      --from-literal=CLIENT_ID=<airflow-client-id> \
      --from-literal=CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx \
      --from-literal=OIDC_ISSUER=https://<your-keycloak-url.fr>/auth/realms/<REALM> \
      --from-literal=AIRFLOW__API__BASE_URL=https://<your-airflow-url.fr>

and configure your Helm chart to use this Secret:

.. code-block:: yaml

    apiServer:
      env:
        - name: CLIENT_ID
          valueFrom:
            secretKeyRef:
              name: airflow-api-keycloak
              key: CLIENT_ID
        - name: CLIENT_SECRET
          valueFrom:
            secretKeyRef:
              name: airflow-api-keycloak
              key: CLIENT_SECRET
        - name: OIDC_ISSUER
          valueFrom:
            secretKeyRef:
              name: airflow-api-keycloak
              key: OIDC_ISSUER
        - name: AIRFLOW__API__BASE_URL
          valueFrom:
            secretKeyRef:
              name: airflow-api-keycloak
              key: AIRFLOW__API__BASE_URL

Here is an example of what you might have in your ``webserver_config.py`` or ``apiServerConfig`` value in Helm chart:

.. code-block:: python

    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
    from base64 import b64decode
    from cryptography.hazmat.primitives import serialization
    from flask import redirect, session
    from flask_appbuilder import expose
    from flask_appbuilder.security.manager import AUTH_OAUTH
    from flask_appbuilder.security.views import AuthOAuthView
    import jwt
    import logging
    import os
    import requests

    log = logging.getLogger(__name__)
    CSRF_ENABLED = True
    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_USER_REGISTRATION_ROLE = "Public"
    PERMANENT_SESSION_LIFETIME = 43200

    # Make sure you create these roles on Keycloak
    AUTH_ROLES_MAPPING = {
        "airflow_admin": ["Admin"],
        "airflow_op": ["Op"],
        "airflow_public": ["Public"],
        "airflow_user": ["User"],
        "airflow_viewer": ["Viewer"],
    }
    PROVIDER_NAME = "keycloak"
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    AIRFLOW__API__BASE_URL = os.getenv("AIRFLOW__API__BASE_URL")
    OIDC_ISSUER = os.getenv("OIDC_ISSUER")
    OIDC_BASE_URL = f"{OIDC_ISSUER}/protocol/openid-connect"
    OIDC_TOKEN_URL = f"{OIDC_BASE_URL}/token"
    OIDC_AUTH_URL = f"{OIDC_BASE_URL}/auth"
    OIDC_METADATA_URL = f"{OIDC_ISSUER}/.well-known/openid-configuration"
    OAUTH_PROVIDERS = [
        {
            "name": PROVIDER_NAME,
            "token_key": "access_token",
            "icon": "fa-key",
            "remote_app": {
                "api_base_url": OIDC_BASE_URL,
                "access_token_url": OIDC_TOKEN_URL,
                "authorize_url": OIDC_AUTH_URL,
                "server_metadata_url": OIDC_METADATA_URL,
                "request_token_url": None,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "client_kwargs": {
                    "scope": "email profile",
                    "code_challenge_method": "S256",  # Needed for PKCE flow
                    "response_type": "code",  # Needed for PKCE flow
                },
            },
        }
    ]

    # Fetch public key
    req = requests.get(OIDC_ISSUER)
    key_der_base64 = req.json()["public_key"]
    key_der = b64decode(key_der_base64.encode())
    public_key = serialization.load_der_public_key(key_der)


    class CustomOAuthView(AuthOAuthView):
        @expose("/logout/", methods=["GET", "POST"])
        def logout(self):
            session.clear()
            return redirect(
                f"{OIDC_ISSUER}/protocol/openid-connect/logout?post_logout_redirect_uri={AIRFLOW__API__BASE_URL}&client_id={CLIENT_ID}"
            )


    class CustomSecurityManager(FabAirflowSecurityManagerOverride):
        authoauthview = CustomOAuthView

        def get_oauth_user_info(self, provider, response):
            if provider == "keycloak":
                token = response["access_token"]
                me = jwt.decode(token, public_key, algorithms=["HS256", "RS256"], audience=CLIENT_ID)

                # Extract roles from resource access
                groups = me.get("resource_access", {}).get(CLIENT_ID, {}).get("roles", [])

                log.info(f"groups: {groups}")

                if not groups:
                    groups = ["Viewer"]

                userinfo = {
                    "username": me.get("preferred_username"),
                    "email": me.get("email"),
                    "first_name": me.get("given_name"),
                    "last_name": me.get("family_name"),
                    "role_keys": groups,
                }

                log.info(f"user info: {userinfo}")

                return userinfo
            else:
                return {}


    # Make sure to replace this with your own implementation of AirflowSecurityManager class
    SECURITY_MANAGER_CLASS = CustomSecurityManager
