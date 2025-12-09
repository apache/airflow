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

Generate JWT token with FAB auth manager
========================================

.. note::
    This guide only applies if your environment is configured with FAB auth manager.

To use the :doc:`Airflow public API <apache-airflow:stable-rest-api-ref>`, you first need to obtain a JWT Token for
authentication.
Once you have the token, include it in the ``Authorization`` header when making requests to the public API.

You can generate a JWT token using the ``Create Token`` API endpoint,
documented in :doc:`/api-ref/fab-token-api-ref`.

Example
'''''''

Use the following example to generate a token via username and password.

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080"
    curl -X 'POST' \
        "${ENDPOINT_URL}/auth/token" \
        -H 'Content-Type: application/json' \
        -d '{
        "username": "<username>",
        "password": "<password>"
        }'

If successful, this request returns a JWT token that you can use for subsequent Airflow public API calls.

Only users authenticated via the database (``AUTH_TYPE = AUTH_DB``) or LDAP
(``AUTH_TYPE = AUTH_LDAP``) can generate tokens using this method.
For more details, see :doc:`webserver-authentication`.

If you need to generate a token using a different authentication mechanism, see the next section.

Custom authentication implementation
------------------------------------

By default, JWT tokens for the Airflow public API can only be generated using
basic authentication (username and password) for database or LDAP users.

If you want to support another authentication mechanism, such as oauth, you can do so by overriding the
``create_token`` method in the FAB auth manager.

Example
'''''''

.. code-block:: python

    class MyAuthManager(FabAuthManager):

        def create_token(self, headers: dict[str, str], body: dict[str, Any]) -> User:
            """
            Return the authenticated user for a given payload.

            Implement your own custom token creation logic here.
            """
            ...

Oauth example
'''''''''''''

Below is an example implementation that uses OAuth to allow users to obtain a JWT token.
This custom logic overrides the default ``create_token`` method from the FAB authentication manager.

.. warning::
    The example shown below disables signature verification (``verify_signature=False``).
    This is **insecure** and should only be used for testing. Always validate tokens properly in production.

.. code-block:: python

    class MyAuthManager(FabAuthManager):

        def create_token(self, headers: dict[str, str], body: dict[str, Any]) -> User:
            """
            Return the authenticated user derived from an OAuth access token.

            Implement your own custom token validation and user mapping logic here.
            """
            user = None

            # Handle OAuth-based authentication
            if self.security_manager.auth_type == AUTH_OAUTH:
                # Require a Bearer token
                auth_header = headers.get("Authorization")
                if not auth_header:
                    return None

                token = auth_header.replace("Bearer ", "")

                # Example token decoding
                #
                # With signature validation (recommended):
                # me = jwt.decode(
                #     token,
                #     public_key,
                #     algorithms=['HS256', 'RS256'],
                #     audience=CLIENT_ID
                # )
                #
                # Without signature validation (not recommended):
                me = jwt.decode(token, options={"verify_signature": False})

                # Extract groups/roles (example schema — adjust to your provider)
                groups = me["resource_access"]["airflow"]["roles"]  # requires validation
                if not groups:
                    groups = ["airflow_public"]
                else:
                    groups = [g for g in groups if "airflow" in g]

                # Build user info payload for FAB
                userinfo = {
                    "username": me.get("preferred_username"),
                    "email": me.get("email"),
                    "first_name": me.get("given_name"),
                    "last_name": me.get("family_name"),
                    "role_keys": groups,
                }

                user = self.security_manager.auth_user_oauth(userinfo)

            # Fall back to the default implementation
            else:
                user = super().create_token(headers=headers, body=body)

            log.info("User: %s", user)

            # Log user into the session
            if user is not None:
                login_user(user, remember=False)

            return user
