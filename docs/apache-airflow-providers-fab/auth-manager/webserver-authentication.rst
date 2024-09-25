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

Webserver authentication
========================

By default, Airflow requires users to specify a password prior to login. You can use the
following CLI commands to create an account:

.. code-block:: bash

    # create an admin user
    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

To deactivate the authentication and allow users to be identified as Anonymous, the following entry
in ``$AIRFLOW_HOME/webserver_config.py`` needs to be set with the desired role that the Anonymous
user will have by default:

.. code-block:: ini

    AUTH_ROLE_PUBLIC = 'Admin'

Be sure to checkout :doc:`apache-airflow:security/api` for securing the API.

.. note::

   Airflow uses the config parser of Python. This config parser interpolates
   '%'-signs.  Make sure escape any ``%`` signs in your config file (but not
   environment variables) as ``%%``, otherwise Airflow might leak these
   passwords on a config parser exception to a log.

Password
''''''''

One of the simplest mechanisms for authentication is requiring users to specify a password before logging in.

Please use command line interface ``airflow users create`` to create accounts, or do that in the UI.

Other Methods
'''''''''''''

Since Airflow 2.0, the default UI is the Flask App Builder RBAC. A ``webserver_config.py`` configuration file
is automatically generated and can be used to configure the Airflow to support authentication
methods like OAuth, OpenID, LDAP, REMOTE_USER. It should be noted that due to the limitation of Flask AppBuilder
and Authlib, only a selection of OAuth2 providers is supported. This list includes ``github``, ``githublocal``, ``twitter``,
``linkedin``, ``google``, ``azure``, ``openshift``, ``okta``, ``keycloak`` and ``keycloak_before_17``.

The default authentication option described in the :ref:`Web Authentication <web-authentication>` section is related
with the following entry in the ``$AIRFLOW_HOME/webserver_config.py``.

.. code-block:: ini

    AUTH_TYPE = AUTH_DB

A WSGI middleware could be used to manage very specific forms of authentication
(e.g. `SPNEGO <https://www.ibm.com/docs/en/was-liberty/core?topic=authentication-single-sign-http-requests-using-spnego-web>`_)
and leverage the REMOTE_USER method:

.. code-block:: python

    from typing import Any, Callable

    from flask import current_app
    from flask_appbuilder.const import AUTH_REMOTE_USER


    class CustomMiddleware:
        def __init__(self, wsgi_app: Callable) -> None:
            self.wsgi_app = wsgi_app

        def __call__(self, environ: dict, start_response: Callable) -> Any:
            # Custom authenticating logic here
            # ...
            environ["REMOTE_USER"] = "username"
            return self.wsgi_app(environ, start_response)


    current_app.wsgi_app = CustomMiddleware(current_app.wsgi_app)

    AUTH_TYPE = AUTH_REMOTE_USER

Another way to create users is in the UI login page, allowing user self registration through a "Register" button.
The following entries in the ``$AIRFLOW_HOME/webserver_config.py`` can be edited to make it possible:

.. code-block:: ini

    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Desired Role For The Self Registered User"
    RECAPTCHA_PRIVATE_KEY = 'private_key'
    RECAPTCHA_PUBLIC_KEY = 'public_key'

    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_USE_TLS = True
    MAIL_USERNAME = 'yourappemail@gmail.com'
    MAIL_PASSWORD = 'passwordformail'
    MAIL_DEFAULT_SENDER = 'sender@gmail.com'

The package ``Flask-Mail`` needs to be installed through pip to allow user self registration since it is a
feature provided by the framework Flask-AppBuilder.

To support authentication through a third-party provider, the ``AUTH_TYPE`` entry needs to be updated with the
desired option like OAuth, OpenID, LDAP, and the lines with references for the chosen option need to have
the comments removed and configured in the ``$AIRFLOW_HOME/webserver_config.py``.

For more details, please refer to
`Security section of FAB documentation <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_.

Example using team based Authorization with GitHub OAuth
''''''''''''''''''''''''''''''''''''''''''''''''''''''''
There are a few steps required in order to use team-based authorization with GitHub OAuth.

* configure OAuth through the FAB config in webserver_config.py
* create a custom security manager class and supply it to FAB in webserver_config.py
* map the roles returned by your security manager class to roles that FAB understands.

Here is an example of what you might have in your webserver_config.py:

.. code-block:: python

    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
    from flask_appbuilder.security.manager import AUTH_OAUTH
    import os

    AUTH_TYPE = AUTH_OAUTH
    AUTH_ROLES_SYNC_AT_LOGIN = True  # Checks roles on every login
    AUTH_USER_REGISTRATION = True  # allow users who are not already in the FAB DB to register

    AUTH_ROLES_MAPPING = {
        "Viewer": ["Viewer"],
        "Admin": ["Admin"],
    }
    # If you wish, you can add multiple OAuth providers.
    OAUTH_PROVIDERS = [
        {
            "name": "github",
            "icon": "fa-github",
            "token_key": "access_token",
            "remote_app": {
                "client_id": os.getenv("OAUTH_APP_ID"),
                "client_secret": os.getenv("OAUTH_APP_SECRET"),
                "api_base_url": "https://api.github.com",
                "client_kwargs": {"scope": "read:user, read:org"},
                "access_token_url": "https://github.com/login/oauth/access_token",
                "authorize_url": "https://github.com/login/oauth/authorize",
                "request_token_url": None,
            },
        },
    ]


    class CustomSecurityManager(FabAirflowSecurityManagerOverride):
        pass


    # Make sure to replace this with your own implementation of AirflowSecurityManager class
    SECURITY_MANAGER_CLASS = CustomSecurityManager

Here is an example of defining a custom security manager.
This class must be available in Python's path, and could be defined in
webserver_config.py itself if you wish.

.. code-block:: python

    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
    import logging
    from typing import Any, List, Union
    import os

    log = logging.getLogger(__name__)
    log.setLevel(os.getenv("AIRFLOW__LOGGING__FAB_LOGGING_LEVEL", "INFO"))

    FAB_ADMIN_ROLE = "Admin"
    FAB_VIEWER_ROLE = "Viewer"
    FAB_PUBLIC_ROLE = "Public"  # The "Public" role is given no permissions
    TEAM_ID_A_FROM_GITHUB = 123  # Replace these with real team IDs for your org
    TEAM_ID_B_FROM_GITHUB = 456  # Replace these with real team IDs for your org


    def team_parser(team_payload: dict[str, Any]) -> list[int]:
        # Parse the team payload from GitHub however you want here.
        return [team["id"] for team in team_payload]


    def map_roles(team_list: list[int]) -> list[str]:
        # Associate the team IDs with Roles here.
        # The expected output is a list of roles that FAB will use to Authorize the user.

        team_role_map = {
            TEAM_ID_A_FROM_GITHUB: FAB_ADMIN_ROLE,
            TEAM_ID_B_FROM_GITHUB: FAB_VIEWER_ROLE,
        }
        return list(set(team_role_map.get(team, FAB_PUBLIC_ROLE) for team in team_list))


    class GithubTeamAuthorizer(FabAirflowSecurityManagerOverride):
        # In this example, the oauth provider == 'github'.
        # If you ever want to support other providers, see how it is done here:
        # https://github.com/dpgaspar/Flask-AppBuilder/blob/master/flask_appbuilder/security/manager.py#L550
        def get_oauth_user_info(self, provider: str, resp: Any) -> dict[str, Union[str, list[str]]]:
            # Creates the user info payload from Github.
            # The user previously allowed your app to act on their behalf,
            #   so now we can query the user and teams endpoints for their data.
            # Username and team membership are added to the payload and returned to FAB.

            remote_app = self.appbuilder.sm.oauth_remotes[provider]
            me = remote_app.get("user")
            user_data = me.json()
            team_data = remote_app.get("user/teams")
            teams = team_parser(team_data.json())
            roles = map_roles(teams)
            log.debug(f"User info from Github: {user_data}\nTeam info from Github: {teams}")
            return {"username": "github_" + user_data.get("login"), "role_keys": roles}

Example using team based Authorization with KeyCloak
''''''''''''''''''''''''''''''''''''''''''''''''''''''''
Here is an example of what you might have in your webserver_config.py:

.. code-block:: python

  import os
  import jwt
  import requests
  import logging
  from base64 import b64decode
  from cryptography.hazmat.primitives import serialization
  from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH
  from airflow import configuration as conf
  from airflow.www.security import AirflowSecurityManager

  log = logging.getLogger(__name__)

  AUTH_TYPE = AUTH_OAUTH
  AUTH_USER_REGISTRATION = True
  AUTH_ROLES_SYNC_AT_LOGIN = True
  AUTH_USER_REGISTRATION_ROLE = "Viewer"
  OIDC_ISSUER = "https://sso.keycloak.me/realms/airflow"

  # Make sure you create these role on Keycloak
  AUTH_ROLES_MAPPING = {
      "Viewer": ["Viewer"],
      "Admin": ["Admin"],
      "User": ["User"],
      "Public": ["Public"],
      "Op": ["Op"],
  }

  OAUTH_PROVIDERS = [
      {
          "name": "keycloak",
          "icon": "fa-key",
          "token_key": "access_token",
          "remote_app": {
              "client_id": "airflow",
              "client_secret": "xxx",
              "server_metadata_url": "https://sso.keycloak.me/realms/airflow/.well-known/openid-configuration",
              "api_base_url": "https://sso.keycloak.me/realms/airflow/protocol/openid-connect",
              "client_kwargs": {"scope": "email profile"},
              "access_token_url": "https://sso.keycloak.me/realms/airflow/protocol/openid-connect/token",
              "authorize_url": "https://sso.keycloak.me/realms/airflow/protocol/openid-connect/auth",
              "request_token_url": None,
          },
      }
  ]

  # Fetch public key
  req = requests.get(OIDC_ISSUER)
  key_der_base64 = req.json()["public_key"]
  key_der = b64decode(key_der_base64.encode())
  public_key = serialization.load_der_public_key(key_der)


  class CustomSecurityManager(AirflowSecurityManager):
      def oauth_user_info(self, provider, response):
          if provider == "keycloak":
              token = response["access_token"]
              me = jwt.decode(token, public_key, algorithms=["HS256", "RS256"])

              # Extract roles from resource access
              realm_access = me.get("realm_access", {})
              groups = realm_access.get("roles", [])

              log.info("groups: {0}".format(groups))

              if not groups:
                  groups = ["Viewer"]

              userinfo = {
                  "username": me.get("preferred_username"),
                  "email": me.get("email"),
                  "first_name": me.get("given_name"),
                  "last_name": me.get("family_name"),
                  "role_keys": groups,
              }

              log.info("user info: {0}".format(userinfo))

              return userinfo
          else:
              return {}


  # Make sure to replace this with your own implementation of AirflowSecurityManager class
  SECURITY_MANAGER_CLASS = CustomSecurityManager
