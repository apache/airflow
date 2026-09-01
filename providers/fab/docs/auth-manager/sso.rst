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

================================
Single Sign-On (SSO) Integration
================================

The FAB Auth Manager supports Single Sign-On (SSO) through OAuth2 providers.
This guide shows how to configure SSO with various OAuth2 providers such as
Google, Okta, Azure Entra ID, and others.

This guide shows how to configure SSO with the FAB Auth Manager using a
generic OAuth2 provider. The process is similar for providers such as
Okta, Azure Entra ID, Google, or Auth0.

.. contents:: Table of Contents
   :local:
   :depth: 2

Prerequisites
-------------
- Apache Airflow installed and running with FAB Auth Manager
- Access to an OAuth2 SSO provider (e.g., Google, Okta, Auth0, Azure Entra ID)
- Admin access to Airflow and your SSO provider

.. note::
   For provider-specific authentication setup (obtaining client IDs, secrets, etc.),
   refer to the relevant provider documentation:

   - **Google**: :doc:`apache-airflow-providers-google:api-auth-backend/google-openid` and :doc:`apache-airflow-providers-google:connections/gcp`
   - **Microsoft Azure**: :doc:`apache-airflow-providers-microsoft-azure:connections/azure`
   - **Amazon**: :doc:`apache-airflow-providers-amazon:auth-manager/setup/identity-center`

Configuration Steps
-------------------

1. **Enable the FAB Auth Manager**

   Add the following to your ``airflow.cfg`` (or set as env var):

   .. code-block:: ini

      [core]
      auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

   This replaces the default ``SimpleAuthManager``.

2. **Enable OAuth Authentication Type**

   Set ``AUTH_TYPE`` to ``AUTH_OAUTH`` in your ``webserver_config.py`` file
   (located at ``$AIRFLOW_HOME/webserver_config.py`` by default, configurable via
   ``[fab] config_file`` in ``airflow.cfg``):

   .. code-block:: python

      from flask_appbuilder.const import AUTH_OAUTH

      AUTH_TYPE = AUTH_OAUTH

   .. important::
      This step is required. Without setting ``AUTH_TYPE = AUTH_OAUTH``,
      the OAuth providers will not be activated even if ``OAUTH_PROVIDERS``
      is configured. The default ``AUTH_TYPE = AUTH_DB`` uses database
      authentication only.

   .. note::
      If the ``webserver_config.py`` file does not exist in your environment,
      you need to create it manually. A template with default values and examples
      can be found in the Airflow source at
      ``airflow-core/src/airflow/config_templates/default_webserver_config.py``.
      You can copy this file to ``$AIRFLOW_HOME/webserver_config.py`` and modify
      it for your needs.

3. **Install Required Packages**

   If not already installed, ensure the FAB provider is available:

   .. code-block:: bash

      pip install 'apache-airflow-providers-fab'

   .. note::
      The FAB Auth Manager provider is not installed by default in Airflow 3.
      You must install it explicitly to use OAuth2-based SSO.

4. **Configure OAuth2 Provider**

   Define ``OAUTH_PROVIDERS`` in the same ``webserver_config.py`` file as ``AUTH_TYPE``.
   This is a Flask AppBuilder setting read from that file, not an Airflow configuration
   option, so it cannot be set in ``airflow.cfg`` or through an ``AIRFLOW__FAB__``
   environment variable.

   .. code-block:: python

      OAUTH_PROVIDERS = [
          {
              "name": "generic",
              "icon": "fa-circle",
              "token_key": "access_token",
              "remote_app": {
                  "client_id": "your-client-id",
                  "client_secret": "your-client-secret",
                  "api_base_url": "https://provider.com/oauth/",
                  "request_token_url": None,
                  "access_token_url": "https://provider.com/oauth/token",
                  "authorize_url": "https://provider.com/oauth/authorize",
              },
          }
      ]

   Adjust these values according to your provider's documentation.

5. **Restart Airflow API Server**

   .. code-block:: bash

      airflow api-server

6. **Test SSO Login**

   Open the Airflow UI. You should see a login option for your SSO provider.

Provider Examples
-----------------

**Okta**

.. code-block:: python

   OAUTH_PROVIDERS = [
       {
           "name": "okta",
           "icon": "fa-circle",
           "token_key": "access_token",
           "remote_app": {
               "client_id": "your-client-id",
               "client_secret": "your-client-secret",
               "api_base_url": "https://your-org.okta.com/oauth2/default",
               "request_token_url": None,
               "access_token_url": "https://your-org.okta.com/oauth2/default/v1/token",
               "authorize_url": "https://your-org.okta.com/oauth2/default/v1/authorize",
           },
       }
   ]

.. seealso::
   For detailed Okta setup instructions, see the `Okta OAuth2 documentation <https://developer.okta.com/docs/guides/implement-oauth/>`_.

**Azure Entra ID (Azure AD)**

.. code-block:: python

   OAUTH_PROVIDERS = [
       {
           "name": "azure",
           "icon": "fa-circle",
           "token_key": "access_token",
           "remote_app": {
               "client_id": "your-client-id",
               "client_secret": "your-client-secret",
               "api_base_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/",
               "request_token_url": None,
               "access_token_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token",
               "authorize_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/authorize",
               "client_kwargs": {"scope": "openid email profile"},
           },
       }
   ]

.. note::
   The ``<tenant-id>`` in the Azure endpoints can be specified as a tenant GUID
   (e.g., ``72f988bf-86f1-41af-91ab-2d7cd011db47``) or as a tenant domain (e.g.,
   ``contoso.onmicrosoft.com`` or ``example.com``). Alternatively, you can specify
   the tenant explicitly by setting ``tenant_id`` inside ``client_kwargs``. Tenant-agnostic
   authorities (``common``, ``organizations``, ``consumers``) are not accepted; configure a
   specific tenant GUID or domain instead.

.. note::
   National clouds are supported by pointing the endpoints at their authority host:
   ``login.microsoftonline.us`` (Azure Government) or ``login.partner.microsoftonline.cn``
   (Azure operated by 21Vianet). The issuer and the signing key set are then read from that
   tenant's own OpenID metadata, so no extra configuration is needed. Azure AD B2C
   (``<tenant>.b2clogin.com``) is not supported, because its metadata is addressed by policy
   rather than by tenant alone.

   Configuring a tenant domain, or any national-cloud tenant, makes an outbound HTTPS request
   to the authority's OpenID discovery endpoint the first time a user logs in. The result is
   cached for the lifetime of the process. Deployments with restricted egress must allow that
   host.

.. seealso::
   For Azure app registration and OAuth setup, see :doc:`apache-airflow-providers-microsoft-azure:connections/azure`
   and the `Azure OAuth2 documentation <https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow>`_.

**Azure AD with Group-Based Authorization**

.. code-block:: python

   from flask_appbuilder.security.manager import AUTH_OAUTH

   AUTH_TYPE = AUTH_OAUTH

   AUTH_OAUTH_ROLE_KEYS = {
       "azure": "groups",
   }

   OAUTH_PROVIDERS = [
       {
           "name": "azure",
           "token_key": "access_token",
           "icon": "fa-windows",
           "remote_app": {
               "client_id": "your-client-id",
               "client_secret": "your-client-secret",
               "api_base_url": "https://login.microsoftonline.com/<tenant-id>/v2.0",
               "client_kwargs": {
                   "scope": "openid email profile groups",
                   "resource": "your-client-id",
                   "verify_signature": True,
               },
               "request_token_url": None,
               "access_token_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token",
               "authorize_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/authorize",
           },
       }
   ]

   AUTH_ROLES_MAPPING = {
       "airflow-admin-group": ["Admin"],
       "airflow-op-group": ["Op"],
       "airflow-user-group": ["User"],
       "airflow-viewer-group": ["Viewer"],
   }

   AUTH_ROLES_SYNC_AT_LOGIN = True

   AUTH_USER_REGISTRATION = True
   AUTH_USER_REGISTRATION_ROLE = "Viewer"

.. note::
   When using Azure AD groups:

   - Ensure the ``groups`` scope is included in ``client_kwargs``
   - Configure group claims in your Azure app registration
   - The ``AUTH_OAUTH_ROLE_KEYS`` setting allows you to specify which claim field
     contains the authorization information (``roles`` or ``groups``)
   - Group names from Azure AD will be matched against ``AUTH_ROLES_MAPPING``

.. important::
   The ``AUTH_OAUTH_ROLE_KEYS`` configuration is provider-specific. For Azure,
   you can set it to ``"roles"`` (default) or ``"groups"`` depending on your
   Azure AD setup. Other OAuth providers may use different field names.

**Google OAuth2**

.. code-block:: python

   OAUTH_PROVIDERS = [
       {
           "name": "google",
           "icon": "fa-google",
           "token_key": "access_token",
           "remote_app": {
               "client_id": "your-client-id.googleusercontent.com",
               "client_secret": "your-client-secret",
               "api_base_url": "https://www.googleapis.com/oauth2/v2/",
               "request_token_url": None,
               "access_token_url": "https://oauth2.googleapis.com/token",
               "authorize_url": "https://accounts.google.com/o/oauth2/auth",
               "client_kwargs": {"scope": "openid email profile"},
           },
       }
   ]

.. seealso::
   For Google OAuth setup and credential configuration, see :doc:`apache-airflow-providers-google:connections/gcp`
   and :doc:`apache-airflow-providers-google:api-auth-backend/google-openid`.

Troubleshooting
---------------

**Common Issues**

- **Authentication fails after configuration**:

  - Check Airflow and webserver logs for detailed error messages
  - Ensure all environment variables are set and exported correctly
  - Verify callback URLs in your SSO provider match your Airflow webserver URL (typically ``http://your-airflow-domain/auth/oauth-authorized/<provider>``)

- **Redirect URI mismatch**:

  - In your OAuth provider, set the redirect URI to: ``http://your-airflow-domain/auth/oauth-authorized/<provider>``,
    where ``<provider>`` is the ``name`` you gave it in ``OAUTH_PROVIDERS``
  - For development, this might be: ``http://localhost:8080/auth/oauth-authorized/google``
  - On Airflow 2 the path had no ``/auth`` prefix: ``http://your-airflow-domain/oauth-authorized/<provider>``

- **Scope-related errors**:

  - Confirm that scopes (``openid email profile`` or similar) are allowed in your OAuth provider
  - Some providers require specific scopes to be explicitly configured

- **Token validation errors**:

  - Ensure your OAuth provider's clock is synchronized
  - Check if your client secret matches exactly (no extra spaces/characters)

- **User creation issues**:

  - FAB Auth Manager creates users automatically on first login
  - Check if your OAuth provider returns the expected user information fields

References
----------
- `Airflow Authentication <https://airflow.apache.org/docs/apache-airflow/stable/security/authentication.html>`_
- `FAB Auth Manager Provider Docs <https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth_manager.html>`_
- `Flask AppBuilder Security <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
- `Okta OAuth2 Docs <https://developer.okta.com/docs/guides/implement-oauth/>`_
- `Azure OAuth2 Docs <https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow>`_

.. note::
   This example uses the **Flask AppBuilder Auth Manager**.
   If you use a different authentication manager, configuration may differ.
