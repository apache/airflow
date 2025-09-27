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

      [webserver]
      auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

   This replaces the default ``SimpleAuthManager``.

2. **Install Required Packages**

   If not already installed, ensure the FAB provider is available:

   .. code-block:: bash

      pip install 'apache-airflow-providers-fab'

   .. note::
      The FAB Auth Manager provider is not installed by default in Airflow 3.
      You must install it explicitly to use OAuth2-based SSO.

3. **Configure OAuth2 Provider**

   FAB Auth Manager reads provider configuration from the ``[fab]`` section
   of ``airflow.cfg`` or from environment variables.

   **Option A: Environment Variables (Recommended)**

   .. code-block:: bash

      export AIRFLOW__FAB__OAUTH_PROVIDERS='[{
         "name": "generic",
         "icon": "fa-circle",
         "token_key": "access_token",
         "remote_app": {
           "client_id": "your-client-id",
           "client_secret": "your-client-secret",
           "api_base_url": "https://provider.com/oauth/",
           "request_token_url": null,
           "access_token_url": "https://provider.com/oauth/token",
           "authorize_url": "https://provider.com/oauth/authorize"
         }
      }]'

   **Option B: Configuration File**

   Add to your ``airflow.cfg``:

   .. code-block:: ini

      [fab]
      oauth_providers = [
        {
          "name": "generic",
          "icon": "fa-circle",
          "token_key": "access_token",
          "remote_app": {
            "client_id": "your-client-id",
            "client_secret": "your-client-secret",
            "api_base_url": "https://provider.com/oauth/",
            "request_token_url": null,
            "access_token_url": "https://provider.com/oauth/token",
            "authorize_url": "https://provider.com/oauth/authorize"
          }
        }
      ]

   Adjust these values according to your provider's documentation.

4. **Restart Airflow Webserver**

   .. code-block:: bash

      airflow webserver --reload

5. **Test SSO Login**

   Open the Airflow UI. You should see a login option for your SSO provider.

Provider Examples
-----------------

**Okta**

.. code-block:: bash

   export AIRFLOW__FAB__OAUTH_PROVIDERS='[{
      "name": "okta",
      "icon": "fa-circle",
      "token_key": "access_token",
      "remote_app": {
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "api_base_url": "https://your-org.okta.com/oauth2/default",
        "request_token_url": null,
        "access_token_url": "https://your-org.okta.com/oauth2/default/v1/token",
        "authorize_url": "https://your-org.okta.com/oauth2/default/v1/authorize"
      }
   }]'

.. seealso::
   For detailed Okta setup instructions, see the `Okta OAuth2 documentation <https://developer.okta.com/docs/guides/implement-oauth/>`_.

**Azure Entra ID (Azure AD)**

.. code-block:: bash

   export AIRFLOW__FAB__OAUTH_PROVIDERS='[{
      "name": "azure",
      "icon": "fa-circle",
      "token_key": "access_token",
      "remote_app": {
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "api_base_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/",
        "request_token_url": null,
        "access_token_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token",
        "authorize_url": "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/authorize",
        "client_kwargs": {
          "scope": "openid email profile"
        }
      }
   }]'

.. seealso::
   For Azure app registration and OAuth setup, see :doc:`apache-airflow-providers-microsoft-azure:connections/azure`
   and the `Azure OAuth2 documentation <https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow>`_.

**Google OAuth2**

.. code-block:: bash

   export AIRFLOW__FAB__OAUTH_PROVIDERS='[{
      "name": "google",
      "icon": "fa-google",
      "token_key": "access_token",
      "remote_app": {
        "client_id": "your-client-id.googleusercontent.com",
        "client_secret": "your-client-secret",
        "api_base_url": "https://www.googleapis.com/oauth2/v2/",
        "request_token_url": null,
        "access_token_url": "https://oauth2.googleapis.com/token",
        "authorize_url": "https://accounts.google.com/o/oauth2/auth",
        "client_kwargs": {
          "scope": "openid email profile"
        }
      }
   }]'

.. seealso::
   For Google OAuth setup and credential configuration, see :doc:`apache-airflow-providers-google:connections/gcp`
   and :doc:`apache-airflow-providers-google:api-auth-backend/google-openid`.

Troubleshooting
---------------

**Common Issues**

- **Authentication fails after configuration**:

  - Check Airflow and webserver logs for detailed error messages
  - Ensure all environment variables are set and exported correctly
  - Verify callback URLs in your SSO provider match your Airflow webserver URL (typically ``http://your-airflow-domain/oauth-authorized``)

- **Redirect URI mismatch**:

  - In your OAuth provider, set the redirect URI to: ``http://your-airflow-domain/oauth-authorized``
  - For development, this might be: ``http://localhost:8080/oauth-authorized``

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
