
=============================
Single Sign-On (SSO) Integration
=============================

This guide provides a working example and step-by-step instructions for configuring Single Sign-On (SSO) in Apache Airflow using a generic OAuth2 provider. The process is similar for other providers (e.g., Google, Okta, Azure AD, Auth0).

.. contents:: Table of Contents
   :local:
   :depth: 2

Prerequisites
-------------
- Airflow 2.x installed and running
- Access to an OAuth2 SSO provider (e.g., Google, Okta, Auth0, Azure AD)
- Admin access to Airflow and your SSO provider

Configuration Steps
-------------------

1. **Install Required Packages**
   
   Airflow uses Flask AppBuilder (FAB) for authentication. To enable OAuth, install the required provider:

   .. code-block:: bash

      pip install 'apache-airflow[auth,oauth]'  # or your specific provider

2. **Configure Airflow for SSO**

   Edit your ``airflow.cfg`` or set environment variables as follows:

   .. code-block:: ini

      [webserver]
      authenticate = True
      auth_backend = airflow.www.security.oauth_auth.OAuth2AuthBackend

   Or, for some providers, you may use:

      auth_backend = airflow.www.security.fab_security.manager.AuthOAuthView

3. **Set Provider-Specific Environment Variables**

   Example for generic OAuth2:

   .. code-block:: bash

      export AIRFLOW__OAUTH__CLIENT_ID=your-client-id
      export AIRFLOW__OAUTH__CLIENT_SECRET=your-client-secret
      export AIRFLOW__OAUTH__AUTHORIZE_URL=https://provider.com/oauth/authorize
      export AIRFLOW__OAUTH__ACCESS_TOKEN_URL=https://provider.com/oauth/token
      export AIRFLOW__OAUTH__USERINFO_URL=https://provider.com/oauth/userinfo
      export AIRFLOW__OAUTH__SCOPE=openid email profile

   Adjust these for your provider. See the provider's documentation for details.

4. **Restart Airflow Webserver**

   .. code-block:: bash

      airflow webserver --reload

5. **Test SSO Login**

   Open the Airflow UI. You should see a "Sign in with SSO" button. Log in using your SSO provider.

Troubleshooting
---------------
- Check Airflow and webserver logs for errors.
- Ensure all environment variables are set and exported.
- Verify callback URLs in your SSO provider match your Airflow webserver URL.

References
----------
- `Airflow Authentication Docs <https://airflow.apache.org/docs/apache-airflow/stable/security/authentication.html>`_
- `Flask AppBuilder Security <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_
- Your SSO provider's OAuth2 documentation

.. note::
   This is a generic example. For production, review security best practices and provider-specific guides.
