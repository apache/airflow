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



.. _howto/connection:msgraph:

Microsoft Graph API Connection
==============================

The Microsoft Graph API connection type enables Microsoft Graph API Integrations.

The :class:`~airflow.providers.microsoft.azure.hooks.msgraph.KiotaRequestAdapterHook` and :class:`~airflow.providers.microsoft.azure.operators.msgraph.MSGraphAsyncOperator` requires a connection of type ``msgraph`` to authenticate with Microsoft Graph API.

Authenticating to Microsoft Graph API
-------------------------------------

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, client_secret, tenant_id) to the Airflow connection.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Graph API use ``msgraph_default`` by default.

Configuring the Connection
--------------------------

Client ID
    Specify the ``client_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.


Client Secret
    Specify the ``client_secret`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism unless a certificate is used.


Tenant ID
    Specify the ``tenant_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.


API Version
    Specify the ``api_version`` used for the initial connection.
    Default value is ``v1.0``.


Authority
    The ``authority`` parameter defines the endpoint (or tenant) that MSAL uses to authenticate requests.
    It determines which identity provider will handle authentication.
    Default value is ``login.microsoftonline.com``.


Scopes
    The ``scopes`` parameter specifies the permissions or access rights that your application is requesting for a connection.
    These permissions define what resources or data your application can access on behalf of the user or application.
    Default value is ``https://graph.microsoft.com/.default``.


Certificate path
    The ``certificate_path`` parameter specifies the filepath where the certificate is located.
    Both ``certificate_path`` and ``certificate_data`` parameter cannot be used together, they should be mutually exclusive.
    Default value is None.


Certificate data
    The ``certificate_date`` parameter specifies the certificate as a string.
    Both ``certificate_path`` and ``certificate_data`` parameter cannot be used together, they should be mutually exclusive.
    Default value is None.


Disable instance discovery
    The ``disable_instance_discovery`` parameter determines whether MSAL should validate and discover Azure AD endpoints dynamically during runtime.
    Default value is False (e.g. disabled).


Allowed hosts
    The ``allowed_hosts`` parameter is used to define a list of acceptable hosts that the authentication provider will trust when making requests.
    This parameter is particularly useful for enhancing security and controlling which endpoints the authentication provider interacts with.


Proxies
    The ``proxies`` parameter is used to define a dict for the ``http`` and ``https`` schema, the ``no`` key can be use to define hosts not to be used by the proxy.
    Default value is None.


Verify environment
    The ``verify`` parameter specifies whether SSL certificates should be verified when making HTTPS requests.
    By default, ``verify`` parameter is set to True. This means that the `httpx <https://www.python-httpx.org>`_ library will verify the SSL certificate presented by the server to ensure:

    - The certificate is valid and trusted.
    - The certificate matches the hostname of the server.
    - The certificate has not expired or been revoked.

    Setting ``verify`` to False disables SSL certificate verification. This is typically used in development or testing environments when working with self-signed certificates or servers without valid certificates.


Trust environment
    The ``trust_env`` parameter determines whether or not the library should use environment variables for configuration when making HTTP/HTTPS requests.
    By default, ``trust_env`` parameter is set to True. This means the `httpx <https://www.python-httpx.org>`_ library will automatically trust and use environment variables for proxy configuration, SSL settings, and authentication.


Base URL
    The ``base_url`` parameter allows you to override the default base url used to make it requests, namely ``https://graph.microsoft.com/``.
    This can be useful if you want to use the MSGraphAsyncOperator to call other Microsoft REST API's like Sharepoint or PowerBI.
    Default value is None.


.. raw:: html

  <div align="center" style="padding-bottom:10px">
    <img src="images/msgraph.png"
         alt="Microsoft Graph API connection form">
  </div>


.. spelling:word-list::

    Entra
