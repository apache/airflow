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



.. _howto/connection:adx:

Microsoft Azure Data Explorer
=============================

The ``Azure Data Explorer`` connection type enables Azure Data Explorer (ADX) integrations in Airflow.

Authenticating to Azure Data Explorer
---------------------------------------

There are five ways to connect to Azure Data Explorer using Airflow.

1. Use `AAD application certificate
   <https://docs.microsoft.com/en-us/azure/active-directory/develop/active-directory-certificate-credentials>`_
   (i.e. use "AAD_APP" or "AAD_APP_CERT" as the Authentication Method in the Airflow connection).
2. Use `AAD username and password
   <https://docs.microsoft.com/en-us/azure/active-directory/authentication/concept-authentication-methods>`_
   (i.e. use "AAD_CREDS" as the Authentication Method in the Airflow connection).
3. Use a `AAD device code
   <https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-device-code>`_
   (i.e. use "AAD_DEVICE" as the Authentication Method in the Airflow connection).
4. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
5. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI and etc.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Data Explorer use ``azure_data_explorer_default`` by default.

Configuring the Connection
--------------------------

Data Explorer Cluster URL
    Specify the Data Explorer cluster URL. Needed for all authentication methods.

Authentication Method
    Specify authentication method. Available authentication methods are:

    * AAD_APP: Authentication with AAD application certificate. A Tenant ID is required when using this method. Provide application ID and application key through Username and Password parameters.

    * AAD_APP_CERT: Authentication with AAD application certificate. Tenant ID, Application PEM Certificate, and Application Certificate Thumbprint are required when using this method.

    * AAD_CREDS: Authentication with AAD username and password. A Tenant ID is required when using this method. Username and Password parameters are used for authentication with AAD.

    * AAD_DEVICE: Authenticate with AAD device code. Please note that if you choose this option, you'll need to authenticate for every new instance that is initialized. It is highly recommended to create one instance and use it for all queries.

    * AZURE_TOKEN_CRED: Authentication with DefaultAzureCredential_. This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI and etc. Only the "Data Explorer Cluster URL" is required when using this method.

Username (optional)
    Specify the username used for data explorer. Needed for with AAD_APP, AAD_APP_CERT, and AAD_CREDS authentication methods.

Password (optional)
    Specify the password used for data explorer. Needed for with AAD_APP, and AAD_CREDS authentication methods.

Tenant ID (optional)
    Specify AAD tenant. Needed for AAD_APP, AAD_APP_CERT, and AAD_CREDS.

Application PEM Certificate (optional)
    Specify the certificate. Needed for AAD_APP_CERT authentication method.

Application Certificate Thumbprint (optional)
    Specify the thumbprint needed for use with AAD_APP_CERT authentication method.

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``, they'll pass to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with ``managed_identity_client_id``, they'll pass to DefaultAzureCredential_.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_DATA_EXPLORER_DEFAULT='azure-data-explorer://add%20username:add%20password@mycluster.com?auth_method=AAD_APP&tenant=tenant+id'


.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::

    Entra
