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



.. _howto/connection:wasb:

Microsoft Azure Blob Storage Connection
=======================================

The Microsoft Azure Blob Storage connection type enables the Azure Blob Storage Integrations.

Authenticating to Azure Blob Storage
------------------------------------

There are six ways to connect to Azure Blob Storage using Airflow.

1. Use `token credentials`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.
2. Use `Azure Shared Key Credential`_
   i.e. add shared key credentials to ``shared_access_key`` the Airflow connection.
3. Use a `SAS Token`_
   i.e. add a key config to ``sas_token`` in the Airflow connection.
4. Use a `Connection String`_
   i.e. add connection string to ``connection_string`` in the Airflow connection.
5. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
6. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI, etc.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Blob Storage use ``wasb_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the login used for Azure Blob Storage. Strictly needed for Active Directory (token) authentication as Service principle credential. Optional for the rest if host (account url) is specified.
    It can be left out to fall back on DefaultAzureCredential_.

Password (optional)
    Specify the password used for Azure Blob Storage. For use with
    Active Directory (token credential) and shared key authentication.
    It can be left out to fall back on DefaultAzureCredential_.

Host (optional)
    Specify the account url for Azure Blob Storage. Strictly needed for Active Directory (token) and DefaultAzureCredential_ authentication as Service principle credential. Optional for the rest if login (account name) is specified.

Blob Storage Connection String (optional)
    Connection string for use with connection string authentication
    It can be left out to fall back on DefaultAzureCredential_..

Blob Storage Shared Access Key (optional)
    Specify the shared access key. Needed only for shared access key authentication.
    It can be left out to fall back on DefaultAzureCredential_.

SAS Token (optional)
    SAS Token for use with SAS Token authentication.
    It can be left out to fall back on DefaultAzureCredential_.

Tenant Id (Active Directory Auth) (optional)
    Specify the tenant to use. Required only for Active Directory (token) authentication.
    It can be left out to fall back on DefaultAzureCredential_.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``client_secret_auth_config``: Extra config to pass while authenticating as a service principal using `ClientSecretCredential`_ It can be left out to fall back on DefaultAzureCredential_.
    * ``managed_identity_client_id``:  The client ID of a user-assigned managed identity. If provided with `workload_identity_tenant_id`, they'll pass to ``DefaultAzureCredential``.
    * ``workload_identity_tenant_id``: ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with `managed_identity_client_id`, they'll pass to ``DefaultAzureCredential``.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example connect with token credentials:

.. code-block:: bash

   export AIRFLOW_CONN_WASB_DEFAULT='wasb://blob%20username:blob%20password@myblob.com?tenant_id=tenant+id'


.. _token credentials: https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials
.. _Azure Shared Key Credential: https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
.. _SAS Token: https://docs.microsoft.com/en-us/rest/api/storageservices/create-account-sas
.. _Connection String: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/storage
.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential
.. _ClientSecretCredential: https://learn.microsoft.com/en-in/python/api/azure-identity/azure.identity.clientsecretcredential?view=azure-python

.. spelling:word-list::

    Entra
