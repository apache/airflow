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



.. _howto/connection:azure_fileshare:

Microsoft Azure File Share Connection
=====================================

The Microsoft Azure File Share connection type enables the Azure File Share Integrations.

Authenticating to Azure File Share
----------------------------------

There are five ways to connect to Azure File Share using Airflow.

1. Use `token credentials
   <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/identity/azure-identity>`_
   i.e. add specific credentials (client_id, secret) and subscription id to the Airflow connection.
2. Use a `SAS Token
   <https://learn.microsoft.com/en-gb/azure/storage/common/storage-sas-overview>`_
   i.e. add a key config to ``sas_token`` in the Airflow connection.
3. Use a `Connection String
   <https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string>`_
   i.e. add connection string to ``connection_string`` in the Airflow connection.
4. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
5. Fallback on `DefaultAzureCredential`_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI and etc.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Azure File Share use ``azure_fileshare_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the login used for azure blob storage. For use with Shared Key Credential, SAS Token authentication and  `DefaultAzureCredential`_.

Password (optional)
    Specify the password used for azure blob storage. For use with
    Active Directory (token credential) and shared key authentication.
    It can be left out to fall back on DefaultAzureCredential_.

Host (optional)
    Specify the account url for anonymous public read, Active Directory, shared access key authentication.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``connection_string``: Connection string for use with connection string authentication. It can be left out to fall back on DefaultAzureCredential_.
    * ``sas_token``: SAS Token for use with SAS Token authentication. It can be left out to fall back on DefaultAzureCredential_.
    * ``managed_identity_client_id``:  The client ID of a user-assigned managed identity. If provided with `workload_identity_tenant_id`, they'll pass to DefaultAzureCredential_.
    * ``workload_identity_tenant_id``: ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with `managed_identity_client_id`, they'll pass to DefaultAzureCredential_.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example connect with token credentials:

.. code-block:: bash

   export AIRFLOW_CONN_WASP_DEFAULT='azure_fileshare://blob%20username@myblob.com?sas_token=token'


.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::

    Entra
