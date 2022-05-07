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

There are four ways to connect to Azure Blob Storage using Airflow.

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.
2. Use `Azure Shared Key Credential
   <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>`_
   i.e. add shared key credentials to ``extra__wasb__shared_access_key`` the Airflow connection.
3. Use a `SAS Token
   <https://docs.microsoft.com/en-us/rest/api/storageservices/create-account-sas>`_
   i.e. add a key config to ``extra__wasb__sas_token`` in the Airflow connection.
4. Use a `Connection String
   <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/storage>`_
   i.e. add connection string to ``extra__wasb__connection_string`` in the Airflow connection.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Blob Storage use ``wasb_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the login used for azure blob storage. For use with Shared Key Credential and SAS Token authentication.

Password (optional)
    Specify the password used for azure blob storage. For use with
    Active Directory (token credential) and shared key authentication.

Host (optional)
    Specify the account url for anonymous public read, Active Directory, shared access key authentication.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``extra__wasb__tenant_id``: Specify the tenant to use. Needed for Active Directory (token) authentication.
    * ``extra__wasb__shared_access_key``: Specify the shared access key. Needed for shared access key authentication.
    * ``extra__wasb__connection_string``: Connection string for use with connection string authentication.
    * ``extra__wasb__sas_token``: SAS Token for use with SAS Token authentication.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example connect with token credentials:

.. code-block:: bash

   export AIRFLOW_CONN_WASB_DEFAULT='wasb://blob%20username:blob%20password@myblob.com?tenant_id=tenant+id'
