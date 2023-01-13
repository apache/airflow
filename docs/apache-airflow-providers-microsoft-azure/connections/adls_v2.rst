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

.. _howto/connection:adls:

Microsoft Azure Data Lake Storage Gen2 Connection
==================================================

The Microsoft Azure Data Lake Storage Gen2 connection type enables the ADLS gen2 Integrations.

Authenticating to Azure Data Lake Storage Gen2
----------------------------------------------

Currently, there are two ways to connect to Azure Data Lake Storage Gen2 using Airflow.

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.
2. Use a `Connection String
   <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/storage>`_
   i.e. add connection string to ``connection_string`` in the Airflow connection.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Blob Storage use ``azure_data_lake_default`` by default.

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

    * ``tenant_id``: Specify the tenant to use. Needed for Active Directory (token) authentication.
    * ``connection_string``: Connection string for use with connection string authentication.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.
