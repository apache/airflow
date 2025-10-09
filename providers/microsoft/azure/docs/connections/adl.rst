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



.. _howto/connection:adl:

Microsoft Azure Data Lake Connection
====================================

The Microsoft Azure Data Lake connection type enables the Azure Data Lake Integrations.

Authenticating to Azure Data Lake
---------------------------------

There is one way to connect to Azure Data Lake using Airflow.

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and account name to the Airflow connection.
2. Fallback on `DefaultAzureCredential
   <https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential>`_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI...

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Data Lake use ``azure_data_lake_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the ``client_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.
    It can be left out to fall back on ``DefaultAzureCredential``.

Password (optional)
    Specify the ``secret`` used for the initial connection.
    This is only needed for *token credentials* authentication mechanism.
    It can be left out to fall back on ``DefaultAzureCredential``.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure Data Lake connection.
    The following parameters are all optional:

    * ``tenant``: (optional) Specify the tenant to use.
      This is needed for *token credentials* authentication mechanism.
      It can be left out to fall back on ``DefaultAzureCredential``.
    * ``account_name``: Specify the azure data lake account name.
      This is sometimes called the ``store_name``

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_DATA_LAKE_DEFAULT='azure-data-lake://client%20id:secret@?tenant=tenant+id&account_name=store+name'
