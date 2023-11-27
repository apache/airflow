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



.. _howto/connection:azure_cosmos:

Microsoft Azure Cosmos
====================================

The Microsoft Azure Cosmos connection type enables the Azure Cosmos Integrations.

Authenticating to Azure
-----------------------

There are three ways to connect to Azure Cosmos using Airflow.

1. Use `Primary Keys`_
   i.e. add specific credentials (client_id, secret, tenant) and account name to the Airflow connection.
2. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
3. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI, etc.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Cosmos use ``azure_cosmos_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the Cosmos Endpoint URI used for the initial connection.

Password (optional)
    Specify the Cosmos Master Key Token used for the initial connection.
    It can be left out to fall back on DefaultAzureCredential_.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure Cosmos connection.
    The following parameters are all optional:

    * ``database_name``: Specify the azure cosmos database to use.
    * ``collection_name``: Specify the azure cosmos collection to use.
    * ``subscription_id``: Specify the ID of the subscription used for the initial connection. Required for falling back to DefaultAzureCredential_
    * ``resource_group_name``: Specify the  Azure Resource Group Name under which the desired azure cosmos resides. Required for falling back to DefaultAzureCredential_
    * ``managed_identity_client_id``:  The client ID of a user-assigned managed identity. If provided with `workload_identity_tenant_id`, they'll pass to ``DefaultAzureCredential``.
    * ``workload_identity_tenant_id``: ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with `managed_identity_client_id`, they'll pass to ``DefaultAzureCredential``.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_COSMOS_DEFAULT='azure-cosmos://https%3A%2F%2Fairflow.azure.com:master%20key@?database_name=mydatabase&collection_name=mycollection'


.. _Primary Keys: https://docs.microsoft.com/en-us/azure/cosmos-db/secure-access-to-data#primary-keys
.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::

    Entra
