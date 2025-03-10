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



.. _howto/connection:azure_batch:

Microsoft Azure Batch
=====================

The Microsoft Azure Batch connection type enables the Azure Batch integrations.

Authenticating to Azure Batch
------------------------------------------

There is one way to connect to Azure Batch using Airflow.

1. Use `Azure Shared Key Credential
   <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>`_
   i.e. add shared key credentials to the Airflow connection.
2. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
3. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI and etc.


Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Batch use ``azure_batch_default`` by default.

Configuring the Connection
--------------------------

Batch Account Name (optional)
    Specify the Azure Batch Account Name used for the initial connection.
    It can be left out to fall back on DefaultAzureCredential_.

Batch Account Access Key (optional)
    Specify the access key used for the initial connection.
    It can be left out to fall back on DefaultAzureCredential_.

Batch Account URL
    Specify the batch account URL you would like to use.

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``, they'll pass to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with ``managed_identity_client_id``, they'll pass to DefaultAzureCredential_.


When specifying the connection in environment variable you should specify it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_BATCH_DEFAULT='azure-batch://batch%20acount:batch%20key@?account_url=mybatchaccount.com'


.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::
    Entra
