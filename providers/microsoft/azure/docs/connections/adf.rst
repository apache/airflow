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



.. _howto/connection:adf:

Microsoft Azure Data Factory
=======================================

The Microsoft Azure Data Factory connection type enables the Azure Data Factory Integrations.

Authenticating to Azure Data Factory
------------------------------------

There are three ways to connect to Azure Data Factory using Airflow.

1. Use `token credentials <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.
2. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
3. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI...

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Data Factory use ``azure_data_factory_default`` by default.

Configuring the Connection
--------------------------

Client ID
    Specify the ``client_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.
    It can be left out to fall back on DefaultAzureCredential_.

Secret
    Specify the ``secret`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.
    It can be left out to fall back on DefaultAzureCredential_.

Tenant ID
    Specify the Azure tenant ID used for the initial connection.
    This is needed for *token credentials* authentication mechanism.
    It can be left out to fall back on DefaultAzureCredential_.
    Use extra param ``tenantId`` to pass in the tenant ID.

Subscription ID
    Specify the ID of the subscription used for the initial connection.
    This is needed for all authentication mechanisms.
    Use extra param ``subscriptionId`` to pass in the Azure subscription ID.

Factory Name (optional)
    Specify the Azure Data Factory to interface with.
    If not specified in the connection, this needs to be passed in directly to hooks, operators, and sensors.
    Use extra param ``factory_name`` to pass in the factory name.

Resource Group Name (optional)
    Specify the Azure Resource Group Name under which the desired data factory resides.
    If not specified in the connection, this needs to be passed in directly to hooks, operators, and sensors.
    Use extra param ``resource_group_name`` to pass in the resource group name.

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``, they'll pass to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with ``managed_identity_client_id``, they'll pass to DefaultAzureCredential_.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

Examples
--------

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_DATA_FACTORY_DEFAULT='azure-data-factory://applicationid:serviceprincipalpassword@?tenantId=tenant+id&subscriptionId=subscription+id&resource_group_name=group+name&factory_name=factory+name'

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_DATA_FACTORY_DEFAULT='azure-data-factory://applicationid:serviceprincipalpassword@?tenantId=tenant+id&subscriptionId=subscription+id'


.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::
    Entra
