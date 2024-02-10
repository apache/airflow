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



.. _howto/connection:acr:

Microsoft Azure Container Registry Connection
==============================================

The Microsoft Azure Container Registry connection type enables the Azure Container Registry Integrations.

Authenticating to Azure Container Registry
------------------------------------------

There are three way to connect to Azure Container Registry using Airflow.

1. Use `Individual login with Azure AD
   <https://docs.microsoft.com/en-us/azure/container-registry/container-registry-authentication#individual-login-with-azure-ad>`_
   i.e. add specific credentials to the Airflow connection.
2. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
3. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI...

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Container Registry use ``azure_container_registry_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the Image Registry Username used for the initial connection.

Password (optional)
    Specify the Image Registry Password used for the initial connection. It can be left out to fall back on DefaultAzureCredential_.

Host
    Specify the Image Registry Server used for the initial connection.

Subscription ID (optional)
    Specify the ID of the subscription used for the initial connection.
    This is needed for Azure Active Directory (Azure AD) authentication.
    Use extra param ``subscription_id`` to pass in the Azure subscription ID.

Resource Group Name (optional)
    Specify the Azure Resource Group Name under which the desired Azure container registry resides.
    This is needed for Azure Active Directory (Azure AD) authentication.
    Use extra param ``resource_group`` to pass in the resource group name.

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``, they'll pass to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with ``managed_identity_client_id``, they'll pass to DefaultAzureCredential_.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

    export AIRFLOW_CONN_AZURE_CONTAINER_REGISTRY_DEFAULT='azure-container-registry://username:password@myregistry.com?tenant=tenant+id&account_name=store+name'

.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::
    Entra
