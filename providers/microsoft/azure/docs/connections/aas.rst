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



.. _howto/connection:azure_analysis_services:

Microsoft Azure Analysis Services
==================================

The Microsoft Azure Analysis Services connection type enables the Azure Analysis Services integration.

Authenticating to Azure Analysis Services
------------------------------------------

There are two ways to connect to Azure Analysis Services using Airflow.

1. Use `client secret credentials <https://learn.microsoft.com/en-us/azure/analysis-services/analysis-services-addservprinc-admins>`_
   i.e. add specific credentials (Client ID, Client Secret, Tenant ID) to the Airflow connection.
2. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
   or fall back on DefaultAzureCredential_.

Default Connection IDs
-----------------------

All hooks and operators related to Microsoft Azure Analysis Services use ``azure_analysis_services_default`` by default.

Configuring the Connection
---------------------------

Region Endpoint (host)
    The region-specific endpoint for the Azure Analysis Services server,
    e.g. ``eastus.asazure.windows.net``.

Client ID (login)
    The ``client_id`` of the service principal used for authentication.
    Required for *client secret* authentication. Can be left out to fall back on DefaultAzureCredential_.

Client Secret (password)
    The ``client_secret`` of the service principal used for authentication.
    Required for *client secret* authentication. Can be left out to fall back on DefaultAzureCredential_.

Tenant ID
    The Azure tenant ID. Required when using client secret authentication.
    Use extra param ``tenantId`` to pass in the tenant ID.

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``,
    they are passed to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. If provided with ``managed_identity_client_id``,
    they are passed to DefaultAzureCredential_.

When specifying the connection in an environment variable, use URI syntax.
All components of the URI should be URL-encoded.

Examples
--------

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_ANALYSIS_SERVICES_DEFAULT='azure-analysis-services://clientid:clientsecret@eastus.asazure.windows.net?tenantId=tenant+id'


.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential

.. spelling:word-list::
    Entra
    asazure
