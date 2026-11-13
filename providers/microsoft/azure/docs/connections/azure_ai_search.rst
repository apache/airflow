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


.. _howto/connection:azure_ai_search:

Microsoft Azure AI Search
=========================

The Microsoft Azure AI Search connection type enables the Azure AI Search integrations.

Authenticating to Azure AI Search
---------------------------------

There are three ways to connect to Azure AI Search using Airflow.

1. Use `token credentials
   <https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/overview>`_
   i.e. add the credentials of a service principal (client_id, secret, tenant) to the Airflow connection.
2. Use an `API key`_ by setting it as the password, without a client_id.
   A query key is enough to read documents, writing them needs an admin key.
3. Fallback on DefaultAzureCredential_ when the connection has neither.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI...
   Set ``managed_identity_client_id`` and ``workload_identity_tenant_id`` to use a specific managed identity.

With a service principal or ``DefaultAzureCredential`` the identity needs the
`Search Index Data Reader`_ role on the search service, or ``Search Index Data Contributor`` to write documents.

Default Connection IDs
----------------------

All hooks related to Microsoft Azure AI Search use ``azure_ai_search_default`` by default.

Configuring the Connection
--------------------------

Search Service Endpoint
    Specify the endpoint of the search service, e.g. ``https://<service>.search.windows.net``.
    ``https://`` is added to a bare host name.

Azure Client ID (optional)
    Specify the ``client_id`` of the service principal.
    Leave it empty to authenticate with an API key or with DefaultAzureCredential_.

Azure Secret or API Key (optional)
    Specify the ``secret`` of the service principal, or an `API key`_ when no client_id is set.
    It can be left out to fall back on DefaultAzureCredential_.

Azure Tenant ID (optional)
    Specify the ``tenantId`` of the service principal.

Default Index (optional)
    Specify the ``index`` the hook works on when it is given none.

Batch Size (optional)
    Specify the ``batch_size``: the number of documents the hook sends per write request, from 1 to 1000 (the default).

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``, they'll pass to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with ``managed_identity_client_id``, they'll pass to DefaultAzureCredential_.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_AI_SEARCH_DEFAULT='azure-ai-search://:api%20key@my-service.search.windows.net?index=my-index'


.. _API key: https://learn.microsoft.com/en-us/azure/search/search-security-api-keys
.. _Search Index Data Reader: https://learn.microsoft.com/en-us/azure/search/search-security-rbac
.. _DefaultAzureCredential: https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential
