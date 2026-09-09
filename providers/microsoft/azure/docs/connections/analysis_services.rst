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

Microsoft Azure Analysis Services Connection
============================================

The Microsoft Azure Analysis Services connection type enables model refresh operations through the
`asynchronous refresh REST API <https://learn.microsoft.com/en-us/analysis-services/azure-analysis-services/analysis-services-async-refresh>`__.

The :class:`~airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook`,
:class:`~airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesRefreshOperator`,
and :class:`~airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesSensor`
use this connection.

Default Connection ID
---------------------

The default connection ID is ``azure_analysis_services_default``.

Configuring the Connection
--------------------------

Region Endpoint
    Specify the rollout endpoint in ``host``, for example ``westus.asazure.windows.net``. Do not include
    ``https://``, a port, the Analysis Services server name, or a path. You can find this endpoint in the
    server's full name, such as ``asazure://westus.asazure.windows.net/example-server``.

Client ID
    Specify the Microsoft Entra service principal application (client) ID in ``login``.

Client Secret
    Specify the service principal client secret in ``password``.

Tenant ID
    Specify the Microsoft Entra tenant ID in the ``tenantId`` extra field.

Azure Analysis Services currently requires the service principal to be a server administrator for
asynchronous refresh REST API calls. Add it to the server administrator role using the format
``app:<client-id>@<tenant-id>``. See `Add a service principal to the server administrator role
<https://learn.microsoft.com/en-us/analysis-services/azure-analysis-services/analysis-services-addservprinc-admins>`__.

This connection supports service principal client-secret authentication. Managed identity authentication is
not supported because Azure Analysis Services does not support managed identities for these operations.

.. spelling:word-list::

    rollout
