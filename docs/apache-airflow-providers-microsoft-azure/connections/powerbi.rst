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



.. _howto/connection:powerbi:

Microsoft Power BI Connection
=============================

The Microsoft Power BI connection type enables the Power BI Integrations.

The :class:`~airflow.providers.microsoft.azure.hooks.powerbi.PowerBIHook` and :class:`~airflow.providers.microsoft.azure.operators.powerbi.PowerBIDatasetOperator` requires a connection of type ``powerbi`` to authenticate with Power BI.

Authenticating to Power BI
-------------------------------

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Power BI use ``powerbi_default`` by default.

Configuring the Connection
--------------------------

Client ID
    Specify the ``client_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.


Client Secret
    Specify the ``secret`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.


Tenant ID
    Specify the ``tenantId`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.

.. spelling:word-list::

    Entra
