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



.. _howto/connection:azure_service_bus:

Microsoft Azure Service Bus
=======================================

The Microsoft Azure Service Bus connection type enables the Azure Service Bus Integrations.

Authenticating to Azure Service Bus
------------------------------------

There are two ways to authenticate and authorize access to Azure Service Bus resources:
Azure Active Directory (Azure AD) and Shared Access Signatures (SAS).

1. Use `Azure Active Directory
   <https://docs.microsoft.com/en-gb/azure/service-bus-messaging/service-bus-authentication-and-authorization#azure-active-directory>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.
2. Use `Azure Shared access signature
   <https://docs.microsoft.com/en-gb/azure/service-bus-messaging/service-bus-authentication-and-authorization#azure-active-directory>`_
3. Fallback on `DefaultAzureCredential
   <https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential>`_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI...

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Service Bus use ``azure_service_bus_default`` by default.

Configuring the Connection
--------------------------

Client ID (optional)
    Specify the ``client_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.
    It can be left out to fall back on ``DefaultAzureCredential``.

Secret (optional)
    Specify the ``secret`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.
    It can be left out to fall back on ``DefaultAzureCredential``.

Connection ID
    Specify the Azure Service bus connection string ID used for the initial connection.
    `Get connection string
    <https://docs.microsoft.com/en-gb/azure/service-bus-messaging/service-bus-create-namespace-portal#get-the-connection-string.>`_
    Use the key ``extra__azure_service_bus__connection_string`` to pass in the Connection ID .
