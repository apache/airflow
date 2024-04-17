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

The Microsoft Azure Service Bus connection type enables the Azure Service Bus Integration.

Authenticating to Azure Service Bus
------------------------------------

There are three ways to authenticate and authorize access to Azure Service Bus resources:

1. Use a `Connection String`_
   i.e. Use connection string Field to add ``Connection String`` in the Airflow connection.
2. Use managed identity by setting ``managed_identity_client_id``, ``workload_identity_tenant_id`` (under the hook, it uses DefaultAzureCredential_ with these arguments)
3. Fallback on DefaultAzureCredential_.
   This includes a mechanism to try different options to authenticate: Managed System Identity, environment variables, authentication through Azure CLI and etc.
   ``fully_qualified_namespace`` is required in this authentication mechanism.


Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Service Bus use ``azure_service_bus_default`` by default.

Configuring the Connection
--------------------------

Connection String (optional)
    Specify the Azure Service bus connection string ID used for the initial connection.
    Please find the documentation on how to generate connection string in azure service bus
    `Get connection string`_
    Use the key ``connection_string`` to pass in the Connection ID .
    It can be left out to fall back on DefaultAzureCredential_.

Fully Qualified Namespace (optional)
   Specify the fully qualified Service Bus namespace that the connection is associated with. This is likely to be similar to ``{yournamespace}.servicebus.windows.net``.
   Use the key ``fully_qualified_namespace`` to pass in the Connection ID .
   This is required when falling back to DefaultAzureCredential_.

Managed Identity Client ID (optional)
    The client ID of a user-assigned managed identity. If provided with ``workload_identity_tenant_id``, they'll pass to DefaultAzureCredential_.

Workload Identity Tenant ID (optional)
    ID of the application's Microsoft Entra tenant. Also called its "directory" ID. If provided with ``managed_identity_client_id``, they'll pass to DefaultAzureCredential_.


.. _Connection String: https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quickstart-portal#get-the-connection-string
.. _DefaultAzureCredential: https://docs.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential
.. _Get connection string: https://docs.microsoft.com/en-gb/azure/service-bus-messaging/service-bus-create-namespace-portal#get-the-connection-string

.. spelling:word-list::
    Entra
