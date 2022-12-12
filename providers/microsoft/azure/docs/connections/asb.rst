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

There are multiple ways to authenticate and authorize access to Azure Service Bus resources:
Currently Supports Shared Access Signatures (SAS).

1. Use a `Connection String
   <https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quickstart-portal#get-the-connection-string>`_
   i.e. Use connection string Field to add ``Connection String`` in the Airflow connection.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Service Bus use ``azure_service_bus_default`` by default.

Configuring the Connection
--------------------------

Connection String
    Specify the Azure Service bus connection string ID used for the initial connection.
    Please find the documentation on how to generate connection string in azure service bus
    `Get connection string
    <https://docs.microsoft.com/en-gb/azure/service-bus-messaging/service-bus-create-namespace-portal#get-the-connection-string.>`_
    Use the key ``connection_string`` to pass in the Connection ID .
