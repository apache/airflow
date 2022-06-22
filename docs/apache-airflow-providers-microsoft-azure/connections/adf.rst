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

There is one way to connect to Azure Data Factory using Airflow.

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Data Factory use ``azure_data_factory_default`` by default.

Configuring the Connection
--------------------------

Client ID
    Specify the ``client_id`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.

Secret
    Specify the ``secret`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.

Tenant ID
    Specify the ``tenantId`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.

Subscription ID
    Specify the ``subscriptionId`` used for the initial connection.
    This is needed for *token credentials* authentication mechanism.

Factory Name (optional)
    Specify the Azure Data Factory to interface with.
    If not specified in the connection, this needs to be passed in directly to hooks, operators, and sensors.

Resource Group Name (optional)
    Specify the Azure Resource Group Name under which the desired data factory resides.
    If not specified in the connection, this needs to be passed in directly to hooks, operators, and sensors.


When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_DATA_FACTORY_DEFAULT='azure-data-factory://client%20id:secret@?tenantId=tenant+id&subscriptionId=subscription+id&resourceGroup=group+name&factory=factory+name'
