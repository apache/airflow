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



.. _howto/connection:azure_cosmos:

Microsoft Azure Cosmos
====================================

The Microsoft Azure Cosmos connection type enables the Azure Cosmos Integrations.

Authenticating to Azure
-----------------------

There is one way to connect to Azure Cosmos using Airflow.

1. Use `Primary Keys
   <https://docs.microsoft.com/en-us/azure/cosmos-db/secure-access-to-data#primary-keys>`_
   i.e. add specific credentials (client_id, secret, tenant) and account name to the Airflow connection.

Default Connection IDs
----------------------

All hooks and operators related to Microsoft Azure Cosmos use ``azure_cosmos_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the Cosmos Endpoint URI used for the initial connection.

Password
    Specify the Cosmos Master Key Token used for the initial connection.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure Cosmos connection.
    The following parameters are all optional:

    * ``database_name``: Specify the azure cosmos database to use.
    * ``collection_name``: Specify the azure cosmos collection to use.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_COSMOS_DEFAULT='azure-cosmos://https%3A%2F%2Fairflow.azure.com:master%20key@?database_name=mydatabase&collection_name=mycollection'
