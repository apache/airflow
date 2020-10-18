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



.. _howto/connection:azure:

Microsoft Azure Connection
==========================

The Microsoft Azure connection type enables the :ref:`Azure Integrations <Azure>`.

Authenticating to Azure
-----------------------

There are three ways to connect to Azure using Airflow.

1. Use `token credentials
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-token-credentials>`_
   i.e. add specific credentials (client_id, secret, tenant) and subscription id to the Airflow connection.
2. Use a `JSON file
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-a-json-file>`_
   i.e. create a key file on disk and link to it in the Airflow connection.
3. Use a `JSON dictionary
   <https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate?tabs=cmd#authenticate-with-a-json-dictionary>`_
   i.e. add a key config directly into the Airflow connection.

Only one authorization method can be used at a time. If you need to manage multiple credentials or keys then you should
configure multiple connections.

Default Connection IDs
----------------------

Some hooks and operators related to Microsoft Azure use ``azure_default`` by default.

Configuring the Connection
--------------------------

Login (optional)
    Specify the ``client_id`` used for the initial connection.
    This is only needed for *token credentials* authentication mechanism.

Password (optional)
    Specify the ``secret`` used for the initial connection.
    This is only needed for *token credentials* authentication mechanism.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Azure connection.
    The following parameters are all optional:

    * ``tenantId``: Specify the tenant to use.
      This is only needed for *token credentials* authentication mechanism.
    * ``subscriptionId``: Specify the subscription id to use.
      This is only needed for *token credentials* authentication mechanism.
    * ``key_path``: If set, it uses the *JSON file* authentication mechanism.
      It specifies the path to the json file that contains the authentication information.
    * ``key_json``: If set, it uses the *JSON dictionary* authentication mechanism.
      It specifies the json that contains the authentication information. See

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_AZURE_DEFAULT='azure://?key_path=%2Fkeys%2Fkey.json'
