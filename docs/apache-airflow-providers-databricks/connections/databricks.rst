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



.. _howto/connection:databricks:

Databricks Connection
==========================

The Databricks connection type enables the Databricks Integration.

Authenticating to Databricks
----------------------------

There are three ways to connect to Databricks using Airflow.

1. Use a `Personal Access Token (PAT)
   <https://docs.databricks.com/dev-tools/api/latest/authentication.html>`_
   i.e. add a token to the Airflow connection. This is the recommended method.
2. Use Databricks login credentials
   i.e. add the username and password used to login to the Databricks account to the Airflow connection.
3. Use a `service principal
   <https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token>`_
   on Azure Databricks
   i.e. add the Client ID as username, the application secret as password to the Airflow connection and set
   ``use_azure_service_principal`` as well as ``tenant_id`` in the connection extras of the connection.


Default Connection IDs
----------------------

Hooks and operators related to Databricks use ``databricks_default`` by default.

Configuring the Connection
--------------------------

Host (required)
    Specify the Databricks workspace URL

Login (optional)
    Specify the ``username`` used to login to Databricks.
    This is only needed if using the *Databricks login credentials* authentication method.

Password (optional)
    Specify the ``password`` used to login to Databricks.
    This is only needed if using the *Databricks login credentials* authentication method.

Extra (optional)
    Specify the extra parameter (as json dictionary) that can be used in the Databricks connection.
    This parameter is necessary if using the *PAT* authentication method (recommended):

    * ``token``: Specify PAT to use.
    * ``use_azure_service_principal``: Set to ``false`` if login and password are an azure client id and
      client secret.
    * ``tenant_id``: Specify the id of the tenant the azure service principal is defined in. Only used and required
      if ``use_azure_service_principal`` is set to true.

When specifying the connection using an environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://@host-url?token=yourtoken'
