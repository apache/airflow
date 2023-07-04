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

The Databricks connection type enables the Databricks & Databricks SQL Integration.

Authenticating to Databricks
----------------------------

There are several ways to connect to Databricks using Airflow.

1. Use a `Personal Access Token (PAT)
   <https://docs.databricks.com/dev-tools/api/latest/authentication.html>`_
   i.e. add a token to the Airflow connection. This is the recommended method.
2. Use Databricks login credentials
   i.e. add the username and password used to login to the Databricks account to the Airflow connection.
   Note that username/password authentication is discouraged and not supported for
   :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksSqlOperator`.
3. Using Azure Active Directory (AAD) token generated from Azure Service Principal's ID and secret
   (only on Azure Databricks).  Service principal could be defined as a
   `user inside workspace <https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--api-access-for-service-principals-that-are-azure-databricks-workspace-users-and-admins>`_, or `outside of workspace having Owner or Contributor permissions <https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--api-access-for-service-principals-that-are-not-workspace-users>`_
4. Using Azure Active Directory (AAD) token obtained for `Azure managed identity <https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token>`_,
   when Airflow runs on the VM with assigned managed identity (system-assigned or user-assigned)

Default Connection IDs
----------------------

Hooks and operators related to Databricks use ``databricks_default`` by default.

Configuring the Connection
--------------------------

Host (required)
    Specify the Databricks workspace URL

Login (optional)
    * If authentication with *Databricks login credentials* is used then specify the ``username`` used to login to Databricks.
    * If *authentication with Azure Service Principal* is used then specify the ID of the Azure Service Principal
    * If authentication with *PAT* is used then either leave this field empty or use 'token' as login (both work, the only difference is that if login is empty then token will be sent in request header as Bearer token, if login is 'token' then it will be sent using Basic Auth which is allowed by Databricks API, this may be useful if you plan to reuse this connection with e.g. SimpleHttpOperator)

Password (optional)
    * If authentication with *Databricks login credentials*  is used then specify the ``password`` used to login to Databricks.
    * If authentication with *Azure Service Principal* is used then specify the secret of the Azure Service Principal
    * If authentication with *PAT* is used, then specify PAT (recommended)

Extra (optional)
    Specify the extra parameter (as json dictionary) that can be used in the Databricks connection.

    Following parameter could be used if using the *PAT* authentication method:

    * ``token``: Specify PAT to use. Consider to switch to specification of PAT in the Password field as it's more secure.

    Following parameters are necessary if using authentication with AAD token:

    * ``azure_tenant_id``: ID of the Azure Active Directory tenant
    * ``azure_resource_id``: optional Resource ID of the Azure Databricks workspace (required if Service Principal isn't
      a user inside workspace)
    * ``azure_ad_endpoint``: optional host name of Azure AD endpoint if you're using special `Azure Cloud (GovCloud, China, Germany) <https://docs.microsoft.com/en-us/graph/deployments#app-registration-and-token-service-root-endpoints>`_. The value must contain a protocol. For example: ``https://login.microsoftonline.de``.

    Following parameters are necessary if using authentication with AAD token for Azure managed identity:

    * ``use_azure_managed_identity``: required boolean flag to specify if managed identity needs to be used instead of
      service principal
    * ``azure_resource_id``: optional Resource ID of the Azure Databricks workspace (required if managed identity isn't
      a user inside workspace)

    Following parameters could be set when using
    :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksSqlOperator`:

    * ``http_path``: optional HTTP path of Databricks SQL endpoint or Databricks cluster. See `documentation <https://docs.databricks.com/dev-tools/python-sql-connector.html#get-started>`_.
    * ``session_configuration``: optional map containing Spark session configuration parameters.
    * named internal arguments to the ``Connection`` object from ``databricks-sql-connector`` package.


When specifying the connection using an environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://@host-url?token=yourtoken'
