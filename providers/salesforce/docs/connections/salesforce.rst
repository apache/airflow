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

.. _howto/connection:SalesforceHook:

Salesforce Connection
=====================
The Salesforce connection type provides connection to Salesforce via several authentication options:

    * Password
    * Direct Session
    * OAuth 2.0 JWT
    * IP Filtering

Configuring the Connection
--------------------------
Username (optional)
    Specify the email address used to login to your account.

    Use for Password authentication or IP Filtering.

Password (optional)
    Specify the password associated with the account.

    Use for Password authentication or IP Filtering.

Security Token (optional)
    Specify the Salesforce security token for the username.

    Use for Password authentication.

Consumer Key (optional)
    the consumer key generated for the user.

    Use for OAuth 2.0 JWT authentication.

Consumer Secret (optional)
    the consumer secret generated for the user.

    Use for OAuth 2.0 JWT authentication.

Private Key (optional)
    The private key to use for signing the JWT. Provide this or a Private Key File Path (both are not necessary).

    Use for OAuth 2.0 JWT authentication.

Private Key File Path (optional)
    A local path to the private key to be used for signing the JWT. Provide this or a Private Key (both are not necessary).

    Use for OAuth 2.0 JWT authentication.

Organization ID (optional)
    The ID of the organization tied to the Salesforce instance.

    Use for IP Filtering.

Instance (optional)
    The domain name of the Salesforce instance, (i.e. `na1.salesforce.com`).

    Use for Direct Session access.  When calling the `SalesforceHook` a `session_id` also needs to be provided.

Instance URL (optional)
    The full URL of the Salesforce instance, (i.e. `https://na1.salesforce.com`). When calling the `SalesforceHook` a `session_id` also needs to be provided.

    Use for Direct Session access.

Domain (optional)
    The domain to using for connecting to Salesforce. Use common domains, such as 'login'
    or 'test', or Salesforce My domain. If not used, will default to 'login'.

Proxies (optional)
    A mapping of scheme-to-proxy server(s).

Salesforce API Version (optional)
    The version of the Salesforce API to use when attempting to connect.  If not specified a default value will be used.

Client ID (optional)
    The ID of the client.

For security reason we suggest you to use one of the secrets Backend to create this
connection (Using ENVIRONMENT VARIABLE or Hashicorp Vault, GCP Secrets Manager etc).

When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
following the standard syntax of DB connections - where extras are passed as parameters of the URI. For example:

  .. code-block:: bash

    export AIRFLOW_CONN_SALESFORCE_DEFAULT='http://your_username:your_password@https%3A%2F%2Fyour_host.lightning.force.com?security_token=your_token'
