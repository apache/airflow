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



.. _howto/connection:snowflake:

Snowflake Connection
====================

The Snowflake connection type enables integrations with Snowflake.

Authenticating to Snowflake
---------------------------

Authenticate to Snowflake using the `Snowflake python connector default authentication
<https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-using-the-default-authenticator>`_.

Default Connection IDs
----------------------

Hooks, operators, and sensors related to Snowflake use ``snowflake_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the snowflake username. For OAuth, the OAuth Client ID.

Password
    Specify the snowflake password. For public key authentication, the passphrase for the private key.
    For OAuth, the OAuth Client Secret.

Schema (optional)
    Specify the snowflake schema to be used.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the snowflake connection.
    The following parameters are all optional:

    * ``account``: Snowflake account name.
    * ``database``: Snowflake database name.
    * ``region``: Warehouse region.
    * ``warehouse``: Snowflake warehouse name.
    * ``role``: Snowflake role.
    * ``authenticator``: To connect using OAuth set this parameter ``oauth``.
    * ``token_endpoint``: Specify token endpoint for external OAuth provider.
    * ``grant_type``: Specify grant type for OAuth authentication. Currently supported: ``refresh_token`` (default), ``client_credentials``.
    * ``scope``: Specify OAuth scope to include in the access token request for any OAuth grant type.
    * ``refresh_token``: Specify refresh_token for OAuth connection.
    * ``azure_conn_id``: Azure Connection ID to be used for retrieving the OAuth token using Azure Entra authentication. Login and Password fields aren't required when using this method. Scope for the Azure OAuth token can be set in the config option ``azure_oauth_scope`` under the section ``[snowflake]``. Requires `apache-airflow-providers-microsoft-azure>=12.8.0`.
    * ``private_key_file``: Specify the path to the private key file.
    * ``private_key_content``: Specify the content of the private key file in base64 encoded format. You can use the following Python code to encode the private key:

      .. code-block:: python

         import base64

         with open("path/to/private_key.pem", "rb") as key_file:
             private_key_content = base64.b64encode(key_file.read()).decode("utf-8")
             print(private_key_content)
    * ``session_parameters``: Specify `session level parameters <https://docs.snowflake.com/en/user-guide/python-connector-example.html#setting-session-parameters>`_.
    * ``insecure_mode``: Turn off OCSP certificate checks. For details, see: `How To: Turn Off OCSP Checking in Snowflake Client Drivers - Snowflake Community <https://community.snowflake.com/s/article/How-to-turn-off-OCSP-checking-in-Snowflake-client-drivers>`_.
    * ``host``: Target Snowflake hostname to connect to (e.g., for local testing with LocalStack).
    * ``port``: Target Snowflake port to connect to (e.g., for local testing with LocalStack).
    * ``ocsp_fail_open``: Specify `ocsp_fail_open <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#label-python-ocsp-choosing-fail-open-or-fail-close-mode>`_.

URI format example
^^^^^^^^^^^^^^^^^^

If serializing with Airflow URI:

.. code-block:: bash

   export AIRFLOW_CONN_SNOWFLAKE_DEFAULT='snowflake://user:password@/db-schema?account=account&database=snow-db&region=us-east&warehouse=snow-warehouse'

When specifying the connection as an environment variable in Airflow versions prior to 2.3.0, you need to specify the connection using the URI format.

Note that all components of the URI should be URL-encoded.

JSON format example
^^^^^^^^^^^^^^^^^^^

If serializing with JSON:

.. code-block:: bash

    export AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{
        "conn_type": "snowflake",
        "login": "user",
        "password": "password",
        "schema": "db-schema",
        "extra": {
            "account": "account",
            "database": "database",
            "region": "us-east",
            "warehouse": "snow-warehouse"
        }
    }'
