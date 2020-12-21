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
The HTTP connection type provides connection to Salesforce.

Configuring the Connection
--------------------------
Host (required)
    specify the host address to connect: ``https://your_host.lightning.force.com``

Login (required)
    Specify the email address used to login to your account.

Password (required)
    Specify the password associated with the account.

Extra (required)
    Specify the extra parameters (as json dictionary) that can be used in Salesforce
    connection.
    The following parameter is required:

    * ``security_token``: Salesforce token.

    The following parameter is optional:

    * ``domain``: set to ``test`` if working in sandbox mode.

    For security reason we suggest you to use one of the secrets Backend to create this
    connection (Using ENVIRONMENT VARIABLE or Hashicorp Vault, GCP Secrets Manager etc).


    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections - where extras are passed as parameters
    of the URI.

    For example:

    .. code-block:: bash

       export AIRFLOW_CONN_SALESFORCE_DEFAULT='http://your_username:your_password@https%3A%2F%2Fyour_host.lightning.force.com?security_token=your_token'


Examples for the **Extra** field
--------------------------------
Setting up sandbox mode:

.. code-block:: json

    {
      "security_token": "your_token",
      "domain":"test"
    }

.. note::
  Airflow currently does not support other login methods such as IP filtering and JWT.
