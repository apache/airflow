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



.. _howto/connection:mssql:

MSSQL Connection
======================
The MSSQL connection type enables connection to `Microsoft SQL Server <https://www.microsoft.com/en-in/sql-server/>`__.

Default Connection IDs
----------------------

MSSQL Hook uses parameter ``mssql_conn_id`` for the connection ID. The default value is ``mssql_default``.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Port (required)
    The port to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in MSSQL
    connection.

    More details on all MSSQL parameters supported can be found in
    `MSSQL documentation <https://docs.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver15>`_.

When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
following the standard syntax of DB connections - where extras are passed as parameters
of the URI. Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_MSSQL_DEFAULT='mssql://username:password@server.com:1433/database_name'

If serializing with JSON:

.. code-block:: bash

    export AIRFLOW_CONN_MSSQL_DEFAULT='{
        "conn_type": "mssql",
        "login": "username",
        "password": "password",
        "host": "server.com",
        "port": 1433,
        "schema": "database_name"
    }'
