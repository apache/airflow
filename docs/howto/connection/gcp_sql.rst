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



Google Cloud SQL Connection
===========================

The gcpcloudsql:// connection is used by
:class:`airflow.contrib.operators.gcp_sql_operator.CloudSqlQueryOperator` to perform query
on a Google Cloud SQL database. Google Cloud SQL database can be either
Postgres or MySQL, so this is a "meta" connection type. It introduces common schema
for both MySQL and Postgres, including what kind of connectivity should be used.
Google Cloud SQL supports connecting via public IP or via Cloud SQL Proxy.
In the latter case the
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook` uses
:class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlProxyRunner` to automatically prepare
and use temporary Postgres or MySQL connection that will use the proxy to connect
(either via TCP or UNIX socket.

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

Extra (optional)
    Specify the extra parameters (as JSON dictionary) that can be used in Google Cloud SQL
    connection.

    Details of all the parameters supported in extra field can be found in
    :class:`~airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook`

    Example "extras" field:

    .. code-block:: json

       {
          "database_type": "mysql",
          "project_id": "example-project",
          "location": "europe-west1",
          "instance": "testinstance",
          "use_proxy": true,
          "sql_proxy_use_tcp": false
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable), you should specify
    it following the standard syntax of DB connection, where extras are passed as
    parameters of the URI. Note that all components of the URI should be URL-encoded.

    For example:

    .. code-block:: bash

        gcpcloudsql://user:XXXXXXXXX@1.1.1.1:3306/mydb?database_type=mysql&project_id=example-project&location=europe-west1&instance=testinstance&use_proxy=True&sql_proxy_use_tcp=False
