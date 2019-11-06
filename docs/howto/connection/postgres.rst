..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

PostgresSQL Connection
======================
The Postgres connection type provides connection to a Postgres database.

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
    Specify the extra parameters (as json dictionary) that can be used in postgres
    connection. The following parameters out of the standard python parameters
    are supported:

    * **sslmode** - This option determines whether or with what priority a secure SSL
      TCP/IP connection will be negotiated with the server. There are six modes:
      'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'.
    * **sslcert** - This parameter specifies the file name of the client SSL certificate,
      replacing the default.
    * **sslkey** - This parameter specifies the file name of the client SSL key,
      replacing the default.
    * **sslrootcert** - This parameter specifies the name of a file containing SSL
      certificate authority (CA) certificate(s).
    * **sslcrl** - This parameter specifies the file name of the SSL certificate
      revocation list (CRL).
    * **application_name** - Specifies a value for the application_name
      configuration parameter.
    * **keepalives_idle** - Controls the number of seconds of inactivity after which TCP
      should send a keepalive message to the server.

    More details on all Postgres parameters supported can be found in
    `Postgres documentation <https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING>`_.

    Example "extras" field:

    .. code-block:: json

       {
          "sslmode": "verify-ca",
          "sslcert": "/tmp/client-cert.pem",
          "sslca": "/tmp/server-ca.pem",
          "sslkey": "/tmp/client-key.pem"
       }

    When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        postgresql://postgres_user:XXXXXXXXXXXX@1.1.1.1:5432/postgresdb?sslmode=verify-ca&sslcert=%2Ftmp%2Fclient-cert.pem&sslkey=%2Ftmp%2Fclient-key.pem&sslrootcert=%2Ftmp%2Fserver-ca.pem
