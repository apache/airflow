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



.. _howto/connection:vertica:

Vertica Connection
==================
The Vertica connection type provides connection to a Vertica database.

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
    Specify the extra parameters (as json dictionary) that can be used in Vertica
    connection.

    The following extras are supported:

      * ``backup_server_node``: See `Connection Failover <https://github.com/vertica/vertica-python#connection-failover>`_.
      * ``binary_transfer``: See `Data Transfer Format <https://github.com/vertica/vertica-python#data-transfer-format>`_.
      * ``connection_load_balance``: See `Connection Load Balancing <https://github.com/vertica/vertica-python#connection-load-balancing>`_.
      * ``connection_timeout``: The number of seconds (can be a nonnegative floating point number) the client
        waits for a socket operation (Establishing a TCP connection or read/write operation).
      * ``disable_copy_local``: See `COPY FROM LOCAL <https://github.com/vertica/vertica-python#method-2-copy-from-local-sql-with-cursorexecute>`_.
      * ``kerberos_host_name``: See `Kerberos Authentication <https://github.com/vertica/vertica-python#kerberos-authentication>`_.
      * ``kerberos_service_name``: See `Kerberos Authentication <https://github.com/vertica/vertica-python#kerberos-authentication>`_.
      * ``log_level``: Enable vertica client logging. Traces will be visible in tasks log. See `Logging <https://github.com/vertica/vertica-python#logging>`_.
      * ``request_complex_types:``: See `SQL Data conversion to Python objects <https://github.com/vertica/vertica-python#sql-data-conversion-to-python-objects>`_.
      * ``session_label``: Sets a label for the connection on the server.
      * ``ssl``: Support only True or False. See `TLS/SSL <https://github.com/vertica/vertica-python#tlsssl>`_.
      * ``unicode_error``: See `UTF-8 encoding issues <https://github.com/vertica/vertica-python#utf-8-encoding-issues>`_.
      * ``use_prepared_statements``: See `Passing parameters to SQL queries <https://github.com/vertica/vertica-python#passing-parameters-to-sql-queries>`_.
      * ``workload``: Sets the workload name associated with this session.

    See `vertica-python docs <https://github.com/vertica/vertica-python#usage>`_ for details.


      Example "extras" field:

      .. code-block:: json

         {
            "connection_load_balance": true,
            "log_level": "error",
            "ssl": true
         }

      or

      .. code-block:: json

         {
            "session_label": "airflow-session",
            "connection_timeout": 30,
            "backup_server_node": ["bck_server_1", "bck_server_2"]
         }
