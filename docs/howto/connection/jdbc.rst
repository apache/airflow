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

JDBC connection
===============

The JDBC connection type enables connection to a JDBC data source.

Configuring the Connection
--------------------------

Host (required)
    The host to connect to.

Schema (required)
    Specify the database name to be used in.

Login (required)
    Specify the user name to connect to.

Password (required)
    Specify the password to connect to.

Port (optional)
    Port of host to connect to. Not user in ``JdbcOperator``.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in JDBC connection. The following parameters out of the standard python parameters are supported:

    * ``conn_prefix`` - Used to build the connection url in ``SparkJDBCOperator``, added in front of host (``conn_prefix`` ``host`` [: ``port`` ] / ``schema``)
    * ``extra__jdbc__drv_clsname`` - Full qualified Java class name of the JDBC driver. For ``JdbcOperator``.
    * ``extra__jdbc__drv_path`` - Jar filename or sequence of filenames for the JDBC driver libs. For ``JdbcOperator``.
