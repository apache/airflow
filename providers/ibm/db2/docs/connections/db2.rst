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

.. _howto/connection:ibmdb2:

IBM Db2 Connection
==================

The IBM Db2 connection type enables connection to IBM Db2 databases.

Configuring the Connection
---------------------------

Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Port (optional)
    Port of the Db2 database. Default is 50000.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the
    Db2 connection. The following parameters are supported:

    * ``database`` - The database name to connect to.
    * ``protocol`` - The protocol to use (default: TCPIP).
    * ``security`` - Security protocol (e.g., SSL).
    * Any other parameter supported by the IBM Db2 driver.

    Example "extras" field:

    .. code-block:: json

       {
          "database": "sample",
          "protocol": "TCPIP",
          "security": "SSL"
       }
