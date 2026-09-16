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

Schema (required)
    The Db2 database name to connect to. Maps to the ``DATABASE`` keyword in
    the Db2 connection string.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Port (optional)
    Port of the Db2 database. Default is ``50000``.

Extra (optional)
    Specify extra parameters (as a JSON dictionary) that are appended verbatim
    to the Db2 connection string. Parameter names are converted to uppercase
    automatically.

    .. note::

        Do **not** put ``database`` or ``protocol`` here — ``database`` is
        taken from the **Schema** field above, and ``protocol`` is always set
        to ``TCPIP`` by the hook. Values placed in extras for these keys will
        be appended as duplicates and ignored by the driver.

    Common parameters:

    * ``SECURITY`` - Enable SSL (set to ``"SSL"``).
    * ``SSLServerCertificate`` - Path to the server SSL certificate.
    * Any other parameter supported by the IBM Db2 driver.

    Example "extras" field for SSL:

    .. code-block:: json

       {
          "SECURITY": "SSL",
          "SSLServerCertificate": "/path/to/server.crt"
       }
