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



.. _howto/connection:drill:

Apache Drill Connection
=======================

The Apache Drill connection type configures a connection to Apache Drill via the sqlalchemy-drill Python package.

Default Connection IDs
----------------------

Drill hooks and operators use ``drill_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host of the Drillbit to connect to (HTTP, JDBC) or the DSN of the Drill ODBC connection.

Port (optional)
    The port of the Drillbit to connect to.

Extra (optional)
     A JSON dictionary specifying the extra parameters that can be used in sqlalchemy-drill connection.

    * ``dialect_driver`` - The dialect and driver as understood by sqlalchemy-drill.  Defaults to ``drill_sadrill`` (HTTP).
    * ``storage_plugin`` - The default Drill storage plugin for this connection.  Defaults to ``dfs``.
