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



.. _howto/connection:druid_ingest:

Apache Druid Ingest Connection
==============================

The Apache Druid connection type configures a connection to Apache Druid Overload
via the RestAPI to submit Indexing Jobs.


Default Connection IDs
----------------------

Druid hooks and operators use ``druid_ingest_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host of the Druid Overload server.

Port (optional)
    The port of the Druid Overload to connect to.

Login (optional)
    The username to connect the Druid Overload server.

Password (optional)
    The password to connect the Druid Overload server.

Schema (optional)
    The connection schema. Defaults to ``http``

Extra (optional)
     A JSON dictionary specifying the extra parameters that can be used in pydruid connection.

    * ``endpoint`` - endpoint to connect druid Overload (Batch Ingestion )
    * ``msq_endpoint`` - endpoint to connect druid Overload
