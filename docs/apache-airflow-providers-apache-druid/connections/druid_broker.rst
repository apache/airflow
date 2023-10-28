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



.. _howto/connection:druid_broker:

Apache Druid Broker Connection
==============================

The Apache Druid connection type configures a connection to Apache Druid Broker via the ``pydruid`` DBAPI Python package.


Default Connection IDs
----------------------

Druid hooks and operators use ``druid_broker_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host of the Druid broker server.

Port (optional)
    The port of the Druid broker to connect to.

Extra (optional)
     A JSON dictionary specifying the extra parameters that can be used in pydruid connection.

    * ``endpoint`` - endpoint to connect druid broker. Defaults to ``/druid/v2/sql``
    * ``schema`` - connection schema (http or https). Defaults to ``http``
    * ``header`` - to get header or not. Defaults to ``False``
    * ``ssl_verify_cert`` - verify ssl certificate or not. Defaults to ``True``
    * ``ssl_client_cert`` - ssl client certificate
    * ``proxies`` - map of proxies schema to proxy server. e.g ``{'http': 'localhost:9050'}``
