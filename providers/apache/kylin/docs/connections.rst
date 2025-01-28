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

Apache Kylin Connection
=======================

The Apache Kylin connection type enables connection to Apache Kylin.

Default Connection IDs
----------------------

Kylin Hook uses parameter ``kylin_conn_id`` for Connection IDs and the value of the
parameter as ``kylin_default`` by default.

Configuring the Connection
--------------------------
Host
    The host of the Kylin cluster (should be without scheme).

Port
    Specify the port to use for connecting the Kylin cluster.

Schema
    The default Kylin project that will be used, if not specified.

Login
    The user that will be used for authentication against the Kylin cluster.

Password
    The password of the user that will be used for authentication against the Kylin cluster.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in Kylin connection.
