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

Apache HDFS Connection
======================

The Apache HDFS connection type enables connection to Apache HDFS.

Default Connection IDs
----------------------

Web HDFS Hook uses parameter ``webhdfs_conn_id`` for Connection IDs and the value of the
parameter as ``webhdfs_default`` by default.

Configuring the Connection
--------------------------
Host
    The host to connect to, it can be ``local``, ``yarn`` or an URL. For Web HDFS Hook it is possible to specify multiple hosts as a comma-separated list.

Port
    Specify the port in case of host be an URL.

Login
    Effective user for HDFS operations (non-Kerberized).

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in Web HDFS connection.
    The following extra parameters can be used to configure SSL for Web HDFS Hook:

    * ``use_ssl`` - If SSL should be used. By default is set to `false`.
    * ``verify`` - How to verify SSL. For more information refer to https://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification.
    * ``cert`` - Client certificate path for mTLS, can be combined cert or used with ``key``
    * ``key`` - Client key path for mTLS with ``cert``
