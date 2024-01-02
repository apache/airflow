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



.. _howto/connection:spark-connect:

Apache Spark Connect Connection
===============================

The Apache Spark Connect connection type enables connection to Apache Spark via the Spark connect interface.

Default Connection IDs
----------------------

The Spark Connect hook uses ``spark_connect_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to, should be a valid hostname.

Port (optional)
    Specify the port in case of host be an URL.

User ID (optional, only applies to Spark Connect)
    The user ID to authenticate with the proxy.

Token (optional, only applies to Spark Connect)
    The token to authenticate with the proxy.

Use SSL (optional, only applies to Spark Connect)
    Whether to use SSL when connecting.

.. warning::

  Make sure you trust your users with the ability to configure the host settings as it may enable the connection to
  establish communication with external servers. It's crucial to understand that directing the connection towards a
  malicious server can lead to significant security vulnerabilities, including the risk of encountering
  Remote Code Execution (RCE) attacks.
