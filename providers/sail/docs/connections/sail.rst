
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

.. _howto/connection:sail:

Sail Connection
===============

The Sail connection type enables connecting to a
`Sail <https://lakesail.com/>`__ server using the Spark Connect protocol.

Configuring the Connection
--------------------------

Host
    The hostname of the Sail server (e.g., ``sail-server.example.com``).
    You can also include the ``sc://`` scheme prefix.

Port
    The port of the Sail server. Default is ``50051``.

Login (User ID)
    Optional user identifier for the Spark Connect session.

Password (Token)
    Optional authentication token for the Spark Connect session.

Extra (optional)
    Specify extra parameters as JSON:

    * ``use_ssl``: Set to ``True`` to enable SSL for the connection.
    * Any additional key-value pairs are passed as Spark configuration
      properties to the ``SparkConf``.

Example Connection URI
-----------------------

.. code-block::

    sail://sail-user:secret-token@sail-host:50051?use_ssl=True
