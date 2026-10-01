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

.. _howto/connection:redis:

Redis Connection
================

The Redis connection type enables connection to a Redis deployment, either a standalone
server or one running in cluster mode.

Default Connection IDs
----------------------

Redis Hook uses parameter ``redis_conn_id`` for Connection IDs and the value of the
parameter as ``redis_default`` by default.

Configuring the Connection
--------------------------
Host
    The host of the Redis server.

Port
    Specify the port to use for connecting the Redis server (Default is ``6379``).

Login
    The user that will be used for authentication against the Redis server (only applicable in Redis 6.0 and above).

Password
    The password of the user that will be used for authentication against the Redis server.

DB
    The DB number to use in the Redis server (Default is ``0``). Not supported in cluster mode.

Is cluster
    Whether Redis deployment is a cluster or a standalone instance (Default is ``False``).
    See :ref:`redis-cluster-deployment` below.

Startup nodes
    Extra bootstrap nodes as a comma-separated ``host:port`` list. The port may be omitted and
    defaults to ``6379``. Only for cluster Redis deployments (Default is ``None``).

Enable SSL
    Whether to enable SSL connection to the Redis server (Default is ``False``).

SSL verify mode
    Whether to try to verify other peers' certificates and how to behave if verification fails.
    For more information, see: `Python SSL docs <https://docs.python.org/3/library/ssl.html#ssl.SSLContext.verify_mode>`_.
    Allowed values are: ``required``, ``optional``, ``none``.

CA certificate path
    The path to a file of concatenated CA certificates in PEM format (Default is ``None``).

Private key path
    Path to an ssl private key (Default is ``None``).

Certificate path
    Path to an ssl certificate (Default is ``None``).

Enable hostname check
    If set, match the hostname during the SSL handshake (Default is ``False``).

.. _redis-cluster-deployment:

Redis Cluster Deployment
------------------------

Redis Cluster spreads the keyspace over 16384 hash slots owned by different masters, and expects
the client to route each command to the node owning that key's slot. A standalone client does not
do this: when it asks a node for a key that node does not serve, the node answers ``MOVED`` and
the standalone client fails.

Enable cluster mode to use a cluster-aware client that follows those redirects:

.. code-block:: json

    {
      "cluster": true,
      "startup_nodes": "node-2:6379,node-3:6379" // Connection extra
    }

The client discovers the full topology from the first node it reaches. ``startup_nodes`` matters for
bootstrap resilience: every task builds its own connection, so with a single seed node one unreachable
node breaks every task.
