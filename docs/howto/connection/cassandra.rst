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



Apache Cassandra Connection
===========================

The Apache Cassandra connection type enables connection to `Apache Cassandra <https://cassandra.apache.org/>`__.

Default Connection IDs
----------------------

Cassandra hook and Cassandra operators use ``cassandra_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to. It is possible to specify multiple hosts as a comma-separated list.

Schema (required)
    The schema (keyspace) name to be used in the database.

Login (required)
    The user name to connect.

Password (required)
    The password to connect.

Port (required)
    The port to connect.

Extra (optional)
    The extra parameters (as json dictionary) that can be used in cassandra
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``load_balancing_policy`` - This parameter specifies the load balancing policy to be used. There are four available policies:
      ``RoundRobinPolicy``, ``DCAwareRoundRobinPolicy``, ``WhiteListRoundRobinPolicy`` and ``TokenAwarePolicy``. ``RoundRobinPolicy`` is the default load balancing policy.
    * ``load_balancing_policy_args`` - This parameter specifies the arguments for the load balancing policy being used.
    * ``cql_version`` - This parameter specifies the CQL version of cassandra.
    * ``protocol_version`` - This parameter specifies the maximum version of the native protocol to use.
    * ``ssl_options`` - This parameter specifies the details related to SSL, if it's enabled in Cassandra.


Examples for the **Extra** field
--------------------------------

1. Specifying ``ssl_options``

If SSL is enabled in Cassandra, pass in a dict in the extra field as kwargs for ``ssl.wrap_socket()``. For example:

.. code-block:: JSON

    {
      "ssl_options" : {
        "ca_certs" : "PATH/TO/CA_CERTS"
      }
    }

2. Specifying ``load_balancing_policy`` and ``load_balancing_policy_args``

Default load balancing policy is ``RoundRobinPolicy``. Following are a few samples to specify a different LB policy:

DCAwareRoundRobinPolicy:

.. code-block:: JSON

    {
      "load_balancing_policy": "DCAwareRoundRobinPolicy",
      "load_balancing_policy_args": {
        "local_dc": "LOCAL_DC_NAME",
        "used_hosts_per_remote_dc": "SOME_INT_VALUE"
      }
    }

WhiteListRoundRobinPolicy:

.. code-block:: JSON

    {
      "load_balancing_policy": "WhiteListRoundRobinPolicy",
      "load_balancing_policy_args": {
        "hosts": ["HOST1", "HOST2", "HOST3"]
      }
    }

TokenAwarePolicy:

.. code-block:: JSON

    {
      "load_balancing_policy": "TokenAwarePolicy",
      "load_balancing_policy_args": {
        "child_load_balancing_policy": "CHILD_POLICY_NAME",
        "child_load_balancing_policy_args": {}
      }
    }

.. seealso::
    https://pypi.org/project/cassandra-driver/
