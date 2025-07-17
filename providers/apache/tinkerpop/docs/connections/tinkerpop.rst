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



.. _howto/connection:tinkerpop:

Gremlin Connection
====================

The Gremlin connection type enables integrations with Gremlin Server.

Authenticating to Gremlin
---------------------------

Authenticate to Gremlin using the `Germlin python client default authentication
<https://tinkerpop.apache.org/docs/current/reference/#gremlin-python-configuration>`_.

Default Connection IDs
----------------------

Hooks and operators related to Gremlin use ``gremlin_default`` by default.

Configuring the Connection
--------------------------

Use the ``gremlin_conn_id`` argument to connect to your Gremlin instance where
the connection metadata is structured as follows:

.. list-table:: Gremlin Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Gremlin hostname
   * - Port: int
     - Gremlin port
   * - Username: string
     - Gremlin user
   * - Password: string
     - Gremlin user password
   * - Scheme: string
     - Gremlin Scheme
   * - Schema: string
     - Gremlin Schema

URI format example
^^^^^^^^^^^^^^^^^^

If serializing with Airflow URI:

.. code-block:: bash

  export AIRFLOW_CONN_GREMLIN_DEFAULT='gremlin://username:password@host:port/schema'

Note that all components of the URI should be URL-encoded.

JSON format example
^^^^^^^^^^^^^^^^^^^

If serializing with JSON:

.. code-block:: bash

  export AIRFLOW_CONN_GREMLIN_DEFAULT='{
      "conn_type": "gremlin",
      "login": "username",
      "host": "host",
      "password": "password",
      "schema": "schema",
      "port": 433
  }'
