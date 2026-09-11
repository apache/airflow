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

.. _howto/connection:druid:

Apache Druid Connection
=======================

The Apache Druid connection type enables connection to a Druid broker, in order to query data.
It is used by :class:`~airflow.providers.apache.druid.hooks.druid.DruidDbApiHook`.

To submit ingestion tasks to a Druid overlord instead, use the
:ref:`Apache Druid Ingest connection <howto/connection:druid_ingest>`.

Default Connection IDs
----------------------

The Druid broker hook uses the parameter ``druid_broker_conn_id`` for Connection IDs and the value of the
parameter as ``druid_broker_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host of the Druid broker, without a scheme, for example ``druid-broker``.

Port (required)
    The port the Druid broker listens on, ``8082`` by default.

Login (optional)
    The username used for basic authentication, when the cluster is secured with the
    ``druid-basic-security`` extension.

Password (optional)
    The password used for basic authentication.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in the Druid connection.
    The following parameters are supported:

    * ``endpoint`` - Path of the SQL endpoint of the broker. Defaults to ``/druid/v2/sql``.
    * ``schema`` - The URL scheme used to reach the broker, either ``http`` or ``https``.
      Defaults to ``http``. This is an extra parameter, not the connection's own Schema
      field, which this hook does not read.
    * ``ssl_verify_cert`` - Whether the broker TLS certificate is verified. Defaults to ``true``.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DRUID_BROKER_DEFAULT='druid://druid-broker:8082/?endpoint=druid%2Fv2%2Fsql'
