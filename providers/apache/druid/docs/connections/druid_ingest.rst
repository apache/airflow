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

The Apache Druid Ingest connection type enables connection to a Druid overlord, which accepts
indexing (ingestion) tasks. It is used by :class:`~airflow.providers.apache.druid.hooks.druid.DruidHook`
and by the operators built on top of it, such as
:class:`~airflow.providers.apache.druid.operators.druid.DruidOperator`.

To query a Druid broker instead, use the :ref:`Apache Druid connection <howto/connection:druid>`.

Default Connection IDs
----------------------

The Druid Ingest hook, ``DruidOperator`` and ``HiveToDruidOperator`` use the parameter
``druid_ingest_conn_id`` for Connection IDs and the value of the parameter as
``druid_ingest_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host of the Druid overlord, without a scheme, for example ``druid-overlord``.

Port (required)
    The port the Druid overlord listens on, ``8081`` by default.

Schema (optional)
    The URL scheme used to reach the overlord, either ``http`` or ``https``. Defaults to ``http``.

Login (optional)
    The username used for basic authentication, when the cluster is secured with the
    ``druid-basic-security`` extension.

Password (optional)
    The password used for basic authentication. Set Login and Password together: if either one
    is empty the hook sends its requests without authentication.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in the Druid Ingest connection.
    The following parameters are supported:

    * ``endpoint`` - Path of the endpoint that accepts native batch ingestion tasks, for example
      ``druid/indexer/v1/task``.
    * ``msq_endpoint`` - Path of the endpoint that accepts SQL-based (MSQ) ingestion tasks, for example
      ``druid/v2/sql/task``.
    * ``status_endpoint`` - Path of the endpoint used to poll the status of an SQL-based ingestion task.
      Defaults to ``druid/indexer/v1/task``.
    * ``ca_bundle_path`` - Path to a CA bundle used to verify the connection. Only used when the hook is
      created with ``verify_ssl=False``.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DRUID_INGEST_DEFAULT='druid-ingest://druid-overlord:8081/?endpoint=druid%2Findexer%2Fv1%2Ftask'
