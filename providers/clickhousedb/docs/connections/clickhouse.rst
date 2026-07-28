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



.. _howto/connection:clickhouse:

ClickHouse Connection
=====================

The ClickHouse connection type enables integrations with
`ClickHouse <https://clickhouse.com/>`__ via the HTTP interface (port 8123 by default).
It uses the `clickhouse-connect <https://clickhouse.com/docs/en/integrations/python>`__ library
which provides a DB-API 2.0 compliant interface.

Authenticating to ClickHouse
-----------------------------

Authenticate using a username and password.  For ClickHouse Cloud and self-hosted clusters
with TLS enabled, set ``secure=True`` in the **Extra** field, and make sure to use the relevant port.

Default Connection IDs
----------------------

All hooks and operators related to ClickHouse use ``clickhouse_default`` by default.

Configuring the Connection
--------------------------

Host
    Hostname or IP address of the ClickHouse server (e.g. ``localhost`` or
    ``abc123.clickhouse.cloud``).

Port
    HTTP(S) port.  Defaults to ``8123`` (plain) or ``8443`` (TLS).

Login
    ClickHouse username.  Defaults to ``default``.

Password
    ClickHouse user password.  Leave blank if the user has no password.

Database (Schema field)
    The default database for the connection.  Defaults to ``default``.


Extra (optional)
    Specify additional parameters as a JSON dictionary.  All keys are optional:

    * ``secure`` *(bool, default: ``false``)*: Use HTTPS/TLS.
    * ``verify`` *(bool, default: ``true``)*: Verify the server TLS certificate when
      ``secure`` is ``true``.  Set to ``false`` for self-signed certificates.
    * ``connect_timeout`` *(int, default: ``10``)*: HTTP connection timeout in seconds.
    * ``send_receive_timeout`` *(int, default: ``300``)*: Query read/write timeout in seconds.
      Increase this for long-running analytical queries.
    * ``compress`` *(bool, default: ``true``)*: Enable LZ4 query result compression.
    * ``client_name`` *(str)*: Optional label appended to the Airflow version identifier in the
      ClickHouse ``User-Agent`` and ``system.query_log`` ``client_name`` field.  The hook always
      prepends ``apache-airflow/<version> apache-airflow-providers-clickhousedb/<version>``; this
      field adds a human-readable suffix, e.g. ``"my-pipeline"``.
    * ``session_settings`` *(dict)*: ClickHouse session-level settings applied to every
      query on this connection.  These are passed directly to the ``settings`` parameter of
      ``clickhouse_connect.get_client()``.  Common examples:

      .. code-block:: json

          {
              "max_execution_time": 300,
              "max_memory_usage": 10000000000,
              "max_threads": 8,
              "readonly": 1
          }

      Settings set here act as **defaults** — any ``session_settings`` passed to the
      :class:`~airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook` constructor
      are **merged on top** and take precedence on conflicting keys.

      For a full list of available settings see the
      `ClickHouse session settings reference <https://clickhouse.com/docs/operations/settings/settings>`__.

URI format example
^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    export AIRFLOW_CONN_CLICKHOUSE_DEFAULT='clickhouse://user:password@localhost:8123/my_database'

Note that all components of the URI should be URL-encoded.
For TLS, timeouts, or session settings use the JSON format below.

JSON format example
^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    export AIRFLOW_CONN_CLICKHOUSE_DEFAULT='{
        "conn_type": "clickhouse",
        "host": "localhost",
        "port": 8123,
        "login": "default",
        "password": "secret",
        "schema": "my_database",
        "extra": {
            "secure": false,
            "verify": true,
            "connect_timeout": 10,
            "send_receive_timeout": 300,
            "compress": true,
            "client_name": "airflow",
            "session_settings": {
                "max_execution_time": 300,
                "max_memory_usage": 10000000000
            }
        }
    }'
