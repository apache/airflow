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

.. _howto/redis-adbc:

Redis through ADBC
==================

The community-maintained `Redis ADBC driver <https://github.com/redis/redis-adbc-driver>`__
provides an external ``RedisAdbcHook`` for SQL queries and Arrow ingestion against
Redis with the Query Engine (Search). It is distinct from the Redis command hook
in ``apache-airflow-providers-redis`` and is not an Apache-maintained provider.

Installation
------------

Install a driver build containing ``adbc_driver_redis.airflow`` in every task
execution environment, alongside this Arrow provider. For packaged builds:

.. code-block:: bash

    python -m pip install 'adbc-driver-redis[airflow]'

An older release may not include the hook or the ``airflow`` extra. In that case,
use the driver's `source installation instructions <https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#install-and-discover-the-driver>`__
and install its ``airflow`` extra. Source installs require a separately built
native library. Redis must run separately with Search; installing the wheel does
not start a Redis server.

Connection configuration
------------------------

Create an Airflow connection named ``redis_analytics`` with type ``adbc`` and
these extras, substituting your worker-reachable Redis URI:

.. code-block:: json

    {
      "dialect": "redis",
      "db_kwargs": {"uri": "redis://localhost:6379/0"}
    }

This configuration applies to the explicitly imported Redis hook. Keep credentials
in the Airflow connection or secrets backend. Use ``rediss://`` for verified TLS.
The hook resolves the driver's bundled native library, including macOS libraries,
and honors ``REDIS_ADBC_LIBRARY`` for source development.

Query inside a Python task
--------------------------

.. code-block:: python

    from adbc_driver_redis.airflow import RedisAdbcHook


    def redis_answer():
        hook = RedisAdbcHook(adbc_conn_id="redis_analytics")
        return hook.get_records("SELECT $1 AS answer", parameters=(42,))

Call this function from a Python task in your Dag. A constant SELECT still
requires Redis with Search. Query tables created, ingested or adopted through
the driver; arbitrary Redis keys do not automatically appear as SQL tables.

Autocommit and provider selection
---------------------------------

The Redis ADBC driver supports autocommit only. ``RedisAdbcHook`` opens connections
in autocommit mode and defaults ``run()``, ``get_records()`` and ``get_first()`` to it.
Explicit ``autocommit=False`` is rejected. Pass ``autocommit=True`` to inherited
``insert_rows()``. Failed or retried writes can leave partial writes or duplicates;
rollback cannot undo them.

The external hook is not registered as an Airflow provider. An ``adbc`` connection
used by ``SQLExecuteQueryOperator`` still selects the generic Arrow hook. In Arrow
provider 0.1.1, its ``get_records()`` requests transaction mode and fails with this
driver. Import and call the Redis hook explicitly inside your Python task.

The driver's `hook validation record <https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/airflow-get-records/validation.md>`__
records local checks with Airflow 3.3.2 and Arrow provider 0.1.1. Remote executors,
Airflow 2.x and a live hook TLS handshake are outside those recorded results.
