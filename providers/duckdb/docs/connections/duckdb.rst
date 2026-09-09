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

.. _howto/connection:duckdb:

DuckDB connection
=================

The DuckDB connection describes which in-process database
:class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook` opens and how the engine is configured.

.. note::

    This connection is optional. With no connection configured the hook opens an in-memory database.
    Configure a connection when you need a persistent database file, a MotherDuck database, or shared
    engine settings.

Default Connection ID
---------------------

``duckdb_default``

Configuring the Connection
--------------------------

Database path (Host field)
    Path to a DuckDB database file, for example ``/tmp/analytics.duckdb``. Leave empty for an
    in-memory database.

MotherDuck database (Schema field)
    Name of the `MotherDuck <https://motherduck.com/>`__ database to attach. Only used when a
    MotherDuck token is supplied.

MotherDuck token (Password field)
    MotherDuck service token. When set, the hook opens ``md:<database>`` instead of a local file.

Extra (JSON)
    A JSON object with the following recognized keys:

    ``database`` *(string, optional)*
        Database to open. Takes precedence over the Host and Schema fields. Useful when the value is
        neither a plain path nor a MotherDuck database.

    ``extensions`` *(list of strings, optional)*
        Extensions to load on connect, for example ``["httpfs", "iceberg"]``.

    ``memory_limit`` *(string, optional)*
        Memory DuckDB may use, for example ``"2GB"``.

    ``threads`` *(int, optional)*
        Number of threads DuckDB may use.

    ``temp_directory`` *(string, optional)*
        Directory DuckDB spills to when a query exceeds ``memory_limit``.

    ``settings`` *(object, optional)*
        Additional DuckDB configuration options, passed through verbatim.

.. warning:: **Extension loading reaches the network**

    DuckDB downloads extensions from its extension repository the first time they are used. In an
    environment without outbound internet access, pre-populate an extension directory, point
    ``extension_directory`` at it and set ``autoinstall_extensions=False`` so a missing extension
    fails with a clear error instead of hanging on a network call. Doing this also removes the
    per-task download latency where egress *is* available.

.. warning:: **Extensions are native code**

    A DuckDB extension is a shared library loaded into the task process. Community extensions come
    from outside the DuckDB project, so the hook refuses to load them unless
    ``allow_community_extensions`` is set. Anyone who can edit this connection or author a Dag that
    uses it chooses which extensions get loaded.

Examples
--------

In-memory database with S3 support:

.. code-block:: json

    {
      "extensions": ["httpfs"]
    }

Persistent database with explicit resource limits:

.. code-block:: json

    {
      "database": "/opt/airflow/data/analytics.duckdb",
      "memory_limit": "4GB",
      "threads": 4,
      "temp_directory": "/opt/airflow/spill"
    }
