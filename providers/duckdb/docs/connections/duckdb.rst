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

    This connection is optional. With no ``duckdb_default`` connection configured the hook opens an
    in-memory database. Configure a connection when you need a persistent database file, a MotherDuck
    database, or shared engine settings.

    Only the default connection id is optional. Passing any other id asserts that the connection
    exists, a missing connection raises rather than silently opening an in-memory database.

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

    ``extension_directory`` *(string, optional)*
        Directory DuckDB loads extensions from, and installs them into when downloads are enabled.

    ``autoinstall_extensions`` *(bool, optional)*
        Whether an extension that is not installed locally may be downloaded from DuckDB's extension
        repository. Defaults to ``False``.

    ``autoload_extensions`` *(bool, optional)*
        Whether DuckDB may load an already-installed extension implicitly, so that querying an
        ``s3://`` path, for example, pulls in ``httpfs`` without listing it in ``extensions``.
        Defaults to ``True``.

    ``memory_limit`` *(string, optional)*
        Memory DuckDB may use, for example ``"2GB"``.

    ``threads`` *(int, optional)*
        Number of threads DuckDB may use.

    ``temp_directory`` *(string, optional)*
        Directory DuckDB spills to when a query exceeds ``memory_limit``.

    ``settings`` *(object, optional)*
        Additional DuckDB configuration options, passed through verbatim.

.. warning:: **Extension downloads are off by default**

    DuckDB fetches extensions from its extension repository the first time they are used. That is not
    possible in an environment without outbound internet access, and where it is possible it costs
    every worker the download, so ``autoinstall_extensions`` defaults to ``False`` and a missing
    extension fails with a clear error rather than reaching the network.

    Pre-populate an extension directory and point ``extension_directory`` at it, or set
    ``autoinstall_extensions=True`` if downloading on demand is acceptable. Loading an extension that
    is already present needs neither setting.

.. warning:: **Installing an extension needs a home directory**

    DuckDB installs extensions under the Airflow user's home directory, in
    ``~/.duckdb/extensions/<duckdb_version>/<platform>/``, so installing one requires the
    environment to provide a home directory that exists and is writable. When ``HOME`` is unset,
    empty, or points at a directory that does not exist, the install fails with
    ``IO Error: Can't find the home directory``.

    Providing a writable home directory is part of configuring the environment, and is the
    deployment administrator's responsibility.

    Setting ``extension_directory`` is not a reliable substitute. On DuckDB 1.5.0 and later an
    install still resolves the home directory when ``HOME`` is unset or empty, even with
    ``extension_directory`` set, so it fails anyway. What does work on every supported DuckDB
    version is not installing at task runtime at all: pre-populate ``extension_directory`` and set
    ``autoinstall_extensions=False``. Loading an extension that is already present needs no home
    directory.

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
