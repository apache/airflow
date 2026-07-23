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

.. _howto/connection:adbc:

ADBC connection
===============

The ADBC connection type enables connection to any database that has an
`Arrow Database Connectivity (ADBC) <https://arrow.apache.org/adbc/>`__ driver,
such as PostgreSQL, SQLite, DuckDB, Snowflake, BigQuery, and Flight SQL servers.

Connections of this type are used by :class:`~airflow.providers.apache.arrow.hooks.adbc.AdbcHook`,
which builds on top of :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` and transfers
data as Apache Arrow :class:`~pyarrow.RecordBatch` objects for efficient, zero-copy bulk loads.

Default Connection ID
---------------------

``adbc_default``

Configuring the Connection
--------------------------

Connection URL (Host field)
    The database URI passed to the driver.  For drivers that use a standard
    connection string (e.g. ``postgresql://user:pass@host:5432/db`` or
    ``file::memory:``), put it here.  If the value contains ``::`` the hook
    passes it through unchanged; otherwise the ``adbc://`` scheme prefix is
    replaced with the dialect name.

Login / Password
    Convenience fields.  When set, Airflow builds a URI of the form
    ``<dialect>://login:password@host/schema`` that is merged into
    ``db_kwargs["uri"]``.  Driver-specific URIs in the Host field take
    precedence.

Extra (JSON)
    A JSON object with the following recognized keys:

    ``driver`` *(string)*
        Python package name of the ADBC driver to load, e.g.
        ``"adbc_driver_postgresql"`` or ``"adbc_driver_sqlite"``.
        When omitted the hook derives the name from the dialect as
        ``adbc_driver_<dialect>``.

    ``entrypoint`` *(string, optional)*
        Fully-qualified Python symbol used as the driver entrypoint, e.g.
        ``"adbc_driver_sqlite.dbapi.connect"``.  Most drivers do not need
        this; it is only required when the automatic entrypoint discovery
        built into ``adbc_driver_manager`` cannot locate the function.

    ``dialect`` *(string, optional)*
        SQL dialect name used to build the URI scheme and derive the
        default driver name.  Defaults to ``"default"``.

    ``db_kwargs`` *(object, optional)*
        Keyword arguments forwarded verbatim to the ADBC
        ``AdbcDatabase`` constructor (database-level connection settings).
        ``uri`` is always injected automatically from the Host field and
        must not be repeated here.  Common keys:

        .. list-table::
           :header-rows: 1
           :widths: 30 15 55

           * - Key
             - Type
             - Description
           * - ``username``
             - string
             - Database user name (alternative to encoding it in the URI).
           * - ``password``
             - string
             - Database password (alternative to encoding it in the URI).
           * - ``adbc.connection.autocommit``
             - bool
             - Enable autocommit at the database level (driver-dependent).
           * - ``adbc.sqlite.query.batch_rows``
             - int
             - SQLite: number of rows fetched per Arrow batch.

        All other keys are driver-specific.  Consult the documentation for
        your ADBC driver for the full list.  See also the
        `ADBC driver documentation <https://arrow.apache.org/adbc/current/driver/>`__.

    ``conn_kwargs`` *(object, optional)*
        Keyword arguments forwarded verbatim to the ADBC
        ``AdbcConnection`` constructor (connection-level settings within the
        already-open database).  Common keys defined by the ADBC
        specification:

        .. list-table::
           :header-rows: 1
           :widths: 30 15 55

           * - Key
             - Type
             - Description
           * - ``autocommit``
             - bool
             - Enable autocommit for this connection.
           * - ``read_only``
             - bool
             - Open the connection in read-only mode.
           * - ``current_catalog``
             - string
             - Default catalog for unqualified table names.
           * - ``current_db_schema``
             - string
             - Default schema for unqualified table names.

        All other keys are driver-specific.

.. warning:: **Security — blast radius of db_kwargs / conn_kwargs**

    Both ``db_kwargs`` and ``conn_kwargs`` are passed through to the ADBC
    driver **without filtering**.  Any user or role that can edit this
    connection in the Airflow UI or via the Connections API can therefore
    supply arbitrary driver-level options.  Depending on the driver, this
    may include options that:

    * load extensions from arbitrary file-system paths (e.g. SQLite's
      ``load_extension`` or DuckDB's extension loading);
    * configure network endpoints or proxy addresses;
    * change authentication parameters.

    Restrict access to the Connections UI and API (Airflow RBAC ``Connections``
    resource) to trusted operators only.  Do not expose connection-editing
    permissions to untrusted Dag authors.

    Similarly, the ``driver`` and ``entrypoint`` extras load shared libraries
    or Python symbols by name.  Treat them with the same level of trust as
    executable code.

Examples
--------

SQLite in-memory database:

.. code-block:: json

    {
      "driver": "adbc_driver_sqlite"
    }

PostgreSQL with extra driver options:

.. code-block:: json

    {
      "driver": "adbc_driver_postgresql",
      "dialect": "postgresql",
      "db_kwargs": {
        "adbc.postgresql.query.batch_rows": 1000
      },
      "conn_kwargs": {
        "autocommit": true,
        "read_only": false
      }
    }

Usage
-----

Use :class:`~airflow.providers.apache.arrow.hooks.adbc.AdbcHook` in a task to query or load data:

.. code-block:: python

    from airflow.providers.apache.arrow.hooks.adbc import AdbcHook


    def load_data():
        hook = AdbcHook(adbc_conn_id="my_adbc_conn")

        # Run a query and get results as a list of tuples
        records = hook.get_records("SELECT id, name FROM users WHERE active = 1")

        # Bulk-insert rows in chunks of 5 000 using Arrow-native transfer
        rows = [(1, "Alice"), (2, "Bob")]
        hook.insert_rows(
            table="users",
            rows=rows,
            target_fields=["id", "name"],
            commit_every=5000,
        )
