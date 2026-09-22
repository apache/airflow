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

Files with DataFusion: ``DataFusionToolset``
============================================

Curated toolset wrapping
:class:`~airflow.providers.common.sql.datafusion.engine.DataFusionEngine`
with three tools — ``list_tables``, ``get_schema``, and ``query`` — for
querying files on object stores (S3, GCS, local filesystem, Iceberg) via Apache DataFusion.

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Tool
     - Description
   * - ``list_tables``
     - Lists registered table names
   * - ``get_schema``
     - Returns column names and types for a table (Arrow schema)
   * - ``query``
     - Executes a SQL query and returns bounded, columnar JSON (see
       :ref:`bounded-query-results`)

Each :class:`~airflow.providers.common.sql.config.DataSourceConfig` entry
registers a table backed by Parquet, CSV, Avro, or Iceberg data. Multiple
configs can be registered so that SQL queries can join across tables.

.. code-block:: python

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.sql.config import DataSourceConfig

    toolset = DataFusionToolset(
        datasource_configs=[
            DataSourceConfig(
                conn_id="aws_default",
                table_name="sales",
                uri="s3://my-bucket/data/sales/",
                format="parquet",
            ),
            DataSourceConfig(
                conn_id="aws_default",
                table_name="returns",
                uri="s3://my-bucket/data/returns/",
                format="csv",
            ),
        ],
        max_rows=100,
    )

The ``DataFusionEngine`` is created lazily on the first tool call. This
toolset requires the ``datafusion`` extra of
``apache-airflow-providers-common-sql``.

Parameters
----------

- ``datasource_configs``: One or more
  :class:`~airflow.providers.common.sql.config.DataSourceConfig` entries.
  Requires ``apache-airflow-providers-common-sql[datafusion]``.
- ``allow_writes``: Allow data-modifying SQL (CREATE TABLE, CREATE VIEW,
  INSERT INTO, etc.). Default ``False`` — only SELECT-family statements are
  permitted. DataFusion on object stores is mostly read-only, but it does
  support DDL for in-memory tables; this guard blocks those by default.
- ``max_rows``: Maximum rows returned from the ``query`` tool. Default ``50``.
- ``max_result_bytes``: Budget for the serialized ``query`` result. Default 64 KiB.
  See :ref:`bounded-query-results`.

When to choose it
-----------------

**Choose it when** the data is files on an object store rather than rows in a
database — Parquet, CSV or Avro — or a table in a catalog such as Iceberg, and
you want the agent to ask SQL questions of them without loading them anywhere
first. (This route needs the ``datafusion`` extra of
``apache-airflow-providers-common-sql``.) Each ``DataSourceConfig`` registers
one table, and several can be registered so the agent can join across them.
The two shapes take different fields: an object-store format needs a
``uri``, while a catalog format like
Iceberg is looked up by ``db_name`` instead, and ``DataSourceConfig`` raises
``ValueError`` at construction if a catalog format is missing one.

**What it cannot do**

- It has no table allow-list. ``allow_writes=False`` is the only guard, and it
  blocks non-SELECT statements, not reach: the defense-layer table records that
  this toolset "does not prevent the agent from reading any registered data
  source". The registration list is therefore the whole boundary — register
  exactly what the agent may read.
- It bounds the payload, not the scan. DataFusion has already materialized the
  full result before ``max_rows`` and ``max_result_bytes`` apply, so those limits
  protect the model's context, not the cost of the query.
- It cannot tell failure kinds apart precisely. The DataFusion Python bindings
  expose no native exception types, so the retry decision is made by matching the
  error message against regular expressions — which a wording change upstream can
  quietly defeat.

**A real example.** The same bucket as the ``HookToolset`` example on :doc:`hook`, reached
the other way. Rather than exposing ``list_keys`` and ``read_key`` and leaving
the agent to reassemble files, this registers the prefix as a table and lets it
write SQL:

.. code-block:: python

    from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
    from airflow.providers.common.sql.config import DataSourceConfig

    toolset = DataFusionToolset(
        datasource_configs=[
            DataSourceConfig(
                conn_id="aws_default",
                table_name="sales",
                uri="s3://my-bucket/data/sales/",
                format="parquet",
            ),
        ],
        max_rows=100,
    )

Which of the two fits depends on the question. "Read me this object" is a hook
method. "What were last quarter's returns by region" is a query, and expressing
it through ``list_keys`` and ``read_key`` means the model does the aggregation in
its context window instead of the engine doing it.

An Iceberg table is registered differently. (Iceberg support needs the
``apache.iceberg`` extra of ``apache-airflow-providers-common-sql``; without
it, registration raises ``AirflowOptionalProviderFeatureException``.) There is
no ``uri`` to read files from; the catalog resolves the table by name, so the
config carries a ``db_name`` instead, following the same ``DataSourceConfig``
shape that ``example_analytics.py`` in the ``common.sql`` provider uses:

.. code-block:: python

    toolset = DataFusionToolset(
        datasource_configs=[
            DataSourceConfig(
                conn_id="iceberg_default",
                table_name="users_data",
                db_name="demo",
                format="iceberg",
            ),
        ],
        max_rows=100,
    )

**Credentials and where it runs.** Each ``DataSourceConfig`` carries its own
``conn_id``, so object-store access is an Airflow connection. DataFusion is an
embedded engine: the query runs inside the worker process, not on a remote
cluster. Its tool calls act as barriers, as they do for the other routes that
build their own tools; see :ref:`toolset-call-barriers`.
