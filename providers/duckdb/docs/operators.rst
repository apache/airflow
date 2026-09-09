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

DuckDB Operators
================

`DuckDB <https://duckdb.org/>`__ is an in-process analytical database. Queries run inside the
Airflow task process, so there is no cluster to provision and no data to move before querying it.

.. _howto/operator:DuckDBExecuteQueryOperator:

Run a query
-----------

Use :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` to run SQL
against an in-process DuckDB database.

Unlike the other SQL operators this one does not require an Airflow connection to exist. With none
configured it runs against an in-memory database, which is the common case for a task that reads its
input, transforms it and writes its output back out:

.. code-block:: python

    aggregate = DuckDBExecuteQueryOperator(
        task_id="aggregate",
        sql="""
            COPY (
                SELECT category, SUM(price * quantity) AS revenue
                FROM read_parquet('/opt/airflow/data/sales/*.parquet')
                GROUP BY category
            ) TO '/opt/airflow/data/summary/revenue.parquet' (FORMAT PARQUET)
        """,
        hook_params={"memory_limit": "2GB", "threads": 4},
    )

Querying cloud object storage
-----------------------------

DuckDB can read and write object storage directly, which needs the ``httpfs`` extension and
credentials for the store:

.. code-block:: python

    hook_params = {"extensions": ["httpfs"]}

Supplying those credentials is backend-specific and this provider does not do it for you: DuckDB does
not use the cloud SDKs, so it needs its own secret rather than the Airflow connection. Use the hook
from the provider for your storage backend, which builds that secret from the credentials Airflow
already holds, or issue ``CREATE SECRET`` yourself.

Configuring the engine
----------------------

Anything :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook` accepts can be passed through
``hook_params``, or set once on the connection so every task inherits it. Prefer setting
``memory_limit`` and ``threads`` explicitly: DuckDB otherwise sizes itself from the resources it
detects on the host, which over-commits inside a container.

Using the hook directly
-----------------------

For work that is not a single statement — registering a dataframe, chaining several queries against
one database, or returning Arrow — use the hook:

.. code-block:: python

    @task
    def summarize():
        hook = DuckDBHook()
        with hook.get_conn() as conn:
            conn.execute("CREATE TABLE sales AS SELECT * FROM read_parquet('/opt/airflow/data/sales.parquet')")
            return conn.execute("SELECT category, SUM(revenue) FROM sales GROUP BY category").fetchall()

Opening one connection for several statements also avoids paying connection setup and extension
loading more than once per task.
