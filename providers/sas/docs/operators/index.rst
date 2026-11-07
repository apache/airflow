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

Using the SAS hook
==================

:class:`~airflow.providers.sas.hooks.sas.SASHook` is a
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`, so the common-sql operators and the
transfer operators that accept a SQL connection work with it unchanged. The provider has no
operators of its own.

Each statement is one SAS run: the hook uploads ``PROC SQL`` as a SAS program, runs it, and
downloads the whole result as one ``.sas7bdat`` file. Prefer one query that returns what you
need over many small ones.

Running a query
---------------

.. code-block:: python

    import datetime

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    count_orders = SQLExecuteQueryOperator(
        task_id="count_orders",
        conn_id="sas_default",
        sql="SELECT COUNT(*) AS n FROM sales.orders WHERE order_date >= %s",
        parameters=(datetime.date(2026, 1, 1),),
    )

SAS has no client-side parameter binding. Bind parameters (``%s`` or ``%(name)s``) are written
into the program as SAS literals: strings single-quoted, ``datetime.date`` as ``'01JAN2026'd``,
``datetime.datetime`` as ``'01JAN2026:00:00:00'dt``. ``None`` has no SAS literal and raises;
use ``IS MISSING`` in the SQL instead.

Typed DataFrames and Parquet
----------------------------

``get_df()`` types each column from the metadata in the ``.sas7bdat`` file, not from its
values: character columns become strings, numeric columns floats, ``DATE`` formats ``date32`` and
``DATETIME`` formats microsecond timestamps. With ``dtype_backend="pyarrow"`` the frame is
Arrow-backed end to end, so writing it to Parquet keeps the same schema for every run and every
chunk:

.. code-block:: python

    from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

    orders_to_s3 = SqlToS3Operator(
        task_id="orders_to_s3",
        sql_conn_id="sas_default",
        query="SELECT * FROM sales.orders",
        s3_bucket="my-bucket",
        s3_key="raw/sales/orders.parquet",
        file_format="parquet",
        replace=True,
        read_kwargs={"dtype_backend": "pyarrow"},
    )

``get_df_by_chunks(sql, chunksize=N)`` runs SAS once and parses the result while it is read,
so only one chunk is held in memory. ``get_df(sql, limit=N)`` (``PROC SQL OUTOBS=``) caps the
rows on the server, which is handy for smoke tests.

Transferring a table
--------------------

SAS SQL has no ``LIMIT`` / ``OFFSET``. When ``GenericTransfer`` pages a query with its default
clause, the hook does not send the clause to SAS: the first page starts one SAS run of the
query and every following page is the next chunk of that run, so a paged transfer costs one
SAS run and one download.

.. code-block:: python

    from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer

    copy_orders = GenericTransfer(
        task_id="copy_orders",
        source_conn_id="sas_default",
        destination_conn_id="postgres_default",
        sql="SELECT * FROM sales.orders",
        destination_table="staging.orders",
        page_size=10_000,
    )

Leave ``paginated_sql_statement_clause`` unset, and do not use deferrable mode: the trigger pages
from a new hook each time, which starts a SAS run per page.

Async tasks
-----------

``aget_records``, ``aget_df`` and ``aget_df_by_chunks`` are the async counterparts, on
``asyncssh``. They read the connection and its extra without synchronous SDK calls, so they are
safe in async tasks that run several calls on the same event loop. ``aclose()`` releases the SSH
session.
