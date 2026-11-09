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

Object Storage XCom Backend
===========================

The default XCom backend is the :class:`~airflow.sdk.BaseXCom` class, which stores XComs in the Airflow database. This is fine for small values, but can be problematic for large values, or for large numbers of XComs.

To enable storing XComs in an object store, you can set the ``xcom_backend`` configuration option to ``airflow.providers.common.io.xcom.backend.XComObjectStorageBackend``. You will also need to set ``xcom_objectstorage_path`` to the desired location. The connection
id is obtained from the user part of the url that you will provide, e.g. ``xcom_objectstorage_path = s3://conn_id@mybucket/key``. Furthermore, ``xcom_objectstorage_threshold`` is required
to be something larger than -1. Any object smaller than the threshold in bytes will be stored in the database and anything larger will be
put in object storage. This will allow a hybrid setup. If an xcom is stored on object storage a reference will be
saved in the database. Finally, you can set ``xcom_objectstorage_compression`` to fsspec supported compression methods like ``zip`` or ``snappy`` to
compress the data before storing it in object storage.

So for example the following configuration will store anything above 1MB in S3 and will compress it using gzip::

      [core]
      xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend

      [common.io]
      xcom_objectstorage_path = s3://conn_id@mybucket/key
      xcom_objectstorage_threshold = 1048576
      xcom_objectstorage_compression = gzip

Another example using the local filesystem::

      [core]
      xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend

      [common.io]
      xcom_objectstorage_path = file://airflow/xcoms

The local filesystem scheme can also be used as it's the same as file show above, see the `fsspec <https://github.com/fsspec/filesystem_spec/blob/f30bc759f30327dfb499f37e967648f175750fac/fsspec/implementations/local.py#L30>`_::

      [core]
      xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend

      [common.io]
      xcom_objectstorage_path = local://airflow/xcoms

.. note::

  Compression requires the support for it is installed in your python environment. For example, to use ``snappy`` compression, you need to install ``python-snappy``. Zip, gzip and bz2 work out of the box.

Reading a large value where it is stored
----------------------------------------

A value stored in object storage still travels through the task that pulls it: ``xcom_pull`` reads the file,
deserializes it and hands the Python object over. For a large extract (a JSON array of millions of rows
pushed by an extraction task, for example) that is unnecessary when the next step is a tool that can read the
file itself, such as a query engine reading JSON or Parquet from object storage: the task only needs to tell it
where the file is.

``XComObjectStorageBackend.get_path`` returns that location, an :class:`~airflow.sdk.ObjectStoragePath`,
without reading the file. In templates, the provider's ``xcom_path`` macro does the same and returns a string.

For example, an ``HttpOperator`` pushes a large JSON array of invoices from a REST API, and a
:class:`~airflow.providers.common.sql.operators.generic_transfer.GenericTransfer` with a DuckDB source reads it
straight from object storage, flattens it and inserts the rows into the target database. The rows are never
deserialized in Python between the two tasks:

.. code-block:: sql

    -- sql/flatten_invoices.sql, run by DuckDB
    SELECT *
    FROM read_json(
        '{{ macros.common_io.xcom_path(ti, "extract_invoices") }}',
        format = 'array',
        columns = {
            invoice_id: 'BIGINT',
            customer: 'VARCHAR',
            invoice_date: 'TIMESTAMP',
            amount: 'DOUBLE'
        }
    )

.. code-block:: python

    from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
    from airflow.providers.http.operators.http import HttpOperator

    # Every invoice changed in the last year, as one JSON array: tens of MB, written to object storage
    # by the backend.
    extract_invoices = HttpOperator(
        task_id="extract_invoices",
        http_conn_id="invoicing_api",
        method="GET",
        endpoint="invoices",
        data={"changed_since": "{{ (data_interval_start - macros.timedelta(days=365)) | ts }}"},
        response_filter=lambda response: response.json(),
    )

    load_invoices = GenericTransfer(
        task_id="load_invoices",
        source_conn_id="duckdb_default",
        destination_conn_id="postgres_default",
        destination_table="invoices",
        sql="sql/flatten_invoices.sql",
        preoperator="sql/create_invoices.sql",
    )
    extract_invoices >> load_invoices

DuckDB reads ``.gz`` files transparently, so ``xcom_objectstorage_compression = gzip`` needs no extra option;
for object storage other than the local file system, give DuckDB access to it (its ``httpfs`` extension and a
secret, for example).

Only values written to object storage have a path, so ``xcom_objectstorage_threshold`` decides which ones do;
a threshold of ``0`` writes every value to object storage. ``protocol=False`` returns the path without its
scheme (``bucket/key/...`` instead of ``s3://bucket/key/...``), for a tool that is given the file system
separately.

``get_path`` returns ``None`` when there is no such XCom or when its value is stored in the database. The macro
raises instead, since a ``None`` rendered into a query fails later with a less helpful error:

* ``TypeError`` when the configured XCom backend is not ``XComObjectStorageBackend``;
* ``ValueError`` when the XCom has no path: it does not exist, or its value is stored in the database
  (smaller than the threshold).

Looking up the path requires Airflow 3.
