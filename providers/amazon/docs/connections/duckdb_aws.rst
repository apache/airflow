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

.. _howto/connection:duckdb_aws:

DuckDB on AWS Connection
========================

The DuckDB on AWS connection type is used by
:class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook` to query data in Amazon S3 with
DuckDB. See :doc:`../operators/duckdb` for usage.

Two connections are involved:

* A **DuckDB on AWS** (``duckdb_aws``) connection which configures DuckDB itself, i.e.: *which DuckDB database and with what settings*.
  Its id is what you pass to
  :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` as ``conn_id``, and its
  type is also what selects :class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook`.
* An :ref:`AWS connection <howto/connection:aws>` gives the region and endpoint, and also the credentials
  (keys or role) when ``credential_strategy`` is ``config``. The DuckDB connection points at it through
  ``aws_conn_id``, which defaults to ``aws_default``.

With the default ``credential_strategy`` of ``credential_chain``, DuckDB takes credentials from the
worker's environment, such as environment variables or the worker's role, rather than from the AWS
connection. If ``credential_strategy`` is not set and the AWS connection has credentials, the hook logs
a warning; set it to ``config`` to use them, or to ``credential_chain`` to silence the warning. See
:ref:`howto/operator:duckdb_aws:credentials`.

The ``duckdb_aws`` connection can be empty (though it must exist) and often will be with DuckDB's
ability to use in-memory databases. Create it, name it whatever you like, and leave every field blank
to get an in-memory database.

Configuring the Connection
--------------------------

Database path (optional)
    Path to a DuckDB database file, for example ``/tmp/analytics.duckdb``. Leave empty for an
    in-memory database.

Extra (optional)
    A JSON object. Every parameter of ``AwsDuckDBHook`` can be set here, and ``hook_params`` on the
    operator overrides it per task. The AWS keys are:

    * ``aws_conn_id``: the AWS connection the secret's region and endpoint come from, and its
      credentials too when ``credential_strategy`` is ``config``. Defaults to ``aws_default``. Set
      it to ``null`` to use no Airflow connection and let the AWS SDK find credentials in the
      environment.
    * ``region_name``: region for the S3 secret. Defaults to the region of the AWS connection.
    * ``credential_strategy``: ``credential_chain`` (default), ``config`` or ``none``. See
      :ref:`howto/operator:duckdb_aws:credentials`.
    * ``credential_chain``: the providers DuckDB's credential chain consults, for example
      ``"env;config;sts;instance"``.
    * ``s3_endpoint_url``: S3 endpoint override, for an S3-compatible service or a VPC endpoint.
      Defaults to ``endpoint_url`` from the AWS connection extra.
    * ``secret_name``: name of the DuckDB secret to create. Defaults to ``airflow_aws``.

    The DuckDB engine keys, such as ``memory_limit``, ``threads`` and ``extensions``, are the same as
    in the :doc:`DuckDB connection <apache-airflow-providers-duckdb:connections/duckdb>`. One default
    differs: ``autoinstall_extensions`` is ``true`` here, so the ``httpfs`` and ``aws`` extensions
    are installed if they are missing.

Examples
--------

An in-memory database with explicit resource limits. The region comes from ``aws_default`` and the
credentials from the worker's environment:

.. code-block:: json

    {
      "memory_limit": "2GB",
      "threads": 4
    }

A database file, with credentials from a specific AWS connection:

.. code-block:: bash

    export AIRFLOW_CONN_DUCKDB_AWS_DEFAULT='{
        "conn_type": "duckdb_aws",
        "host": "/tmp/analytics.duckdb",
        "extra": {"aws_conn_id": "aws_analytics", "credential_strategy": "config", "region_name": "eu-west-1"}
    }'
