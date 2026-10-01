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

=============
DuckDB on AWS
=============

`DuckDB <https://duckdb.org/>`__ is an in-process analytical database. It can read and write data in
Amazon S3 directly, which makes it an affordable way to run transforms that do not justify the scale
of a distributed cluster.

DuckDB is not an AWS service, so the generic hook, operator and ``duckdb`` connection type live in the
:doc:`DuckDB provider <apache-airflow-providers-duckdb:index>`. What this provider adds is the AWS
auth. Using an Airflow AWS connection to create the DuckDB secret that grants access to S3.
This means there is no credential wiring needed in the Dag.

You run :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` and point it
at a ``duckdb_aws`` connection when the SQL should reach S3.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

DuckDB support is an optional extra

.. code-block:: bash

    pip install 'apache-airflow-providers-amazon[duckdb]'

.. note::

    S3 access needs DuckDB's ``httpfs`` and ``aws`` extensions. This provider installs them if they
    are missing by default. If your workers should not download anything at runtime, set
    ``autoinstall_extensions=False`` and make the extensions available yourself, either
    in an ``extension_directory`` or baked into your image.

.. _howto/connection:duckdb_aws:

Run a DuckDB query against Amazon S3
====================================

Two connections are involved:

* A **DuckDB on AWS** (``duckdb_aws``) connection which configures DuckDB itself, i.e.: *which DuckDB database and with what settings*.
  Its id is what you pass to
  :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` as ``conn_id``, and its
  type is also what selects :class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook`.
* An **AWS** (``aws``) connection says *which AWS identity*, i.e.: keys or role, region, endpoint, etc. The DuckDB
  connection points at it through ``aws_conn_id``, which defaults to ``aws_default``.

Credentials stay in the usual AWS connection. The ``duckdb_aws`` connection can be empty (though it must exist) and often
will be with DuckDB's ability to use in-memory databases. Create it, name it whatever you like, and leave every field blank
to get an in-memory database.

Given that connection, the hook loads the ``httpfs`` and ``aws`` extensions and creates the S3 secret,
so the task supplies only SQL:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_duckdb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_aws_duckdb]
    :end-before: [END howto_operator_aws_duckdb]

Pass the id explicitly. With no ``conn_id`` the operator uses ``duckdb_default`` and the vendor-neutral
hook, which has no S3 access.

The DuckDB connection carries the database and the engine settings; leave its ``Database path`` empty for
an in-memory database, which suits a task that reads from S3 and writes back to it. Everything
:class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook` accepts can be set in the connection
``extra``, and ``hook_params`` on the operator overrides it per task. Set ``memory_limit`` and
``threads`` explicitly: DuckDB otherwise sizes itself from the resources it detects on the host.

.. code-block:: python

    hook_params = {"memory_limit": "2GB", "threads": 4}

.. note::

    ``aws_conn_id`` resolves on whether the key is present, not on its value, because ``None`` is a
    valid choice for it rather than the absence of one. Set it to ``null`` in the ``extra`` (or
    ``None`` in ``hook_params``) to use no Airflow AWS connection at all and let the AWS SDK find
    credentials in the environment, such as environment variables or an instance role.

Using the hook directly
=======================

For work that is not a single statement (e.g. chaining several queries against one database, or returning
results to Python) you may use the hook directly. Opening one connection for several statements also
avoids paying connection setup and extension loading more than once per task:

.. code-block:: python

    @task
    def compare_regions():
        hook = AwsDuckDBHook(aws_conn_id="aws_default")
        with hook.get_conn() as conn:
            conn.execute("CREATE TABLE sales AS SELECT * FROM read_parquet('s3://my-bucket/sales.parquet')")
            return conn.execute("SELECT region, SUM(revenue) FROM sales GROUP BY region").fetchall()

How credentials reach DuckDB
============================

DuckDB does not use ``boto3``. The hook therefore issues a ``CREATE SECRET`` statement that tells
DuckDB how to authenticate, and the ``credential_strategy`` parameter selects how:

``credential_chain`` (default)
    Issues ``CREATE SECRET (TYPE s3, PROVIDER credential_chain)``, which resolves credentials through
    the AWS SDK inside DuckDB's ``aws`` extension. Nothing secret is written into SQL text and the SDK
    refreshes expiring credentials itself, so a long query does not fail partway through when a set of
    temporary credentials expires.

    From DuckDB 1.4 onward the chain is resolved when the secret is created rather than when S3 is
    first read. If nothing can be resolved, the hook logs a warning and opens the connection anyway,
    so a query that does not use S3 still runs; one that does will fail on its own. A ``config``
    secret that cannot be created raises instead, since explicit credentials failing is a
    misconfiguration.

``config``
    Resolves credentials through
    :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook` and writes them into the secret
    explicitly. This also works with a container-assigned role, because boto3 resolves that on the
    Airflow side, but the credentials are frozen when the connection is opened rather than refreshed.
    Needed when the Airflow connection carries static keys that the AWS SDK running inside
    DuckDB cannot see. The credentials become part of the SQL statement, so prefer
    ``credential_chain`` where it works.

``none``
    Creates no secret. Use for a DuckDB database that never touches S3.

.. code-block:: python

    DuckDBExecuteQueryOperator(
        task_id="query",
        conn_id="duckdb_aws_default",
        sql="SELECT COUNT(*) FROM read_parquet('s3://my-bucket/data.parquet')",
        hook_params={"aws_conn_id": "aws_static_keys", "credential_strategy": "config"},
    )

Non-default S3 endpoints
========================

Set ``s3_endpoint_url``, or ``endpoint_url`` in the AWS connection extra, to reach an S3-compatible
service or an S3 interface VPC endpoint. The hook translates it into the secret's ``ENDPOINT``,
``URL_STYLE`` and ``USE_SSL`` clauses, selecting path-style addressing because virtual-host-style
bucket names generally do not resolve against such endpoints.

.. note::

    The S3 secret is the only channel through which Airflow configuration reaches DuckDB. A DuckDB
    session with no secret ignores the Airflow connection entirely — including its region and endpoint
    falling back to whatever DuckDB's own settings and environment provide.

    DuckDB's ``httpfs`` extension reads credentials from the standard
    ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` / ``AWS_SESSION_TOKEN`` environment
    variables, so it can authenticate where those are set. What its HTTP client does not implement is
    the ECS container credential provider, so a container-assigned task role (often how a managed Airflow
    worker receives credentials) is invisible to it and S3 access fails with an opaque
    ``HTTP 403``. Both ``credential_chain`` and ``config`` handle that case; the difference is whether
    DuckDB or Airflow does the resolving, and therefore whether the credentials refresh.

Reference
=========

* `DuckDB S3 API support <https://duckdb.org/docs/stable/extensions/httpfs/s3api>`__
* `DuckDB secrets manager <https://duckdb.org/docs/stable/configuration/secrets_manager>`__
