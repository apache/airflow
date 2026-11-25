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

===============
DuckDB on Azure
===============

`DuckDB <https://duckdb.org/>`__ is an in-process analytical database. With its ``azure`` extension it
reads and writes Azure Blob Storage and Azure Data Lake Storage Gen2 (``az://`` and ``abfss://`` paths)
directly, Parquet, JSON, CSV and Delta tables alike.

DuckDB is not an Azure service, so the generic hook, operator and ``duckdb`` connection type live in the
:doc:`DuckDB provider <apache-airflow-providers-duckdb:index>`. What this provider adds is the Azure
auth: it uses an Airflow ADLS connection to create the DuckDB secret that grants access to the storage,
so there is no credential wiring in the Dag. It is the Azure counterpart of
:doc:`DuckDB on AWS <apache-airflow-providers-amazon:operators/duckdb>`.

You run :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` and point it at a
``duckdb_azure`` connection when the SQL should reach Azure storage.

Prerequisite Tasks
------------------

.. include:: /operators/_partials/prerequisite_tasks.rst

DuckDB support is an optional extra:

.. code-block:: bash

    pip install 'apache-airflow-providers-microsoft-azure[duckdb]'

.. note::

    Azure access needs DuckDB's ``azure`` extension. This provider installs it if it is missing by
    default. If your workers should not download anything at runtime, set
    ``autoinstall_extensions=False`` and make the extension available yourself, either in an
    ``extension_directory`` or baked into your image.

Run a DuckDB query against Azure storage
========================================

Two connections are involved: a :ref:`DuckDB on Azure connection <howto/connection:duckdb_azure>` that
says which DuckDB database to open and with what settings, and an
:ref:`ADLS connection <howto/connection:adls>` that gives the Azure identity and the storage account.
Credentials stay in the ADLS connection. The DuckDB on Azure connection can be empty, but the operator
needs it to exist.

Given those connections, the hook loads the ``azure`` extension and creates the Azure secret, so the task
supplies only SQL:

.. code-block:: python

    from airflow.providers.duckdb.operators.duckdb import DuckDBExecuteQueryOperator

    count_rows = DuckDBExecuteQueryOperator(
        task_id="count_rows",
        conn_id="duckdb_azure_default",
        sql="SELECT COUNT(*) FROM read_parquet('abfss://my-container@myaccount.dfs.core.windows.net/sales/*.parquet')",
    )

Pass the id explicitly. With no ``conn_id`` the operator uses ``duckdb_default`` and the vendor-neutral
hook, which has no Azure access.

Everything :class:`~airflow.providers.microsoft.azure.hooks.duckdb.AzureDuckDBHook` accepts can be set in
the DuckDB connection ``extra``, and ``hook_params`` on the operator overrides it per task. Set
``memory_limit`` and ``threads`` explicitly: DuckDB otherwise sizes itself from the resources it detects on
the host.

.. code-block:: json

    {
      "azure_conn_id": "adls_default",
      "memory_limit": "2GB",
      "threads": 4
    }

Microsoft Fabric OneLake works the same way: an ADLS connection whose ``account_url`` is
``https://onelake.dfs.fabric.microsoft.com`` yields the account ``onelake``, and DuckDB then reads paths such
as ``abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<schema>/<table>``.

Using the hook directly
=======================

For work that is not a single statement (e.g. chaining several queries against one database, or returning
results to Python) use the hook directly. Opening one connection for several statements also avoids paying
connection setup and extension loading more than once per task:

.. code-block:: python

    from airflow.providers.microsoft.azure.hooks.duckdb import AzureDuckDBHook
    from airflow.sdk import task


    @task
    def compare_regions():
        hook = AzureDuckDBHook(azure_conn_id="adls_default")
        with hook.get_conn() as conn:
            conn.execute(
                "CREATE TABLE sales AS "
                "SELECT * FROM read_parquet('abfss://my-container@myaccount.dfs.core.windows.net/sales.parquet')"
            )
            return conn.execute("SELECT region, SUM(revenue) FROM sales GROUP BY region").fetchall()

.. _howto/operator:duckdb_azure:credentials:

How credentials reach DuckDB
============================

DuckDB does not use the Azure SDK for Python. The hook therefore issues a ``CREATE SECRET`` statement that
tells DuckDB how to authenticate, and the ``credential_strategy`` parameter selects how:

``config`` (default)
    Builds the secret from the credentials of the ADLS connection, read in the order
    :class:`~airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook` reads them:

    * its ``connection_string`` when the extra has one (``PROVIDER config``);
    * else an Active Directory service principal when the extra has a ``tenant_id`` (``PROVIDER
      service_principal``), with the connection's login as client id and its password as client secret;
    * else the connection's password as the key of the storage account (``PROVIDER config``, with a
      connection string built from the storage account and the key).

    An ADLS connection without credentials, which the ADLS hook resolves through
    ``DefaultAzureCredential``, is rejected: use ``credential_chain`` for it. The extra's
    ``proxies["https"]``, as the ADLS hook uses it, becomes the secret's ``HTTP_PROXY``. The credentials
    become part of the SQL statement.

``credential_chain``
    Issues ``CREATE SECRET (TYPE azure, PROVIDER credential_chain)``, which resolves credentials through the
    Azure SDK inside DuckDB's ``azure`` extension: Azure CLI, managed identity, workload identity or
    environment variables, optionally narrowed with ``credential_chain`` (for example
    ``"managed_identity;env"``). Nothing secret is written into SQL text. The default ADLS connection is
    optional with this strategy: when it exists, it supplies the storage account and proxy; otherwise set
    ``account_name``. An ``azure_conn_id`` other than ``adls_default`` has to exist. The chain does not read
    the ``managed_identity_client_id`` or ``workload_identity_tenant_id`` of the ADLS connection; DuckDB
    picks the identity from the environment.

``none``
    Creates no secret. Use for a DuckDB database that never touches Azure.

The storage account of the secret is ``account_name`` when it is set, otherwise it is derived from the
ADLS connection: the first label of its ``account_url`` (with or without a scheme), or its host.

.. code-block:: python

    DuckDBExecuteQueryOperator(
        task_id="query",
        conn_id="duckdb_azure_default",
        sql="SELECT COUNT(*) FROM read_parquet('az://my-container/data.parquet')",
        hook_params={"credential_strategy": "credential_chain", "account_name": "myaccount"},
    )

TLS certificate errors
======================

The Azure SDK inside DuckDB looks for the CA bundle in one fixed location with its default HTTP transport.
On a Linux image whose bundle lives elsewhere, token requests fail with an SSL CA certificate error. Set
``transport_option_type`` to ``curl`` to switch to the curl transport, which searches the usual locations.
The hook sets it as the global ``azure_transport_option_type`` setting before it creates the secret.

Reference
=========

* `DuckDB Azure extension <https://duckdb.org/docs/stable/core_extensions/azure>`__
* `DuckDB secrets manager <https://duckdb.org/docs/stable/configuration/secrets_manager>`__
