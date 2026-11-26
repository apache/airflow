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

.. _howto/connection:duckdb_azure:

DuckDB on Azure Connection
==========================

The DuckDB on Azure connection type is used by
:class:`~airflow.providers.microsoft.azure.hooks.duckdb.AzureDuckDBHook` to query data in Azure Blob
Storage, Azure Data Lake Storage Gen2 and Microsoft Fabric OneLake with DuckDB. See
:doc:`../operators/duckdb` for usage.

Two connections are involved:

* A **DuckDB on Azure** (``duckdb_azure``) connection which configures DuckDB itself, i.e.: *which
  DuckDB database and with what settings*. Its id is what you pass to
  :class:`~airflow.providers.duckdb.operators.duckdb.DuckDBExecuteQueryOperator` as ``conn_id``, and its
  type is also what selects :class:`~airflow.providers.microsoft.azure.hooks.duckdb.AzureDuckDBHook`.
* An :ref:`ADLS connection <howto/connection:adls>` says *which Azure identity and storage account*. The
  DuckDB connection points at it through ``azure_conn_id``, which defaults to ``adls_default``.

Credentials stay in the ADLS connection. With the default ``credential_strategy`` of ``config`` the
hook builds the DuckDB secret from them; see :ref:`howto/operator:duckdb_azure:credentials` for the
credentials it reads and for the other strategies.

The ``duckdb_azure`` connection can be empty. Create it, name it whatever you like, and leave every
field blank to get an in-memory database. Pass its id to ``DuckDBExecuteQueryOperator`` as ``conn_id``;
the operator needs the connection to exist, and with no ``conn_id`` it uses ``duckdb_default`` and the
plain DuckDB hook, which has no Azure access. ``AzureDuckDBHook`` used on its own defaults to
``duckdb_azure_default``, and opens an in-memory database if that connection does not exist.

Configuring the Connection
--------------------------

Database path (optional)
    Path to a DuckDB database file, for example ``/tmp/analytics.duckdb``. Leave empty for an
    in-memory database.

Extra (optional)
    A JSON object. Every parameter of ``AzureDuckDBHook`` can be set here, and ``hook_params`` on the
    operator overrides it per task. The Azure keys are:

    * ``azure_conn_id``: the ADLS connection the secret is built from. Defaults to ``adls_default``.
      With the ``credential_chain`` strategy the default connection may be missing; any other id has
      to exist.
    * ``account_name``: the storage account of the secret. Defaults to the account of the ADLS
      connection: the first label of its ``account_url``, or its host.
    * ``credential_strategy``: ``config`` (default), ``credential_chain`` or ``none``. See
      :ref:`howto/operator:duckdb_azure:credentials`.
    * ``credential_chain``: the providers DuckDB's credential chain consults, for example
      ``"managed_identity;env"``.
    * ``transport_option_type``: the HTTP transport of the Azure SDK in DuckDB, ``default`` or
      ``curl``. ``curl`` resolves TLS certificate errors on Linux images whose CA bundle is not where
      the default transport looks.
    * ``secret_name``: name of the DuckDB secret to create. Defaults to ``airflow_azure``.

    The DuckDB engine keys, such as ``memory_limit``, ``threads`` and ``extensions``, are the same as
    in the :doc:`DuckDB connection <apache-airflow-providers-duckdb:connections/duckdb>`. One default
    differs: ``autoinstall_extensions`` is ``true`` here, so missing extensions, including ``azure``,
    are downloaded from DuckDB's extension repository.

Examples
--------

An in-memory database with explicit resource limits. The credentials, storage account and proxy come
from ``adls_default``:

.. code-block:: json

    {
      "azure_conn_id": "adls_default",
      "memory_limit": "2GB",
      "threads": 4
    }

A database file, with credentials the Azure SDK inside DuckDB resolves from the worker's environment:

.. code-block:: bash

    export AIRFLOW_CONN_DUCKDB_AZURE_DEFAULT='{
        "conn_type": "duckdb_azure",
        "host": "/tmp/analytics.duckdb",
        "extra": {"credential_strategy": "credential_chain", "account_name": "myaccount"}
    }'
