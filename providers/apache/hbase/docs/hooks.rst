

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

HBase Hooks
===========

The HBase provider exposes two hooks:

- :class:`~airflow.providers.apache.hbase.hooks.hbase.HBaseThriftHook` -
  Thrift2 protocol for table operations.
- :class:`~airflow.providers.apache.hbase.hooks.hbase_cli.HBaseCLIHook` -
  CLI-based hook for administrative backup/restore workflows.

Both hooks register under the ``hbase`` connection type.

HBaseThriftHook
---------------

Uses the Apache Thrift2 binary protocol to communicate with a running
``HBase Thrift2`` server. Supports:

* **CRUD operations**: ``put_row``, ``get_row``, ``delete_row``
* **Scan operations**: ``scan_table`` with optional start/stop rows, column filters, and row limits
* **Batch operations**: ``batch_put_rows``, ``batch_get_rows``, ``batch_delete_rows``
* **Table administration**: ``create_table``, ``delete_table``, ``table_exists``
* **Connectivity check**: ``test_connection``, used by the Airflow *Test connection* UI and
  ``airflow connections test`` CLI command

**Connection strategy**

The hook uses a :class:`~airflow.providers.apache.hbase.hooks.hbase_strategy.HBaseStrategy`
pattern to select the connection mode:

* ``Thrift2Strategy`` - single persistent connection (default)
* ``PooledThrift2Strategy`` - connection pool for high-throughput scenarios

To enable the connection pool, set ``connection_pool.enabled: true`` in the
connection extra.

**Code example**

.. code-block:: python

    from airflow.providers.apache.hbase.hooks.hbase import HBaseThriftHook

    hook = HBaseThriftHook(hbase_conn_id="hbase_default")
    hook.put_row("my_table", "row_key_1", {"cf1:col1": "value1"})
    result = hook.get_row("my_table", "row_key_1")

HBaseCLIHook
------------

Executes HBase CLI commands on the worker host for administrative operations.
Designed for backup/restore workflows that require direct HBase shell access.

* **Backup set management**: ``create_backup_set``, ``list_backup_sets``
* **Backup operations**: ``create_full_backup``, ``create_incremental_backup``
* **Restore operations**: ``restore_backup``
* **History/describe**: ``get_backup_history``, ``describe_backup``
* **Arbitrary commands**: ``execute_command`` for any HBase CLI command
* **Lifecycle**: ``on_kill`` — terminates a running backup/restore process when
  the task is killed by Airflow

**Prerequisites**

* HBase CLI tools installed on the worker. The binary name defaults to ``hbase``
  and can be overridden via the ``hbase_cmd`` constructor parameter or the
  ``HBASE_CMD`` environment variable.
* Java runtime (``java_home`` parameter or ``JAVA_HOME`` env)
* HBase installation directory (``hbase_home`` parameter or ``HBASE_HOME`` env)

**Code example**

.. code-block:: python

    from airflow.providers.apache.hbase.hooks.hbase_cli import HBaseCLIHook

    hook = HBaseCLIHook(
        hbase_conn_id="hbase_default",
        java_home="/usr/lib/jvm/java",
        hbase_home="/usr/lib/hbase",
    )
    hook.create_backup_set("my_set", ["table1", "table2"])
    output = hook.create_full_backup(backup_root="hdfs:///hbase/backup")

When to use hooks directly
---------------------------

Most users interact with the hooks indirectly through operators and sensors.
Use hooks directly when you need:

* Ad-hoc queries inside ``PythonOperator`` callables
* Custom logic that doesn't map to a pre-built operator
* Backups or restores with dynamic parameters computed at runtime
