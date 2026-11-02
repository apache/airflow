

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

HBase Operators
===============

The HBase provider exposes 10 operators organized into three groups:
CRUD, batch, and backup/restore.

Prerequisites
-------------

* A HBase Thrift2 server must be running and accessible.
* An ``hbase`` connection must be configured in Airflow.
* For backup/restore operators, HBase CLI tools and Java must be installed
  on the worker.

Common operator parameters
--------------------------

All operators share these parameters:

* ``hbase_conn_id`` - Airflow connection ID (default: ``hbase_default``).
* ``template_fields`` - Jinja-templatable fields for dynamic DAG parameters.

CRUD operators
--------------

**HBasePutOperator**

Insert a single row into an HBase table.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import HBasePutOperator

    HBasePutOperator(
        task_id="put_data",
        table_name="my_table",
        row_key="row_1",
        data={"cf1:col1": "value1", "cf1:col2": "value2"},
        hbase_conn_id="hbase_thrift2",
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - HBase table name (templated).
   * - ``row_key``
     - Row key for the data (templated).
   * - ``data``
     - Dictionary of ``family:column`` -> value pairs (templated).

**HBaseCreateTableOperator**

Create an HBase table with column families.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import (
        HBaseCreateTableOperator,
        IfExistsAction,
    )

    HBaseCreateTableOperator(
        task_id="create_table",
        table_name="my_table",
        families={"cf1": {}, "cf2": {"max_versions": 3}},
        if_exists=IfExistsAction.IGNORE,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Table name (templated).
   * - ``families``
     - Dict of family name -> config dict (templated).
   * - ``if_exists``
     - ``IfExistsAction.IGNORE`` or ``.ERROR``

**HBaseDeleteTableOperator**

Delete an HBase table.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import (
        HBaseDeleteTableOperator,
        IfNotExistsAction,
    )

    HBaseDeleteTableOperator(
        task_id="delete_table",
        table_name="my_table",
        if_not_exists=IfNotExistsAction.IGNORE,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Table name (templated).
   * - ``disable``
     - Disable table before deletion (default: ``True``).
   * - ``if_not_exists``
     - ``IfNotExistsAction.IGNORE`` or ``.ERROR``

**HBaseScanOperator**

Scan rows from an HBase table.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import HBaseScanOperator

    HBaseScanOperator(
        task_id="scan_table",
        table_name="my_table",
        row_start="prefix_",
        row_stop="prefix_~",
        columns=["cf1:col1"],
        limit=100,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Table name (templated).
   * - ``row_start``
     - Start row key for scan (optional).
   * - ``row_stop``
     - Stop row key for scan (optional).
   * - ``columns``
     - List of columns to retrieve (optional).
   * - ``limit``
     - Max rows to return (optional).
   * - ``encoding``
     - Byte decoding encoding (default: ``utf-8``).

Batch operators
---------------

**HBaseBatchPutOperator**

Insert multiple rows in batch.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import HBaseBatchPutOperator

    rows = [
        {"row_key": "row_1", "cf1:name": "Alice", "cf2:age": "30"},
        {"row_key": "row_2", "cf1:name": "Bob", "cf2:age": "25"},
    ]

    HBaseBatchPutOperator(
        task_id="batch_put",
        table_name="my_table",
        rows=rows,
        batch_size=200,
        max_workers=4,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Table name (templated).
   * - ``rows``
     - List of dicts with ``row_key`` and data columns (templated).
   * - ``batch_size``
     - Rows per chunk (default: 200).
   * - ``max_workers``
     - Parallel workers (default: 4).

**HBaseBatchGetOperator**

Get multiple rows in batch.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import HBaseBatchGetOperator

    HBaseBatchGetOperator(
        task_id="batch_get",
        table_name="my_table",
        row_keys=["row_1", "row_2", "row_3"],
        columns=["cf1:name", "cf2:age"],
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Table name (templated).
   * - ``row_keys``
     - List of row keys (templated).
   * - ``columns``
     - List of columns to retrieve (optional, templated).
   * - ``encoding``
     - Byte decoding encoding (default: ``utf-8``).

Backup/restore operators (CLI-based)
-------------------------------------

These operators use ``HBaseCLIHook`` and require the HBase CLI and Java
on the worker host.

``HBaseCreateBackupOperator`` and ``HBaseRestoreOperator`` implement ``on_kill``:
when Airflow sends SIGTERM to the worker (e.g. task timeout or manual kill),
the running HBase CLI subprocess is terminated immediately.

**HBaseBackupSetOperator**

Create or list backup sets.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import (
        HBaseBackupSetOperator,
        BackupSetAction,
    )

    HBaseBackupSetOperator(
        task_id="create_backup_set",
        action=BackupSetAction.ADD,
        backup_set_name="my_set",
        tables=["table1", "table2"],
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``action``
     - ``BackupSetAction.ADD`` or ``.LIST``
   * - ``backup_set_name``
     - Backup set name (templated).
   * - ``tables``
     - List of table names (templated, for ADD).

**HBaseCreateBackupOperator**

Create a full or incremental backup.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import (
        HBaseCreateBackupOperator,
        BackupType,
    )

    HBaseCreateBackupOperator(
        task_id="full_backup",
        backup_type=BackupType.FULL,
        backup_path="hdfs:///hbase/backup",
        backup_set_name="my_set",
        workers=3,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``backup_type``
     - ``BackupType.FULL`` or ``.INCREMENTAL``
   * - ``backup_path``
     - HDFS backup directory (templated).
   * - ``backup_set_name``
     - Backup set name (templated, optional).
   * - ``tables``
     - Alternative to ``backup_set_name`` (templated).
   * - ``workers``
     - Number of workers (default: 3).

Returns the backup ID extracted from HBase CLI output.

**HBaseRestoreOperator**

Restore a table from backup.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import HBaseRestoreOperator

    HBaseRestoreOperator(
        task_id="restore",
        backup_path="hdfs:///hbase/backup",
        backup_id="backup_1234567890",
        tables=["table1"],
        overwrite=True,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``backup_path``
     - HDFS backup directory (templated).
   * - ``backup_id``
     - Backup ID to restore (templated).
   * - ``backup_set_name``
     - Backup set name (templated, optional).
   * - ``tables``
     - Tables to restore (templated, optional).
   * - ``overwrite``
     - Overwrite existing tables (default: ``False``).

**HBaseBackupHistoryOperator**

Get backup history.

.. code-block:: python

    from airflow.providers.apache.hbase.operators.hbase import HBaseBackupHistoryOperator

    HBaseBackupHistoryOperator(
        task_id="backup_history",
        backup_set_name="my_set",
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``backup_set_name``
     - Backup set name (templated, optional).
   * - ``backup_path``
     - HDFS path to filter (templated, optional).

Enum reference
--------------

.. list-table::
   :header-rows: 1

   * - Enum
     - Values
     - Used by
   * - ``BackupSetAction``
     - ``ADD``, ``LIST``
     - ``HBaseBackupSetOperator``
   * - ``BackupType``
     - ``FULL``, ``INCREMENTAL``
     - ``HBaseCreateBackupOperator``
   * - ``IfExistsAction``
     - ``IGNORE``, ``ERROR``
     - ``HBaseCreateTableOperator``
   * - ``IfNotExistsAction``
     - ``IGNORE``, ``ERROR``
     - ``HBaseDeleteTableOperator``
