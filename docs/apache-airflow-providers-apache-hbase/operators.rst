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



Apache HBase Operators
======================

`Apache HBase <https://hbase.apache.org/>`__ is a distributed, scalable, big data store built on Apache Hadoop. It provides random, real-time read/write access to your big data and is designed to host very large tables with billions of rows and millions of columns.

Prerequisite
------------

To use operators, you must configure an :doc:`HBase Connection <connections/hbase>`.

.. _howto/operator:HBaseCreateTableOperator:

Creating a Table
^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseCreateTableOperator` operator is used to create a new table in HBase.

Use the ``table_name`` parameter to specify the table name and ``column_families`` parameter to define the column families for the table.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase.py
    :language: python
    :start-after: [START howto_operator_hbase_create_table]
    :end-before: [END howto_operator_hbase_create_table]

.. _howto/operator:HBasePutOperator:

Inserting Data
^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBasePutOperator` operator is used to insert a single row into an HBase table.

Use the ``table_name`` parameter to specify the table, ``row_key`` for the row identifier, and ``data`` for the column values.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase.py
    :language: python
    :start-after: [START howto_operator_hbase_put]
    :end-before: [END howto_operator_hbase_put]

.. _howto/operator:HBaseBatchPutOperator:

Batch Insert Operations
^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseBatchPutOperator` operator is used to insert multiple rows into an HBase table in a single batch operation.

Use the ``table_name`` parameter to specify the table and ``rows`` parameter to provide a list of row data.
For optimal performance, configure ``batch_size`` (default: 200) and ``max_workers`` (default: 4) parameters.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase_advanced.py
    :language: python
    :start-after: [START howto_operator_hbase_batch_put]
    :end-before: [END howto_operator_hbase_batch_put]

Performance Optimization
""""""""""""""""""""""""

For high-throughput batch operations, use connection pooling and configure batch parameters:

.. code-block:: python

    # Optimized batch insert with connection pooling
    optimized_batch_put = HBaseBatchPutOperator(
        task_id="optimized_batch_put",
        table_name="my_table",
        rows=large_dataset,
        batch_size=500,  # Process 500 rows per batch
        max_workers=8,   # Use 8 parallel workers
        hbase_conn_id="hbase_pooled",  # Connection with pool_size > 1
    )

.. note::
    Connection pooling is automatically enabled when ``pool_size`` > 1 in the connection configuration.
    This provides significant performance improvements for concurrent operations.

.. _howto/operator:HBaseBatchGetOperator:

Batch Retrieve Operations
^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseBatchGetOperator` operator is used to retrieve multiple rows from an HBase table in a single batch operation.

Use the ``table_name`` parameter to specify the table and ``row_keys`` parameter to provide a list of row keys to retrieve.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase_advanced.py
    :language: python
    :start-after: [START howto_operator_hbase_batch_get]
    :end-before: [END howto_operator_hbase_batch_get]

.. _howto/operator:HBaseScanOperator:

Scanning Tables
^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseScanOperator` operator is used to scan and retrieve multiple rows from an HBase table based on specified criteria.

Use the ``table_name`` parameter to specify the table, and optional parameters like ``row_start``, ``row_stop``, ``columns``, and ``filter`` to control the scan operation.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase_advanced.py
    :language: python
    :start-after: [START howto_operator_hbase_scan]
    :end-before: [END howto_operator_hbase_scan]

.. _howto/operator:HBaseDeleteTableOperator:

Deleting a Table
^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseDeleteTableOperator` operator is used to delete an existing table from HBase.

Use the ``table_name`` parameter to specify the table to delete.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase.py
    :language: python
    :start-after: [START howto_operator_hbase_delete_table]
    :end-before: [END howto_operator_hbase_delete_table]

Backup and Restore Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

HBase provides built-in backup and restore functionality for data protection and disaster recovery.

.. _howto/operator:HBaseBackupSetOperator:

Managing Backup Sets
""""""""""""""""""""

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseBackupSetOperator` operator is used to manage backup sets containing one or more tables.

Supported actions:
- ``add``: Create a new backup set with specified tables
- ``list``: List all existing backup sets
- ``describe``: Get details about a specific backup set
- ``delete``: Remove a backup set

Use the ``action`` parameter to specify the operation, ``backup_set_name`` for the backup set name, and ``tables`` parameter to list the tables (for 'add' action).

.. code-block:: python

    # Create a backup set
    create_backup_set = HBaseBackupSetOperator(
        task_id="create_backup_set",
        action="add",
        backup_set_name="my_backup_set",
        tables=["table1", "table2"],
        hbase_conn_id="hbase_default",
    )

    # List backup sets
    list_backup_sets = HBaseBackupSetOperator(
        task_id="list_backup_sets",
        action="list",
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseCreateBackupOperator:

Creating Backups
""""""""""""""""

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseCreateBackupOperator` operator is used to create full or incremental backups of HBase tables.

Use the ``backup_type`` parameter to specify 'full' or 'incremental', ``backup_path`` for the HDFS storage location, and either ``backup_set_name`` or ``tables`` to specify what to backup.

.. code-block:: python

    # Full backup using backup set
    full_backup = HBaseCreateBackupOperator(
        task_id="full_backup",
        backup_type="full",
        backup_path="hdfs://namenode:9000/hbase/backup",
        backup_set_name="my_backup_set",
        workers=4,
        hbase_conn_id="hbase_default",
    )

    # Incremental backup with specific tables
    incremental_backup = HBaseCreateBackupOperator(
        task_id="incremental_backup",
        backup_type="incremental",
        backup_path="hdfs://namenode:9000/hbase/backup",
        tables=["table1", "table2"],
        workers=2,
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseRestoreOperator:

Restoring from Backup
"""""""""""""""""""""

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseRestoreOperator` operator is used to restore tables from a backup to a specific point in time.

Use the ``backup_path`` parameter for the backup location, ``backup_id`` for the specific backup to restore, and either ``backup_set_name`` or ``tables`` to specify what to restore.

.. code-block:: python

    # Restore from backup set
    restore_backup = HBaseRestoreOperator(
        task_id="restore_backup",
        backup_path="hdfs://namenode:9000/hbase/backup",
        backup_id="backup_1234567890123",
        backup_set_name="my_backup_set",
        overwrite=True,
        hbase_conn_id="hbase_default",
    )

    # Restore specific tables
    restore_tables = HBaseRestoreOperator(
        task_id="restore_tables",
        backup_path="hdfs://namenode:9000/hbase/backup",
        backup_id="backup_1234567890123",
        tables=["table1", "table2"],
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseBackupHistoryOperator:

Viewing Backup History
""""""""""""""""""""""

The :class:`~airflow.providers.apache.hbase.operators.hbase.HBaseBackupHistoryOperator` operator is used to retrieve backup history information.

Use the ``backup_set_name`` parameter to get history for a specific backup set, or ``backup_path`` to get history for a backup location.

.. code-block:: python

    # Get backup history for a backup set
    backup_history = HBaseBackupHistoryOperator(
        task_id="backup_history",
        backup_set_name="my_backup_set",
        hbase_conn_id="hbase_default",
    )

    # Get backup history for a path
    path_history = HBaseBackupHistoryOperator(
        task_id="path_history",
        backup_path="hdfs://namenode:9000/hbase/backup",
        hbase_conn_id="hbase_default",
    )

Reference
^^^^^^^^^

For further information, look at `HBase documentation <https://hbase.apache.org/book.html>`_ and `HBase Backup and Restore <https://hbase.apache.org/book.html#_backup_and_restore>`_.