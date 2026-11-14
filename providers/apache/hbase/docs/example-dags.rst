

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

Example DAGs
============

Example DAGs are located in:
``airflow/providers/apache/hbase/src/airflow/providers/apache/hbase/example_dags``.

All example DAGs require a running HBase Thrift2 server and an ``hbase``
connection configured in Airflow.

Configuration layers
--------------------

Example DAGs support multiple configuration methods, from highest to lowest
priority:

1. **Trigger UI / ``dag_run.conf``** - override values at trigger time.
2. **Airflow Variables** - global settings, less granular.
3. **Environment variables** - DAG-level defaults.

Available example DAGs
----------------------

``example_hbase.py`` - Basic Thrift2 operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates fundamental HBase Thrift2 operations:

* Delete table (for idempotency)
* Create table with column families
* Check table exists (``HBaseTableSensor``)
* Put data row
* Check row exists (``HBaseRowSensor``)
* Delete table

Connection ID: ``hbase_thrift2``.

``example_hbase_batch.py`` - Batch operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates batch CRUD with operators:

* ``HBaseBatchPutOperator`` for bulk inserts
* ``HBaseBatchGetOperator`` for bulk reads
* ``HBaseScanOperator`` for range scans
* ``HBaseCreateTableOperator`` / ``HBaseDeleteTableOperator``

Connection ID: ``hbase_thrift2``.

``example_hbase_batch_optimized.py`` - Optimized bulk operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shows performance-optimized bulk operations using ``batch_size``
and ``max_workers`` parameters for efficient data processing.

* ``HBaseBatchPutOperator`` with tuned parameters
* Demonstrates chunk-based parallelism

Connection ID: ``hbase_thrift2``.

``example_hbase_pool.py`` - Connection pool performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compares single connection vs pooled connection performance
for high-throughput scenarios. Uses two connection IDs:

* ``hbase_thrift2`` - single connection (no pool)
* ``hbase_thrift2_pooled`` - pooled connection

Requires ``connection_pool.enabled: true`` in the pooled connection extra.

``example_hbase_sensors.py`` - Sensor pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Simulates a real-world data ingestion pipeline:

* External system creates table and writes data (``PythonOperator``)
* ``HBaseTableSensor`` waits for table to appear
* ``HBaseRowSensor`` waits for control row indicating data is ready
* ``PythonOperator`` processes the data
* Cleanup

Connection ID: ``hbase_thrift2``.

``example_hbase_cli_commands.py`` - CLI commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates ``HBaseCLIHook.execute_command`` for arbitrary HBase CLI
operations such as backup set management.

Requires HBase CLI tools installed on the worker.

``example_hbase_backup_producer.py`` - Backup producer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generates data and produces a dataset event to trigger the backup
consumer DAG:

* Create test table with 1,000 records
* Batch put data
* Produce dataset via ``outlets`` parameter

Uses ``hbase_table_dataset()`` for data-aware scheduling.

``example_hbase_backup_consumer.py`` - Backup consumer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Automatically triggered by dataset updates:

* ``BranchPythonOperator`` decides backup type (FULL first, then INCREMENTAL)
* ``HBaseCreateBackupOperator`` executes full or incremental backup
* ``HBaseBackupHistoryOperator`` retrieves backup history

Requires HDFS backup directory (``hdfs dfs -mkdir /hbase/backup``)
and HBase CLI tools.

``example_hbase_restore.py`` - Restore operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates HBase restore with data verification:

* Delete table (simulate data loss)
* Restore from backup using ``HBaseRestoreOperator``
* Verify data with ``PythonOperator``

Backup ID can be provided via ``dag_run.conf`` or XCom from a previous
backup DAG run.

DAG developer notes
-------------------

* All example DAGs use ``catchup=False`` - they do not backfill.
* Connection IDs in examples are configurable via Airflow connections;
  create the required connections before running.
* Backup/restore examples require HDFS and HBase CLI installed on the
  worker host.
* For connection pool examples, create both a plain and a pooled
  connection with different extra configurations.
