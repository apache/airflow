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

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase_advanced.py
    :language: python
    :start-after: [START howto_operator_hbase_batch_put]
    :end-before: [END howto_operator_hbase_batch_put]

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

Reference
^^^^^^^^^

For further information, look at `HBase documentation <https://hbase.apache.org/book.html>`_.