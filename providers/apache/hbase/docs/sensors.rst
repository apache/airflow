

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

HBase Sensors
=============

The HBase provider provides two sensors for waiting on table or row
availability.

Prerequisites
-------------

* A HBase Thrift2 server must be running.
* An ``hbase`` connection must be configured in Airflow.

HBaseTableSensor
----------------

Wait for an HBase table to exist.

.. code-block:: python

    from airflow.providers.apache.hbase.sensors.hbase import HBaseTableSensor

    HBaseTableSensor(
        task_id="wait_for_table",
        table_name="ingestion_table",
        hbase_conn_id="hbase_thrift2",
        timeout=300,
        poke_interval=10,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Name of the table to check (templated).
   * - ``hbase_conn_id``
     - Connection ID (default: ``hbase_default``).

Inherits ``timeout``, ``poke_interval``, and ``mode`` from
:class:`~airflow.sdk.bases.sensor.BaseSensorOperator`.

HBaseRowSensor
--------------

Wait for a specific row to exist in an HBase table.

.. code-block:: python

    from airflow.providers.apache.hbase.sensors.hbase import HBaseRowSensor

    HBaseRowSensor(
        task_id="wait_for_row",
        table_name="ingestion_table",
        row_key="data_ready",
        hbase_conn_id="hbase_thrift2",
        timeout=300,
        poke_interval=10,
    )

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``table_name``
     - Name of the table to check (templated).
   * - ``row_key``
     - Row key to check for existence (templated).
   * - ``hbase_conn_id``
     - Connection ID (default: ``hbase_default``).

Use case: Data ingestion pipeline
----------------------------------

Sensors are useful when an external ETL system writes data to HBase and
Airflow needs to wait for the data to be available before processing:

.. code-block:: python

    wait_for_table = HBaseTableSensor(task_id="wait_for_table", ...)
    wait_for_row = HBaseRowSensor(task_id="wait_for_row", ...)
    process_data = PythonOperator(task_id="process", ...)

    wait_for_table >> wait_for_row >> process_data
