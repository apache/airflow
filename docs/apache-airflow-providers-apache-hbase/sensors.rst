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



Apache HBase Sensors
====================

`Apache HBase <https://hbase.apache.org/>`__ sensors allow you to monitor the state of HBase tables and data.

Prerequisite
------------

To use sensors, you must configure an :doc:`HBase Connection <connections/hbase>`.

.. _howto/sensor:HBaseTableSensor:

Waiting for a Table to Exist
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.sensors.hbase.HBaseTableSensor` sensor is used to check for the existence of a table in HBase.

Use the ``table_name`` parameter to specify the table to monitor.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase.py
    :language: python
    :start-after: [START howto_sensor_hbase_table]
    :end-before: [END howto_sensor_hbase_table]

.. _howto/sensor:HBaseRowSensor:

Waiting for a Row to Exist
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.sensors.hbase.HBaseRowSensor` sensor is used to check for the existence of a specific row in an HBase table.

Use the ``table_name`` parameter to specify the table and ``row_key`` parameter to specify the row to monitor.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase.py
    :language: python
    :start-after: [START howto_sensor_hbase_row]
    :end-before: [END howto_sensor_hbase_row]

.. _howto/sensor:HBaseRowCountSensor:

Waiting for Row Count Threshold
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.sensors.hbase.HBaseRowCountSensor` sensor is used to check if the number of rows in an HBase table meets a specified threshold.

Use the ``table_name`` parameter to specify the table, ``expected_count`` for the threshold, and ``comparison`` to specify the comparison operator ('>=', '>', '==', '<', '<=').

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase_advanced.py
    :language: python
    :start-after: [START howto_sensor_hbase_row_count]
    :end-before: [END howto_sensor_hbase_row_count]

.. _howto/sensor:HBaseColumnValueSensor:

Waiting for Column Value
^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hbase.sensors.hbase.HBaseColumnValueSensor` sensor is used to check if a specific column in a row contains an expected value.

Use the ``table_name`` parameter to specify the table, ``row_key`` for the row, ``column`` for the column to check, and ``expected_value`` for the value to match.

.. exampleinclude:: /../../airflow/providers/hbase/example_dags/example_hbase_advanced.py
    :language: python
    :start-after: [START howto_sensor_hbase_column_value]
    :end-before: [END howto_sensor_hbase_column_value]

Reference
^^^^^^^^^

For further information, look at `HBase documentation <https://hbase.apache.org/book.html>`_.