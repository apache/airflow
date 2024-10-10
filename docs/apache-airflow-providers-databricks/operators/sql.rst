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

.. _howto/operator:DatabricksSqlOperator:


DatabricksSqlOperator
=====================

Use the :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksSqlOperator` to execute SQL
on a `Databricks SQL warehouse  <https://docs.databricks.com/sql/admin/sql-endpoints.html>`_ or a
`Databricks cluster <https://docs.databricks.com/clusters/index.html>`_.


Using the Operator
------------------

Operator executes given SQL queries against configured warehouse. The only required parameters are:

* ``sql`` - SQL queries to execute. There are 3 ways of specifying SQL queries:

  1. Simple string with SQL statement.
  2. List of strings representing SQL statements.
  3. Name of the file with SQL queries. File must have ``.sql`` extension. Each query should finish with ``;<new_line>``

* One of ``sql_warehouse_name`` (name of Databricks SQL warehouse to use) or ``http_path`` (HTTP path for Databricks SQL warehouse or Databricks cluster).

Other parameters are optional and could be found in the class documentation.

Examples
--------

Selecting data
^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to select data from a table is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_select]
    :end-before: [END howto_operator_databricks_sql_select]

Selecting data into a file
^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to select data from a table and store in a file is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_select_file]
    :end-before: [END howto_operator_databricks_sql_select_file]

Executing multiple statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to perform multiple SQL statements is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_multiple]
    :end-before: [END howto_operator_databricks_sql_multiple]


Executing multiple statements from a file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSqlOperator to perform statements from a file is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sql.py
    :language: python
    :start-after: [START howto_operator_databricks_sql_multiple_file]
    :end-before: [END howto_operator_databricks_sql_multiple_file]


DatabricksSqlSensor
===================

Use the :class:`~airflow.providers.databricks.sensors.sql.DatabricksSqlSensor` to run the sensor
for a table accessible via a Databricks SQL warehouse or interactive cluster.

Using the Sensor
----------------

The sensor executes the SQL statement supplied by the user. The only required parameters are:

* ``sql`` - SQL query to execute for the sensor.

* One of ``sql_warehouse_name`` (name of Databricks SQL warehouse to use) or ``http_path`` (HTTP path for Databricks SQL warehouse or Databricks cluster).

Other parameters are optional and could be found in the class documentation.

Examples
--------
Configuring Databricks connection to be used with the Sensor.

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_databricks_connection_setup]
    :end-before: [END howto_sensor_databricks_connection_setup]

Poking the specific table with the SQL statement:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_databricks_sql]
    :end-before: [END howto_sensor_databricks_sql]


DatabricksPartitionSensor
=========================

Sensors are a special type of Operator that are designed to do exactly one thing - wait for something to occur. It can be time-based, or waiting for a file, or an external event, but all they do is wait until something happens, and then succeed so their downstream tasks can run.

For the Databricks Partition Sensor, we check if a partition and its related value exists and if not, it waits until the partition value arrives. The waiting time and interval to check can be configured in the timeout and poke_interval parameters respectively.

Use the :class:`~airflow.providers.databricks.sensors.partition.DatabricksPartitionSensor` to run the sensor
for a table accessible via a Databricks SQL warehouse or interactive cluster.

Using the Sensor
----------------

The sensor accepts the table name and partition name(s), value(s) from the user and generates the SQL query to check if
the specified partition name, value(s) exist in the specified table.

The required parameters are:

* ``table_name`` (name of the table for partition check).

* ``partitions`` (name of the partitions to check).

* ``partition_operator`` (comparison operator for partitions, to be used for range or limit of values, such as partition_name >= partition_value). `Databricks comparison operators <https://docs.databricks.com/sql/language-manual/sql-ref-null-semantics.html#comparison-operators>`_ are supported.

*   One of ``sql_warehouse_name`` (name of Databricks SQL warehouse to use) or ``http_path`` (HTTP path for Databricks SQL warehouse or Databricks cluster).

Other parameters are optional and can be found in the class documentation.

Examples
--------
Configuring Databricks connection to be used with the Sensor.

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_databricks_connection_setup]
    :end-before: [END howto_sensor_databricks_connection_setup]

Poking the specific table for existence of data/partition:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_databricks_partition]
    :end-before: [END howto_sensor_databricks_partition]
