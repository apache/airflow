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



.. _howto/operator:ClickHouseOperator:

Operators
=======================
You can build your own Operator hook in :class:`~airflow.providers.clickhouse.hooks.ClickHouseHook`,


Use the :class:`~airflow.providers.clickhouse.operators.ClickHouseOperator` to execute
SQL query in `ClickHouse <https://www.clickhouse.com/>`__.

You can further process your result using :class:`~airflow.providers.clickhouse.operators.ClickHouseOperator` and
further process the result using **result_processor** Callable as you like.

An example of Listing all rows in **gettingstarted.clickstream** table can be implemented as following:

.. exampleinclude:: /../../airflow/providers/clickhouse/example_dags/example_clickhouse.py
    :language: python
    :start-after: [START howto_operator_clickhouse]
    :end-before: [END howto_operator_clickhouse]

You can also provide file template (.sql) to load query, remember path is relative to **dags/** folder, if you want to provide any other path
please provide **template_searchpath** while creating **DAG** object,

.. exampleinclude:: /../../airflow/providers/clickhouse/example_dags/example_clickhouse.py
    :language: python
    :start-after: [START howto_operator_template_file_clickhouse]
    :end-before: [END howto_operator_template_file_clickhouse]

Furthermore, you can also query into default database ( provided in ClickHouse Connection ) without using it inside the query as follows,

.. exampleinclude:: /../../airflow/providers/clickhouse/example_dags/example_clickhouse.py
    :language: python
    :start-after: [START howto_operator_clickhouse_with_database]
    :end-before: [END howto_operator_clickhouse_with_database]

Sensors
=======

Use the :class:`~airflow.providers.clickhouse.sensors.ClickHouseSensor` to wait for a condition using
SQL query in `ClickHouse <https://www.clickhouse.com/>`__.

An example for waiting a record **gettingstarted.clickstream** table with **customer_id** as **customer_1** as follows

.. exampleinclude:: /../../airflow/providers/clickhouse/example_dags/example_clickhouse.py
    :language: python
    :start-after: [START howto_sensor_clickhouse]
    :end-before: [END howto_sensor_clickhouse]

Similar to **ClickHouseOperator**, You can also provide file template to load query -

.. exampleinclude:: /../../airflow/providers/clickhouse/example_dags/example_clickhouse.py
    :language: python
    :start-after: [START howto_sensor_template_file_clickhouse]
    :end-before: [END howto_sensor_template_file_clickhouse]
