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



.. _howto/operator:AQLOperator:

Operators
=======================
You can build your own Operator hook in :class:`~airflow.providers.arangodb.hooks.arangodb.ArangoDBHook`.

Use the :class:`~airflow.providers.arangodb.operators.arangodb.AQLOperator` to execute
AQL query in `ArangoDB <https://www.arangodb.com/>`__.

You can further process your result using :class:`~airflow.providers.arangodb.operators.arangodb.AQLOperator` and
further process the result using :class:`result_processor <airflow.providers.arangodb.operators.arangodb.AQLOperator>`
Callable as you like.

An example of Listing all Documents in **students** collection can be implemented as following:

.. exampleinclude:: /../../airflow/providers/arangodb/example_dags/example_arangodb.py
    :language: python
    :start-after: [START howto_aql_operator_arangodb]
    :end-before: [END howto_aql_operator_arangodb]

You can also provide file template (.sql) to load query, remember path is relative to **dags/** folder, if you want to provide any other path
please provide **template_searchpath** while creating **DAG** object,

.. exampleinclude:: /../../airflow/providers/arangodb/example_dags/example_arangodb.py
    :language: python
    :start-after: [START howto_aql_operator_template_file_arangodb]
    :end-before: [END howto_aql_operator_template_file_arangodb]

Sensors
========

Use the :class:`~airflow.providers.arangodb.sensors.arangodb.AQLSensor` to wait for a document or collection using
AQL query in `ArangoDB <https://www.arangodb.com/>`__.

An example for waiting a document in **students** collection with student name **judy** can be implemented as following:

.. exampleinclude:: /../../airflow/providers/arangodb/example_dags/example_arangodb.py
    :language: python
    :start-after: [START howto_aql_sensor_arangodb]
    :end-before: [END howto_aql_sensor_arangodb]

Similar to **AQLOperator**, You can also provide file template to load query -

.. exampleinclude:: /../../airflow/providers/arangodb/example_dags/example_arangodb.py
    :language: python
    :start-after: [START howto_aql_sensor_template_file_arangodb]
    :end-before: [END howto_aql_sensor_template_file_arangodb]
