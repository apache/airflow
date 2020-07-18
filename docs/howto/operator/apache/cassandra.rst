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


Apache Cassandra Operators
======================

.. contents::
  :depth: 1
  :local:

Prerequisite
------------

To use ``CassandraTableSensor`` and ``CassandraRecordSensor``, you must configure a :doc:`Cassandra Connection <../../connection/cassandra>`.

:ref:`howto/operator:CassandraTableSensor`

CassandraTableSensor
-----------------

Checks for the existence of a table in a Cassandra cluster.

For parameter definition take a look at :class:`~airflow.providers.apache.cassandra.sensors.table.CassandraTableSensor`.

Use the ``table`` parameter to poke until the provided table is found. Use dot notation to target a specific keyspace.

.. exampleinclude:: /../airflow/providers/apache/cassandra/example_dags/example_cassandra_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cassandra_table_sensor]
    :end-before: [END howto_operator_cassandra_table_sensor]


:ref:`howto/operator:CassandraRecordSensor`

CassandraRecordSensor
-----------------

Checks for the existence of a record in a Cassandra cluster.

For parameter definition take a look at :class:`~airflow.providers.apache.cassandra.sensors.record.CassandraRecordSensor`.

Use the ``table`` parameter to mention the keyspace and table for the record. Use dot notation to target a specific keyspace.

Use the ``keys`` parameter to poke until the provided record is found. The existence of record is identified using key value pairs. In the given example, we're are looking for value ``v1`` in column ``p1`` and ``v2`` in column ``p2``.

.. exampleinclude:: /../airflow/providers/apache/cassandra/example_dags/example_cassandra_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cassandra_record_sensor]
    :end-before: [END howto_operator_cassandra_record_sensor]

Reference
"""""""""

For further information, look at `Cassandra Query Language (CQL) SELECT statement <https://cassandra.apache.org/doc/latest/cql/dml.html#select>`_.
