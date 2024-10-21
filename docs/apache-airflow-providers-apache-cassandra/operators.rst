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
==========================

`Apache Cassandra <https://cassandra.apache.org/>`__ is an open source distributed NoSQL database that can be used when you need scalability and high availability without compromising performance. It offers linear scalability and fault-tolerance on commodity hardware or cloud infrastructure which makes it the perfect platform for mission-critical data. It supports multi-datacenter replication with lower latencies.

Prerequisite
------------

To use operators, you must configure a :doc:`Cassandra Connection <connections/cassandra>`.

.. _howto/operator:CassandraTableSensor:

Waiting for a Table to be created
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.cassandra.sensors.table.CassandraTableSensor` operator is used to check for the existence of a table in a Cassandra cluster.

Use the ``table`` parameter (set in ``default_args`` in the example below) to poke until the provided table is found. Use dot notation to target a specific keyspace.

.. _howto/operator:CassandraRecordSensor:

Waiting for a Record to be created
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.cassandra.sensors.record.CassandraRecordSensor` operator is used to check for the existence of a record of a table in the Cassandra cluster.

Use the ``table`` parameter (set in ``default_args`` in the example below) to mention the keyspace and table for the record. Use dot notation to target a specific keyspace.

Use the ``keys`` parameter to poke until the provided record is found. The existence of record is identified using key value pairs. In the given example, we're are looking for value ``v1`` in column ``p1`` and ``v2`` in column ``p2``.

Example use of these sensors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../providers/tests/system/apache/cassandra/example_cassandra_dag.py
    :language: python
    :start-after: [START howto_operator_cassandra_sensors]
    :end-before: [END howto_operator_cassandra_sensors]

Reference
^^^^^^^^^

For further information, look at `Cassandra Query Language (CQL) SELECT statement <https://cassandra.apache.org/doc/latest/cql/dml.html#select>`_.
