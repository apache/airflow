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



.. _howto/operator:Neo4jOperator:

Neo4jOperator
=============

Use the :class:`~airflow.providers.neo4j.operators.Neo4jOperator` to execute
SQL commands in a `Neo4j <https://neo4j.com/>`__ database.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``neo4j_conn_id`` argument to connect to your Neo4j instance where
the connection metadata is structured as follows:

.. list-table:: Neo4j Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Neo4j hostname
   * - Schema: string
     - Database name
   * - Login: string
     - Neo4j user
   * - Password: string
     - Neo4j user password
   * - Port: int
     - Neo4j port

.. exampleinclude:: /../../providers/tests/system/neo4j/example_neo4j.py
    :language: python
    :dedent: 4
    :start-after: [START run_query_neo4j_operator]
    :end-before: [END run_query_neo4j_operator]
