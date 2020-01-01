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

Neo4j Operator
===========

This operator enables Airflow DAGs to execute cypher queries against a Neo4j (or ONgDB) graph database.


The results of the query execution will be written to a CSV file on disk. Please consider available space
on the Airflow worker if your query can return a large result.

This operator can be used in conjunction with the S3 operator or email operator to process the results.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^
To use this operator you must define a connection to your Neo4J/ONgDB database via:

  *Admin* -> *Connections* -> *Create*

The connection must set the following properties:
 - login
 - password
 - host
 - port

The connection name is then used in the DAG to reference this definition.

Basic Usage
^^^^^^^^^^^
Use the :class:`airflow.providers.neo4j.operators.neo4j_operator.Neo4JOperator` to execute cypher query:

.. exampleinclude:: ../../../airflow/providers/neo4j/example_dags/example_neo4j_operator.py
    :language: python
    :start-after: [START howto_operator_neo4j]
    :end-before: [END howto_operator_neo4j]

Queries can be managed in text files on disk and read in by placing the file name in the ``cypher_query`` parameter

.. exampleinclude:: ../../../airflow/providers/neo4j/example_dags/example_neo4j_operator.py
    :language: python
    :start-after: [START howto_operator_neo4j_from_file]
    :end-before: [END howto_operator_neo4j_from_file]

Options
^^^^^^^
The following options are available for this operator:
cypher_query: Text string with the query to execute:

- cypher_query: String representing a valid cypher query.
  eg. "MATCH (n) RETURN (n)"
  *OR*
  filename containing a cypher query to be executed
  eg. "my_query.cypher
- output_filename: String of filename to write the output to.
- n4j_conn_id: String of the connection name defined in Airflow
- soft_fail: Bool indicating if the task should fail if no results are returned

More information
^^^^^^^^^^^^^^^^

See Neo4j documentation on how to interact via Python `Neo4j Python Drivers <https://neo4j.com/docs/api/python-driver/current />`__.

See Neo4j documentation on how to write cypher queries `Cypher Query Language <https://neo4j.com/developer/cypher-query-language/ />`__.
