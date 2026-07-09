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

.. _howto/operator:QdrantIngestOperator:

QdrantIngestOperator
======================

Use the :class:`~airflow.providers.qdrant.operators.qdrant.QdrantIngestOperator` to
ingest data into a Qdrant instance.


Using the Operator
^^^^^^^^^^^^^^^^^^

The QdrantIngestOperator requires the ``vectors`` as an input ingest into Qdrant. Use the ``conn_id`` parameter to
specify the Qdrant connection to connect to Qdrant instance. The vectors could also contain metadata referencing
the original text corresponding to the vectors that could be ingested into the database.

An example using the operator in this way:

.. exampleinclude:: /../../qdrant/tests/system/qdrant/example_dag_qdrant.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qdrant_ingest]
    :end-before: [END howto_operator_qdrant_ingest]


.. _howto/operator:QdrantSearchOperator:

QdrantSearchOperator
======================

Use the :class:`~airflow.providers.qdrant.operators.qdrant.QdrantSearchOperator` to
run a vector similarity search against a Qdrant collection and pull the top-k
matches back into the DAG for downstream tasks (for example, a retrieval step
in a RAG pipeline).

Using the Operator
^^^^^^^^^^^^^^^^^^

Pass the target ``collection_name`` and a ``query`` (typically a dense embedding
vector, but any query form accepted by
:meth:`~qdrant_client.QdrantClient.query_points` also works). The operator pushes
the results to XCom as a list of dictionaries -- one per matched point, with
``id``, ``score``, ``payload`` and (optionally) ``vector`` keys -- so downstream
tasks can consume them without any custom serialization.

An example using the operator downstream of an ingest task:

.. exampleinclude:: /../../qdrant/tests/system/qdrant/example_dag_qdrant.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_qdrant_search]
    :end-before: [END howto_operator_qdrant_search]
