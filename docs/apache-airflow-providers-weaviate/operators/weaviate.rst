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

.. _howto/operator:WeaviateIngestOperator:

WeaviateIngestOperator
========================

Use the :class:`~airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator` to
interact with Weaviate APIs to create embeddings for given text and ingest into the database.
Alternatively, you could also provide custom vectors for your text that can be ingested
into the database.

Using the Operator
^^^^^^^^^^^^^^^^^^

The WeaviateIngestOperator requires the ``input_data`` as an input to the operator. Use the ``conn_id`` parameter to specify the Weaviate connection to use to
connect to your account.

An example using the operator to ingest data with custom vectors retrieved from XCOM:

.. exampleinclude:: /../../providers/tests/system/weaviate/example_weaviate_operator.py
    :language: python
    :start-after: [START howto_operator_weaviate_embedding_and_ingest_xcom_data_with_vectors]
    :end-before: [END howto_operator_weaviate_embedding_and_ingest_xcom_data_with_vectors]

An example using the operator to ingest data with custom vectors retrieved from a python callable:

.. exampleinclude:: /../../providers/tests/system/weaviate/example_weaviate_operator.py
    :language: python
    :start-after: [START howto_operator_weaviate_embedding_and_ingest_callable_data_with_vectors]
    :end-before: [END howto_operator_weaviate_embedding_and_ingest_callable_data_with_vectors]

An example using the operator to ingest data without vectors retrieved from XCOM for which the operator would generate embedding vectors:

.. exampleinclude:: /../../providers/tests/system/weaviate/example_weaviate_operator.py
    :language: python
    :start-after: [START howto_operator_weaviate_ingest_xcom_data_without_vectors]
    :end-before: [END howto_operator_weaviate_ingest_xcom_data_without_vectors]

An example using the operator to ingest data without vectors retrieved from a python callable for which the operator would generate embedding vectors:

.. exampleinclude:: /../../providers/tests/system/weaviate/example_weaviate_operator.py
    :language: python
    :start-after: [START howto_operator_weaviate_ingest_callable_data_without_vectors]
    :end-before: [END howto_operator_weaviate_ingest_callable_data_without_vectors]
