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

.. _howto/operator:VoyageEmbeddingOperator:

VoyageEmbeddingOperator
========================

Use the :class:`~airflow.providers.voyageai.operators.embedding.VoyageEmbeddingOperator` to generate text embeddings using the Voyage AI API.

Using the Operator
^^^^^^^^^^^^^^^^^^

The ``VoyageEmbeddingOperator`` requires a list of texts via the ``input_texts`` parameter and a configured ``conn_id`` pointing to your Voyage AI connection. The operator will push a list of embedding vectors to XComs.

The example below shows how to use the operator to generate embeddings and then process them in a downstream task.

.. exampleinclude:: /../../voyageai/tests/system/voyageai/example_embed_dag.py
    :language: python
    :start-after: [START howto_operator_voyageai_embed]
    :end-before: [END howto_operator_voyageai_embed]
