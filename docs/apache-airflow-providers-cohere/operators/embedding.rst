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

.. _howto/operator:CohereEmbeddingOperator:

CohereEmbeddingOperator
========================

Use the :class:`~airflow.providers.cohere.operators.embedding.CohereEmbeddingOperator` to
interact with Cohere APIs to create embeddings for a given text.


Using the Operator
^^^^^^^^^^^^^^^^^^

The CohereEmbeddingOperator requires the ``input_text`` as an input to embedding API. Use the ``conn_id`` parameter to specify the Cohere connection to use to
connect to your account.

Example Code:
-------------

.. exampleinclude:: /../../providers/tests/system/cohere/example_cohere_embedding_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cohere_embedding]
    :end-before: [END howto_operator_cohere_embedding]
