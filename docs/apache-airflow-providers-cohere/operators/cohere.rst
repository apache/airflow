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

Use the :class:`~astronomer_providers_llm.providers.cohere.operators.embedding.CohereEmbeddingOperator` to
interact with Cohere APIs to create embeddings for a given text.


Using the Operator
^^^^^^^^^^^^^^^^^^

The CohereEmbeddingOperator requires the ``texts`` as an input to embedding API. Use the ``cohere_conn_id`` parameter to specify the Cohere connection to use to
connect to your account.

An example using the operator is in way:

Example Code:
-------------

.. code-block:: python

    from datetime import datetime

    from airflow import DAG

    from astronomer_providers_llm.providers.cohere.operators.embedding import CohereEmbeddingOperator

    with DAG("example_cohere_embedding", schedule=None, start_date=datetime(2023, 1, 1), catchup=False) as dag:
        texts = [
            "On Kernel-Target Alignment. We describe a family of global optimization procedures",
            " that automatically decompose optimization problems into smaller loosely coupled",
            " problems, then combine the solutions of these with message passing algorithms.",
        ]

        def get_text():
            return texts

        CohereEmbeddingOperator(input_text=texts, task_id="embedding_via_text")
        CohereEmbeddingOperator(input_callable=get_text, task_id="embedding_via_callable")
