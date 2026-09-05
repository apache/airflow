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

.. _howto/operator:CohereRerankOperator:

CohereRerankOperator
====================

Use :class:`~airflow.providers.cohere.operators.rerank.CohereRerankOperator` to reorder
documents by their relevance to a query with Cohere's
`Rerank API <https://docs.cohere.com/docs/rerank-overview>`__.

Before you begin
^^^^^^^^^^^^^^^^

Configure a :ref:`Cohere connection <howto/connection:cohere>`. The operator uses the
``cohere_default`` connection unless another ``conn_id`` is provided.

The operator requires:

* ``query``: The search query used to evaluate relevance.
* ``documents``: A list of text documents to rank.

The model is configured by the hook. Use ``top_n`` to limit the number of results and
``max_tokens_per_doc`` to control how much of each document Cohere processes. The query,
documents, and both limits are templated fields.

Using the operator
^^^^^^^^^^^^^^^^^^

.. exampleinclude:: /../../cohere/tests/system/cohere/example_cohere_rerank_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cohere_rerank]
    :end-before: [END howto_operator_cohere_rerank]

Output
^^^^^^

The operator converts the Cohere response to an XCom-serializable dictionary. Its ``results``
list is ordered from most to least relevant. Each result contains the original document's
zero-based ``index`` and its ``relevance_score``. Use the index to associate a result with the
corresponding item in the input ``documents`` list.

An abbreviated response looks like this:

.. code-block:: json

    {
      "id": "rerank-request-id",
      "results": [
        {"index": 1, "relevance_score": 0.99},
        {"index": 0, "relevance_score": 0.12}
      ]
    }
