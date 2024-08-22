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

.. _howto/operator:OpenAIEmbeddingOperator:

OpenAIEmbeddingOperator
========================

Use the :class:`~airflow.providers.open_ai.operators.open_ai.OpenAIEmbeddingOperator` to
interact with Open APIs to create embeddings for given text.


Using the Operator
^^^^^^^^^^^^^^^^^^

The OpenAIEmbeddingOperator requires the ``input_text`` as an input to embedding API. Use the ``conn_id`` parameter to specify the OpenAI connection to use to
connect to your account.

An example using the operator is in way:

.. exampleinclude:: /../../tests/system/providers/openai/example_openai.py
    :language: python
    :start-after: [START howto_operator_openai_embedding]
    :end-before: [END howto_operator_openai_embedding]

.. _howto/operator:OpenAITriggerBatchOperator:

OpenAITriggerBatchOperator
===========================

Use the :class:`~airflow.providers.open_ai.operators.open_ai.OpenAITriggerBatchOperator` to
interact with Open APIs to trigger a batch job. This operator is used to trigger a batch job and wait for the job to complete.


Using the Operator
^^^^^^^^^^^^^^^^^^

The OpenAITriggerBatchOperator requires the prepared batch file as an input to trigger the batch job. Provide the ``file_id`` and the ``endpoint`` to trigger the batch job.
Use the ``conn_id`` parameter to specify the OpenAI connection to use to


The OpenAITriggerBatchOperator

An example using the operator is in way:

.. exampleinclude:: /../../tests/system/providers/openai/example_trigger_batch_operator.py
    :language: python
    :start-after: [START howto_operator_openai_trigger_operator]
    :end-before: [END howto_operator_openai_trigger_operator]
