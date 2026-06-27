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

.. exampleinclude:: /../../openai/tests/system/openai/example_openai.py
    :language: python
    :start-after: [START howto_operator_openai_embedding]
    :end-before: [END howto_operator_openai_embedding]

.. _howto/operator:OpenAIResponseOperator:

OpenAIResponseOperator
=======================

Use the :class:`~airflow.providers.openai.operators.openai.OpenAIResponseOperator` to generate a
model response with the OpenAI Responses API, OpenAI's recommended interface for text generation and
tool use. The operator returns the response's aggregated output text.

Using the Operator
^^^^^^^^^^^^^^^^^^^

The OpenAIResponseOperator requires the ``input_text`` prompt. Use the ``conn_id`` parameter to
specify the OpenAI connection to use, and ``response_kwargs`` to pass through options such as
``tools``, ``conversation`` or ``previous_response_id``.

.. exampleinclude:: /../../openai/tests/system/openai/example_openai.py
    :language: python
    :start-after: [START howto_operator_openai_response]
    :end-before: [END howto_operator_openai_response]

Using the OpenAIHook for Responses and Conversations
=====================================================

The :class:`~airflow.providers.openai.hooks.openai.OpenAIHook` exposes the Responses and
Conversations APIs directly for use inside ``@task`` functions or custom operators:

- Responses: ``create_response``, ``get_response``, ``delete_response`` and ``cancel_response``
  (the last cancels a response created with ``background=True``).
- Conversations: ``create_conversation``, ``get_conversation``, ``update_conversation`` and
  ``delete_conversation``. Pass the conversation id to ``create_response`` (via the operator's
  ``response_kwargs`` or the hook) to persist state across responses.

For example, to create a conversation and continue it across responses:

.. code-block:: python

    hook = OpenAIHook()
    conversation = hook.create_conversation()
    hook.create_response(input="Hello", conversation=conversation.id)

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

.. exampleinclude:: /../../openai/tests/system/openai/example_trigger_batch_operator.py
    :language: python
    :start-after: [START howto_operator_openai_trigger_operator]
    :end-before: [END howto_operator_openai_trigger_operator]
