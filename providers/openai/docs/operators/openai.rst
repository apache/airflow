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

Use the :class:`~airflow.providers.openai.operators.openai.OpenAIEmbeddingOperator` to
interact with the OpenAI API to create embeddings for given text.


Using the Operator
^^^^^^^^^^^^^^^^^^

The OpenAIEmbeddingOperator requires the ``input_text`` as an input to embedding API. Use the ``conn_id`` parameter to specify the OpenAI connection to use to
connect to your account.

An example of using the operator:

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

Passing Responses API options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``response_kwargs`` passes straight through to the underlying ``create_response`` call, so any
keyword argument the Responses API accepts can be set there. Options worth knowing about:

- ``background``: run the response asynchronously on OpenAI's side. See the note below before
  using this with ``OpenAIResponseOperator``.
- ``store``: whether the response is retained on OpenAI's side, for example so it can later be
  used as a ``previous_response_id``.
- ``reasoning``: reasoning configuration for reasoning models.
- ``service_tier``: one of ``'auto'``, ``'default'``, ``'flex'``, ``'scale'`` or ``'priority'``,
  selecting the processing tier the request is served from.
- ``prompt_cache_key``: an identifier used to route requests to the same prompt cache.
- ``safety_identifier``: a stable identifier for the end user, used for safety and abuse
  detection.
- ``truncation``: one of ``'auto'`` or ``'disabled'``, controlling whether the model truncates
  context that exceeds its window.
- ``include``: additional output fields to include in the response, such as encrypted reasoning
  content.
- ``metadata``: a mapping of key-value pairs attached to the response for your own bookkeeping.
- ``max_output_tokens``: an upper bound on the number of tokens the model can generate.
- ``max_tool_calls``: an upper bound on the number of built-in tool calls the model can make.

.. note::

    OpenAI does not expose a spend or cost ceiling parameter on the Responses API.
    ``max_output_tokens`` and ``max_tool_calls`` are token and call-count limits, not a way to cap
    the dollar cost of a run; controlling spend means bounding those counts yourself.

.. note::

    ``background=True`` starts the response running asynchronously on OpenAI's side and returns
    before the response finishes. ``OpenAIResponseOperator`` is synchronous: it makes one
    ``create_response`` call and returns ``response.output_text`` immediately, so a response
    started with ``background=True`` comes back incomplete, and the operator logs its own warning
    because ``response.status`` is not yet ``"completed"``. Use ``background=True`` only when you
    plan to poll for completion or cancel the response through
    :class:`~airflow.providers.openai.hooks.openai.OpenAIHook` directly, not through this
    operator.

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

.. note::

    The Assistants/Threads hook methods (``create_assistant``, ``create_thread``, ``create_run`` and
    related) are deprecated, mirroring OpenAI's deprecation of the Assistants API. Migrate to the
    Responses and Conversations methods above.

.. _howto/operator:OpenAITriggerBatchOperator:

OpenAITriggerBatchOperator
===========================

Use the :class:`~airflow.providers.openai.operators.openai.OpenAITriggerBatchOperator` to
interact with the OpenAI API to trigger a batch job. This operator is used to trigger a batch job and wait for the job to complete.


Using the Operator
^^^^^^^^^^^^^^^^^^

The OpenAITriggerBatchOperator requires the prepared batch file as an input to trigger the
batch job. Provide the ``file_id`` and the ``endpoint`` to trigger the batch job, and use the
``conn_id`` parameter to specify the OpenAI connection to use.

An example of using the operator:

.. exampleinclude:: /../../openai/tests/system/openai/example_trigger_batch_operator.py
    :language: python
    :start-after: [START howto_operator_openai_trigger_operator]
    :end-before: [END howto_operator_openai_trigger_operator]
