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

A single string or token array returns one embedding vector. A list of strings or token arrays returns
one vector per input item in the same order.

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
tool use. The operator returns the response's aggregated output text. When ``do_xcom_push`` is
enabled (the default), ``execute`` also pushes two XCom keys: ``response_id`` (the response's ID,
usable as a downstream task's ``previous_response_id`` for chaining) and ``usage`` (the response's
token usage, or ``None`` when the API omits it). ``usage`` is the nested dict returned by
``ResponseUsage.model_dump()``: top-level ``input_tokens``, ``output_tokens`` and ``total_tokens``
counts, plus the nested ``input_tokens_details`` and ``output_tokens_details`` dicts.
``input_tokens_details.cached_tokens`` is part of the ``input_tokens`` total, not
additional to it, so pricing a run correctly means reading the breakdown rather than
treating ``input_tokens`` as a single uniformly priced count -- see OpenAI's `prompt
caching guide <https://platform.openai.com/docs/guides/prompt-caching>`_ for how
cached tokens are priced. Beyond that, ``usage`` reports token counts only -- OpenAI's
response carries no cost field, so turning any of these counts into a price means
multiplying by your own per-token rate. When ``usage`` is not ``None`` it also carries a
``try_number`` key recording which attempt produced it -- XCom is cleared at the start of
every attempt, so on a retried task instance the ``usage`` XCom only ever reflects the
most recent attempt, and ``try_number`` makes that scope explicit instead of letting it
silently under-report total spend across retries. Setting ``do_xcom_push=False`` skips both pushes.
It also disables the operator's own ``return_value`` XCom (standard ``BaseOperator``
behavior), so a downstream task reading ``openai_response.output`` -- which implicitly
reads the ``return_value`` key -- loses that value too.

Using the Operator
^^^^^^^^^^^^^^^^^^^

The OpenAIResponseOperator requires the ``input_text`` prompt. Use the ``conn_id`` parameter to
specify the OpenAI connection to use, and ``response_kwargs`` to pass through options such as
``tools``, ``conversation`` or ``previous_response_id``. ``response_kwargs`` is templated, so
``previous_response_id`` can reference a Dag's upstream ``response_id`` XCom directly. Since
``response_kwargs`` is templated, a literal ``{{ ... }}`` value you want sent to the API as-is
(for example inside a prompt's ``instructions``) must be wrapped in a ``{% raw %}`` block, for
example ``{% raw %}{{ not_a_variable }}{% endraw %}``.

Use ``max_output_tokens`` and ``max_tool_calls`` to cap generation per run -- both are templated,
so a ceiling can vary by environment or Dag run without hardcoding it. ``max_output_tokens`` caps
the number of tokens generated; ``max_tool_calls`` caps the number of built-in tool calls the model
may make. Both limits are enforced by the OpenAI API itself; OpenAI exposes no monetary cost limit
on the Responses API, so this operator has no cost cap. For a monetary limit, use
:doc:`apache-airflow-providers-common-ai:index` instead. Hitting ``max_output_tokens`` does not
fail the request: the response comes back with ``status="incomplete"``, so ``return_value`` will
not raise -- but it is not guaranteed to be truncated text either. A reasoning model can spend
the entire ceiling on reasoning tokens and return an empty ``output_text``, in which case
``return_value`` is an empty string. Hitting ``max_tool_calls`` is different: the OpenAI API
silently drops any tool calls beyond the ceiling without changing ``status`` or setting
``incomplete_details`` -- there is no log warning and no signal in ``return_value``, so a run
truncated by ``max_tool_calls`` looks identical to a clean run.

A rendered ``max_output_tokens`` or ``max_tool_calls`` that is blank or whitespace-only -- for
example ``max_output_tokens="{{ params.tokens | default('', true) }}"`` when ``params.tokens`` is
unset -- is treated as "no ceiling for this run" rather than raising. This only applies when the same run does
not also set the corresponding key in ``response_kwargs``: the mutually-exclusive-with-``response_kwargs``
check happens when the operator is constructed and fires regardless of what the template later
renders to.

.. exampleinclude:: /../../openai/tests/system/openai/example_openai.py
    :language: python
    :start-after: [START howto_operator_openai_response]
    :end-before: [END howto_operator_openai_response]

Passing Responses API options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the `Responses API reference
<https://platform.openai.com/docs/api-reference/responses/create>`__ for the authoritative list
of parameters. ``response_kwargs`` passes straight through to the underlying ``create_response``
call, so most keyword arguments the Responses API accepts can be set there, with the exceptions
noted below. What actually works also depends on the ``openai`` package version installed in the
environment, not the reference page above: ``Responses.create`` accepts no arbitrary keyword
arguments, so passing one the installed package doesn't recognize raises ``TypeError`` before any
request is sent. Use ``extra_body`` as a fallback to pass a parameter the installed package doesn't
know about yet. Options worth knowing about:

- ``background``: run the response asynchronously on OpenAI's side. See the note on ``background``
  below before using this with ``OpenAIResponseOperator``.
- ``stream``: return a stream of response events instead of a single completed response. Do not set
  this on ``OpenAIResponseOperator``: ``execute`` reads ``response.status`` and
  ``response.output_text``, neither of which exists on the streamed response object, so the task
  raises ``AttributeError``. Stream responses from a ``@task`` using
  :class:`~airflow.providers.openai.hooks.openai.OpenAIHook` instead.
- ``store``: whether the response is retained on OpenAI's side, for example so it can later be used
  as a ``previous_response_id``. When ``do_xcom_push`` is enabled, ``execute`` pushes ``response.id``
  to the ``response_id`` XCom regardless of ``store``, so a downstream task can retrieve it. If
  ``store=False``, the pushed id has no practical use: nothing was retained on OpenAI's side, so
  ``previous_response_id`` cannot reference it.
- ``previous_response_id``: the id of a prior response to continue a multi-turn conversation from.
  Cannot be used together with ``conversation`` — pass one or the other, not both.
- ``reasoning``: configuration for reasoning models, for example ``{"effort": ...}``. The example
  Dag above (and the operator's own default) uses ``gpt-4o-mini``, which is not a reasoning model,
  so this option only takes effect if ``model`` is also set to a reasoning model.
- ``service_tier``: the processing tier the request is served from.
- ``prompt_cache_key``: an identifier used to route requests to the same prompt cache. How long a
  cache entry is retained is a separate option whose name depends on the installed package:
  ``prompt_cache_retention`` at the 2.37.0 floor, deprecated in later releases in favor of
  ``prompt_cache_options.ttl``.
- ``safety_identifier``: a stable identifier for the end user, used for safety and abuse detection.
- ``truncation``: one of ``'auto'`` or ``'disabled'`` (the default). Under ``'disabled'``, a
  request whose input exceeds the model's context window fails with a 400 error; ``'auto'``
  shortens the input to fit instead.
- ``include``: additional output fields to include in the response, such as encrypted reasoning
  content. These fields land on ``response.output``, but ``response.output_text`` only aggregates
  ``message``/``output_text`` content, so anything ``include`` adds is fetched and then discarded
  by ``execute``. Use ``OpenAIHook`` directly to access it.
- ``metadata``: a mapping of key-value pairs attached to the response for your own bookkeeping.
- ``max_output_tokens``: an upper bound on the number of tokens the model can generate, including
  reasoning tokens as well as visible output tokens. Setting this key here (instead of the
  operator's own ``max_output_tokens`` parameter -- see above) applies the same validation and
  type-coercion rules; setting the same ceiling in both places raises when the operator is
  constructed. The one behavioral difference: a blank value here is popped from the payload sent
  to ``create_response`` (rather than never being added, as with the operator argument), and a
  present key whose value is a literal ``None`` always raises, since presence of the key -- not
  the value -- is what "supplied" means for this path.
- ``max_tool_calls``: an upper bound on the number of built-in tool calls the model can make. Same
  validation, coercion, and blank/``None`` handling as ``max_output_tokens`` above.

.. note::

    OpenAI does not expose a spend or cost ceiling parameter on the Responses API.
    ``max_output_tokens`` and ``max_tool_calls`` are token and call-count limits, not a way to cap
    the dollar cost of a run; controlling spend means bounding those counts yourself.

.. note::

    ``background=True`` starts the response running asynchronously on OpenAI's side and returns
    before the response finishes. ``OpenAIResponseOperator`` is synchronous: it makes one
    ``create_response`` call and returns ``response.output_text`` immediately, so a response
    started with ``background=True`` comes back incomplete, and the operator logs its own warning
    because ``response.status`` is not yet ``"completed"``. Do not set ``background=True`` on
    ``OpenAIResponseOperator``. If you need a background response, create it from a ``@task``
    using :class:`~airflow.providers.openai.hooks.openai.OpenAIHook`'s ``create_response`` directly.

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

.. _howto/operator:OpenAIAgentSessionOperator:

Managed Agents sessions
=======================

Use :class:`~airflow.providers.openai.operators.agent.OpenAIAgentSessionOperator`
to submit a message to OpenAI's Managed Agents service. The service runs the agent
loop. Airflow waits for the first turn to complete, optionally releasing the worker
with ``deferrable=True``. This requires OpenAI Python SDK 3.13.0 or newer and access
to the beta Agents API on your configured endpoint.

The provider's base dependency still permits older SDKs for other OpenAI APIs.
Install ``openai>=3.13.0`` on both workers and triggerers to use Managed Agents.
Libraries that require ``openai<3`` (including current LlamaIndex OpenAI LLM
integrations) cannot share that environment.

.. exampleinclude:: /../../openai/tests/system/openai/example_openai_agent.py
    :language: python
    :start-after: [START howto_operator_openai_agent]
    :end-before: [END howto_operator_openai_agent]

Parameters
^^^^^^^^^^

* ``input``: Initial user message.
* ``environment``: SDK environment configuration, such as ``{"type": "none"}``,
  or an environment template reference for a hosted sandbox.
* ``agent_id``: An existing saved agent. Alternatively, supply an inline agent
  with a model in ``session_kwargs["agent"]``.
* ``session_kwargs``: SDK session creation options, including agent overrides,
  ``vault_ids`` and ``metadata``. The keys ``input``, ``environment``, ``agent_id``
  and ``stream`` are reserved.
* ``conn_id``: OpenAI connection, defaulting to ``openai_default``.
* ``deferrable``: Whether to release the worker while waiting. Defaults to the
  Airflow ``operators.default_deferrable`` setting.
* ``poll_interval``: Seconds between checks, defaulting to 10.
* ``timeout``: Seconds to wait for completion, defaulting to 3600. A shorter
  ``execution_timeout`` still applies to a deferred task and preempts the
  cancel-on-timeout path below.

Transient polling failures are retried; three consecutive failures fail the task.

The operator returns the session ID. When XCom pushing is enabled, it also writes
``session_id``, ``turn_id`` and the turn's available token ``usage``. Usage includes
the Airflow ``try_number``; it represents the current attempt, not cumulative spend
across retries. Full message histories and artifacts are not stored in XCom.
Retrieve them with ``OpenAIHook().get_conn().beta.agents.sessions.items`` and
``.artifacts`` using the returned session ID.

Each attempt creates a fresh session. Do not submit additional turns to it while
this task is running. An idle session without a visible turn is not treated as
success. Failed or cancelled turns fail the task. Client-side function tools are
not executed by the operator and fail the task when requested; use service-side
tools instead. A self-hosted environment must have an independently managed worker.

On timeout or polling failure, the operator requests cancellation of its session's
active turn. It retains the session and artifacts for inspection. Cancellation does
not delete the environment or guarantee that its resources have been released.
Killing a synchronous task also requests cancellation. Cancellation of a killed
deferred task requires Airflow 3.3 or newer; on older versions, cancel it manually.
A hard worker termination or Airflow execution timeout can bypass cleanup. Retrying
the task creates another session and can repeat external side effects.

Hook methods
^^^^^^^^^^^^

:class:`~airflow.providers.openai.hooks.openai.OpenAIHook` provides
``create_agent``, ``create_agent_session``, ``get_agent_session`` and
``cancel_agent_session``. ``poll_agent_session`` checks the first turn of a fresh,
exclusively owned session; it is not a general waiter for reused sessions.
For other resources, use the SDK client returned by ``get_conn()``. See the
`OpenAI Agents API reference <https://developers.openai.com/api/reference/python/resources/beta/subresources/agents>`__.
