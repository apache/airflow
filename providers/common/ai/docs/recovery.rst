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

.. _howto/recovery:

Recovering from failures
========================

An agent task can fail partway through a run: the model provider goes down or rate
limits the task, or the worker running it dies. Two settings keep the work the agent
already did: ``fallback_conn_ids`` moves a failed model request to another connection,
and ``durable=True`` lets an Airflow retry replay the steps an earlier attempt
completed. To decide whether a failure is worth retrying at all, see
:doc:`retry_policies`.

What each failure costs
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Failure
     - Handled by
     - What happens to the completed steps
   * - The provider returns any HTTP 4xx or 5xx error, times out, or cannot be reached
     - ``fallback_conn_ids``, inside the attempt
     - Kept. The next connection receives the run's message history, including every
       tool result, and continues the run.
   * - Every connection in the chain fails, and the task raises
       ``pydantic_ai.exceptions.FallbackExceptionGroup``
     - Airflow ``retries`` with ``durable=True``
     - Replayed. The retry replays the steps an earlier attempt recorded, whichever
       connection answered them, and continues on the first connection that answers now.
   * - The worker dies, or the task hits its ``execution_timeout``
     - Airflow ``retries`` with ``durable=True``
     - Replayed, as above. A step that was still running runs again.
   * - Either of the last two, without ``durable=True``
     - Airflow ``retries``
     - Run again from the start: every model call is paid for again and every tool runs
       again.

A response the provider does return but pydantic-ai rejects, such as a content-filter
refusal (``ContentFilterError``), does not fail over: the chain moves on only for
``ModelAPIError``.

The OpenAI and Anthropic SDKs retry a connection error, a timeout, and a 408, 409, 429 or
5xx response twice by default before the chain sees the failure. A refused connection
therefore costs a few seconds before failover, and a request that hangs costs up to three
times the ``timeout`` in ``model_settings``, plus backoff. Without a ``timeout``, each
attempt waits for the SDK's default of 600 seconds. Both SDKs also wait out a 429's
``Retry-After`` header before retrying: the OpenAI SDK for up to two minutes, the
Anthropic SDK for as long as the header asks.

On Airflow before 3.3 the journal is a file under ``[common.ai] durable_cache_path``; a
retry that runs on another worker replays only if that path is shared storage. See
:doc:`durable_execution`.

Keep a run going through an outage
----------------------------------

This task declares both settings: a standby connection for a failed request, and a
journal for a failed attempt:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_recovery.py
    :language: python
    :start-after: [START howto_agent_recovery]
    :end-before: [END howto_agent_recovery]

See :doc:`provider_fallback` for the chain itself, including how a bare model name is
forwarded to connections that set no ``model`` of their own. See
:doc:`durable_execution` for the journal, including the ``id`` it needs on every
``FunctionToolset``, ``MCPToolset`` and ``DynamicToolset``.

What the next connection receives
---------------------------------

Failover happens per model request. Each request carries the run's whole message
history: the prompt, every model response so far and every tool result. The next
connection in the chain gets that same history and continues the run; a tool that
already ran is not called again. The next connection can be another vendor's model,
for example an Anthropic model taking over a run an OpenAI model started.

A streamed request is the exception. When the run streams, for example with an
``event_stream_handler`` in ``agent_params``, the chain moves on only if the stream fails
to open; an error after the stream started propagates and fails the attempt.

To drill the example, point ``llm_primary`` at a port nothing listens on. The task
succeeds, and its log shows the chain, the OpenAI SDK's two retries against the primary,
and the model that finished the run:

.. code-block:: text

    Resolving LLM fallback chain: llm_primary -> llm_standby
    Retrying request to /responses in 0.459459 seconds
    Retrying request to /responses in 0.913650 seconds
    ::group::Tool call: list_tables
    Retrying request to /responses in 0.485229 seconds
    Retrying request to /responses in 0.849597 seconds
    ::group::Tool call: get_schema
    ...
    Durable: replayed 0 steps (0 model, 0 tool, 0 other), recorded 7 new steps (4 model, 3 tool, 0 other)
    ::group::LLM run complete: model=claude-sonnet-5, requests=4, tool_calls=3, input_tokens=5135, output_tokens=334, total_tokens=5469
    Tool call sequence: list_tables -> get_schema -> query

Each of the run's four model requests tried the primary first and paid its retries
before moving on. The chain keeps no memory of an outage between requests, so keep the
primary's ``timeout`` short.

Retries after the whole chain fails
-----------------------------------

With ``durable=True``, each step is recorded in the task instance's journal whichever
connection answered it. When every connection fails, the task raises
``FallbackExceptionGroup`` and Airflow retries it. The retry replays the recorded steps,
then sends its first live request through the chain from the top. The run continues on
the first connection that answers, whether or not that is the one that answered before.

A recorded model step replays only if the retry's request has the same fingerprint, and
for a chain the model in the fingerprint is the whole chain, whichever connection
answered. Adding, removing or reordering a connection in ``fallback_conn_ids``, or
changing the ``model`` of a connection in it, between attempts therefore makes the retry
run every step live, tools included. Changing a connection's credentials or host does
not. "How a retry replays" in :doc:`durable_execution` gives the full matching rules.

What is not covered
-------------------

- A tool that sent an email and then raised sends it again on retry, and so does one
  whose worker died before its result was recorded. Make such tools idempotent; see
  "Side effects and idempotency" in :doc:`durable_execution`.
- A request every provider rejects, such as a malformed one that each returns a 400
  for, walks the whole chain on every attempt. A ``RetryRule`` cannot stop those retries
  without stopping outage retries too: once a chain is configured, both raise
  ``FallbackExceptionGroup``, and a rule compares only that top-level type. Retry
  policies need Airflow 3.3 or later; see "When the connection also carries a fallback
  chain" in :doc:`retry_policies`.
- No connection type builds the ``AnthropicVertex`` client that Claude on Vertex AI
  needs, so a chain cannot include Claude on Vertex AI. A Vertex AI connection reaches
  Gemini.
- Failover and replay cover pydantic-ai agents only: ``fallback_conn_ids`` on the
  pydantic-ai hooks and the operators built on them, and replay through ``durable=True``
  or the ``AirflowDurability`` capability in a plain ``@task``. A Strands
  Agents agent run through ``invoke_durably`` resumes from its own checkpoint on
  Airflow 3.3 and later (:ref:`howto/frameworks:strands-durable`), and a retried
  ``LLMBatchOperator`` re-attaches to its running batch (:ref:`llm-batch-reattach`).
- A ``ManagedAgentToolset`` runs again on every attempt unless it is built with
  ``replayable=True``. Durable execution cannot be combined with ``enable_hitl_review``,
  a ``SandboxToolset`` or tool approval; see the limitations in :doc:`durable_execution`.
