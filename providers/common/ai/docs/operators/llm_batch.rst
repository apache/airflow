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

.. _howto/operator:llm_batch:

``LLMBatchOperator``
=====================

Use :class:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator` to run many
prompts through a provider's **batch API** instead of one synchronous call per prompt:
roughly half the per-token cost of :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`,
in exchange for up to a 24-hour turnaround. The ``"<provider>:<model>"`` prefix of ``model_id``
selects the OpenAI or Anthropic adapter; ``llm_conn_id`` must be a ``pydanticai`` connection.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Results never go to XCom
-------------------------

This is the most important way ``LLMBatchOperator`` differs from ``LLMOperator``: results are
written as JSONL to ``result_path`` (an object storage directory), and the XCom value is a small
manifest describing where to find them and how many requests landed in each outcome bucket. A
batch can have up to 100,000 results, far too much to push through XCom.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_basic]
    :end-before: [END howto_operator_llm_batch_basic]

The manifest
^^^^^^^^^^^^

The XCom value is a JSON object with a stable shape (``schema_version: 1``):

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Key
     - Meaning
   * - ``result_uri``
     - The JSONL file, ``{result_path}/{custom_id_prefix}.jsonl``.
   * - ``request_count``
     - How many requests were submitted. Always equals the sum of ``counts``.
   * - ``counts``
     - ``succeeded``, ``errored`` (the provider rejected the request), ``invalid_output``
       (the model answered but not in the requested schema), ``expired``, ``cancelled`` and
       ``missing`` (the provider never reported an outcome for the request).
   * - ``terminal_reason``
     - ``succeeded`` when every request succeeded, ``expired`` if any request expired,
       ``cancelled`` if any was cancelled, otherwise ``partial``.
   * - ``rejoin_key`` / ``ordered``
     - Always ``"index"`` and ``false``: rows arrive in provider order, and each row's
       ``index`` is the position of its input in ``requests``.
   * - ``batch_id``, ``adapter``, ``llm_conn_id``, ``model_id``, ``output_type_ref``,
       ``structured``, ``submitted_at``, ``completed_at``
     - Provenance for the run.
   * - ``duplicate_result_count``, ``out_of_range_result_count``
     - Anomalies the provider stream contained and the merge dropped. Normally zero.

The result rows
^^^^^^^^^^^^^^^

Every request gets exactly one JSONL row with the same keys:

.. code-block:: json

    {"custom_id": "3f9c...-0", "index": 0, "status": "success",
     "output": {"label": "positive", "confidence": 0.93}, "raw_output": null, "error": null,
     "model": "gpt-5", "usage": {"input_tokens": 41, "output_tokens": 12}, "finish_reason": "stop"}

``status`` is one of ``success``, ``error``, ``invalid_output``, ``expired``, ``cancelled`` or
``missing``. ``output`` is set only for ``success``; ``raw_output`` preserves what the model
returned when validation failed; ``error`` carries the provider's type, message and code for
provider-side failures. The manifest's ``counts`` keys use the past tense (``succeeded``,
``errored``) while row statuses use the bare word; the other buckets spell the same.

A downstream task reads the rows back from ``result_uri`` with
:class:`~airflow.sdk.ObjectStoragePath` and rejoins them to its inputs on ``index``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_read_results]
    :end-before: [END howto_operator_llm_batch_read_results]

Structured output
------------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass (or another type ``TypeAdapter``
supports, such as ``int`` or ``list[str]``). OpenAI requests use ``response_format``; Anthropic
requests use a single forced tool call. Both providers require the schema root to be an object,
so a non-object ``output_type`` is wrapped as ``{"response": ...}`` on the way out and unwrapped
again before validation; the JSONL ``output`` holds the plain value.

A model occasionally returns something that does not match the schema. That is not a task
failure: the row gets ``status: "invalid_output"`` with the original text preserved in
``raw_output``, and ``counts.invalid_output`` is separate from ``counts.errored`` so you can tell
"the model produced output that doesn't match the schema" apart from "the request never even
succeeded". A response with no content at all (a refusal, or a reasoning model that spent its
whole ``max_tokens`` budget before producing visible output) is also ``invalid_output``, for
``output_type=str`` too.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_structured_output_class]
    :end-before: [END howto_operator_llm_batch_structured_output_class]

``@task.llm_batch``
--------------------

The TaskFlow decorator wraps a callable that returns the batch's inputs (a ``list[str]`` or a
list of ``{"prompt": ..., "model": ..., ...}`` dicts) instead of a single prompt string:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_decorator_llm_batch]
    :end-before: [END howto_decorator_llm_batch]

The decorated callable **must be deterministic across attempts**: its return value feeds the
fingerprint that decides whether a retry re-attaches to the batch already submitted. A callable
that embeds something that changes between attempts (a wall-clock timestamp, say) looks like
"the input changed" on every retry and pays for a new batch each time.

Unlike ``@task.llm``, the returned prompts are **not** rendered as Jinja templates. Batch inputs
are usually bulk text the Dag author did not write, where a stray ``{{`` or ``{%`` would either
fail the whole batch or resolve ``var``/``conn`` accessors against Airflow secrets. Put anything
dynamic in the callable, which receives the task context. ``result_path`` and the other operator
parameters are templated as usual; keep ``result_path`` stable across attempts of the same task
instance (``{{ run_id }}`` is fine, ``{{ ts }}`` is not), or a retry looks for its recorded state
at a location the previous attempt never wrote to.

Provider-specific parameters
------------------------------

The operator builds each request body from the prompt, the system prompt, the model, the token
cap and (for structured output) the schema directive, and translates those per provider:
``max_tokens`` is sent as ``max_completion_tokens`` to OpenAI and as ``max_tokens`` to Anthropic,
and ``output_type`` becomes ``response_format`` on OpenAI and a forced tool on Anthropic.

Everything else the provider's chat endpoint accepts goes through ``request_params``, which is
merged into every request body as-is. For an ``openai:`` model that means OpenAI chat-completions
keys (``temperature``, ``reasoning_effort``, ``user``, ``seed``, ...); for an ``anthropic:`` model
it means Anthropic Messages keys (``temperature``, ``top_k``, ``metadata``, ``thinking``, ...).
The operator does not validate these; an unknown key surfaces as a per-request ``error`` row
from the provider, or as a failed batch when the provider rejects the whole input file.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_provider_params]
    :end-before: [END howto_operator_llm_batch_provider_params]

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Operator parameter
     - OpenAI request body
     - Anthropic request body
   * - ``system_prompt``
     - First ``messages`` entry with ``role: system``
     - ``system``
   * - ``max_tokens``
     - ``max_completion_tokens`` (``max_tokens`` is dropped)
     - ``max_tokens``
   * - ``output_type``
     - ``response_format`` of type ``json_schema`` (``strict: false``)
     - ``tools`` + ``tool_choice`` forcing one tool whose input schema is the type
   * - ``request_params`` / ``params``
     - Merged as-is, managed keys win
     - Merged as-is, managed keys win
   * - ``completion_window``
     - Batch-level ``completion_window`` (``"24h"``)
     - Ignored

Per-request overrides
-----------------------

A request dict may carry ``system_prompt``, ``max_tokens``, ``params`` (extra body parameters
for that request only, layered over ``request_params``) and ``model``. ``model`` is written the
same way ``model_id`` is, ``"<provider>:<model>"``, and must name the same provider as the batch.
Anthropic allows a different model per request; OpenAI requires every request in a batch to
resolve to the same model and the operator rejects a mixed batch before submitting it.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_per_request]
    :end-before: [END howto_operator_llm_batch_per_request]

``request_params`` and a request's ``params`` cannot override the keys the operator manages
(the model, the messages, the token cap and the structured-output directive).

.. _llm-batch-reattach:

Retries re-attach instead of re-submitting
--------------------------------------------

A retry (or a manual **clear**) of this task computes the same identity key as the attempt
before it, based on the Dag, task, run and map index and never the try number, and looks for a
recorded batch under ``{result_path}/_airflow_batch_state/``. If the input (prompts, model,
``output_type`` schema, connection) still matches, the task re-attaches to the existing batch
instead of submitting a new one. This is what makes ``retries`` safe to use here, whereas the
Anthropic provider's ``AnthropicBatchOperator`` recommends ``retries=0``.

The recorded state is kept after a successful landing, so clearing a finished task re-lands the
same results at no cost. To force a fresh submission, delete the state file for that task
instance.

**Changing the input is a new batch.** Editing the prompts, the model, or the ``output_type``
schema (even just a field description) and clearing the task is treated as a different batch,
never silently re-attached to results produced under the old input. ``on_stale_state`` controls
what happens: ``"cancel_and_resubmit"`` (default) cancels the stale batch through the connection
that submitted it and submits a fresh one; ``"fail"`` raises instead.

What each outcome does to the recorded state
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 22 48 30

   * - Outcome
     - Task result
     - Next attempt
   * - ``completed`` / ``expired``
     - Results landed; succeeds unless ``fail_on_partial_error`` says otherwise.
     - Re-lands the same results, no new submission.
   * - ``cancelled`` with lost requests
     - Whatever finished is landed, then ``LLMBatchCancelledError``. State is cleared.
     - Submits a fresh batch.
   * - ``failed`` (OpenAI rejected the input before running it)
     - ``LLMBatchJobError``. State is cleared; nothing was billed.
     - Submits a fresh batch.
   * - ``timeout`` (our own budget ran out)
     - ``LLMBatchTimeoutError`` naming the budget, the deadline and whether the batch was
       cancelled. State is kept.
     - Re-attaches: finds the cancelled batch (then follows the ``cancelled`` row) or, with
       ``cancel_on_timeout=False``, keeps waiting with a fresh ``timeout``.
   * - polling gave up (five consecutive status-check failures)
     - ``LLMBatchJobError``. State is kept; the batch's fate is unknown.
     - Re-attaches and polls again.

Recovering from a crash between submit and recording it
---------------------------------------------------------

The operator records its intent to submit before the paid call, so a crash after the provider
accepted the batch but before the response was recorded leaves a trace. The next attempt asks
the provider for a matching batch, by the recorded identity key **and** a fingerprint of the
exact input, so it can never recover an unrelated submission. For OpenAI this works: the adapter
records both values in the batch's own ``metadata`` at submit time. If that lookup itself fails
(the provider is unreachable), the attempt raises ``LLMBatchOrphanLookupError`` rather than
guess; the retry checks again.

**Anthropic offers no such lookup.** The SDK's batch-create call has no ``metadata`` parameter
and batches cannot be listed by ``custom_id``, so an Anthropic orphan falls through to
``on_orphaned_intent``: ``"resubmit"`` (default) submits a new batch, which pays for both if the
original request did reach the provider; ``"fail"`` raises so you can reconcile against the
provider's own batch listing first.

``cancel_on_kill`` and ``cancel_on_timeout``
----------------------------------------------

``cancel_on_kill`` cancels the batch if the task is killed. In deferrable mode this runs from the
trigger's ``on_kill``, which only **Airflow 3.3+** calls; on those versions clearing, marking
success or marking failed on a deferred task from the UI counts as a kill, so the batch is
cancelled and the next attempt submits a fresh one rather than re-attaching. On Airflow 3.0 to
3.2 a killed deferred task's batch keeps running and a clear re-attaches to it. Set
``cancel_on_kill=False`` if you want clear-to-re-attach on 3.3+ as well.

``cancel_on_timeout=False`` lets a batch keep running (and billing) past this task's own
``timeout``: the task fails, and a later retry re-attaches to it and waits again. A retry that
runs after the original budget has elapsed gets a fresh ``timeout`` measured from the retry, so
``retries`` and ``retry_delay`` bound the total wait.

Reaching the batch API through a gateway
------------------------------------------

The OpenAI adapter reads ``base_url`` from the connection's host, so a ``pydanticai`` connection
pointed at an OpenAI-compatible gateway that exposes ``/v1/files`` and ``/v1/batches`` (LiteLLM
does, and routes to Azure OpenAI, Vertex and Bedrock batch behind it) runs the whole batch
through that gateway with ``model_id="openai:<gateway-model-name>"``. That is the path to take
today for a provider ``@task.llm_batch`` has no adapter for yet; the error message for a
``pydanticai_azure``, ``pydanticai_bedrock`` or ``pydanticai_vertex`` connection says so.

``LLMBatchOperator`` or the vendor batch operators?
-----------------------------------------------------

``OpenAITriggerBatchOperator`` and ``AnthropicBatchOperator`` submit and poll the same provider
batch APIs. Reach for them when you need the vendor's native request bodies (multi-turn
conversations, images, tools, non-chat endpoints such as embeddings) or a fire-and-forget submit
with a separate sensor. Reach for ``LLMBatchOperator`` when the job is "many prompts, one
optional output schema": it owns JSONL construction and upload, chunk-size validation,
retry-safe re-attachment, provider-neutral structured output, and results landed on object
storage with a reconciled manifest.

Adding an adapter
------------------

Another provider package can add a batch engine without changes here: subclass
:class:`~airflow.providers.common.ai.batch.base.BatchAdapter`, set its ``name`` to the model
prefix it serves and ``conn_types`` to the connection types it can authenticate from, and either
call :func:`~airflow.providers.common.ai.batch.dispatch.register_adapter` at import time or
declare an entry point in the ``airflow.providers.common.ai.batch_adapters`` group whose name
is the prefix and whose value is ``module:Class``.
