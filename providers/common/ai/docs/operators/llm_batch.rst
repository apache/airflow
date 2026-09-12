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
prompts through a provider's **batch API** instead of one synchronous call per prompt --
roughly half the per-token cost of :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`,
in exchange for up to a 24-hour turnaround. Routes to OpenAI or Anthropic based on
``llm_conn_id``'s connection type and ``model_id``'s ``"<provider>:<model>"`` prefix.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Results never go to XCom
-------------------------

This is the most important way ``LLMBatchOperator`` differs from ``LLMOperator``: results are
written as JSONL to ``result_path`` (an object storage directory), and the XCom value is a small
manifest describing where to find them and how many requests landed in each outcome bucket
(``succeeded``, ``errored``, ``invalid_output``, ``expired``, ``cancelled``, ``missing``). A batch
can have up to 100,000 results -- far too much to push through XCom.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_batch.py
    :language: python
    :start-after: [START howto_operator_llm_batch_basic]
    :end-before: [END howto_operator_llm_batch_basic]

Structured output
------------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass (or another type ``TypeAdapter``
supports). OpenAI requests use ``response_format``; Anthropic requests use a single forced
tool call. Both translations happen per-adapter; the batch surface itself only deals in
``output_type`` and JSON Schema.

A model occasionally returns something that does not match the schema even when asked nicely.
That is not treated as a task failure -- the corresponding JSONL row gets
``status: "invalid_output"`` with the original text preserved in ``raw_output``, and the
manifest's ``counts.invalid_output`` is a separate number from ``counts.errored`` (a
provider-side failure, e.g. rate limiting) precisely so you can tell "the model produced
output that doesn't match the schema" apart from "the request never even succeeded", and fix
the right thing (the schema/prompt vs. the request rate).

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
that embeds something that changes between attempts (e.g. a wall-clock timestamp) looks like
"the input changed" on every retry, paying for a brand new batch each time instead of
re-attaching. ``result_path`` is templated too, but is not part of that fingerprint -- keep it
stable across attempts of the same task instance (run-level template values like ``{{ run_id }}``
are fine; attempt-level ones are not), or a retry will look for its recorded state at a location
the previous attempt never wrote to.

Per-request model override
----------------------------

A request may override the batch-level ``model_id`` with its own ``"model"`` key, written the
same way ``model_id`` itself is: ``"<provider>:<model>"`` (e.g. ``"anthropic:claude-3-opus"``), not
a bare model name -- a bare name is rejected before any network call. Anthropic allows a
different model per request within the same batch; OpenAI requires every request in a batch to
resolve to the same model and rejects a batch that mixes more than one::

    {"prompt": "...", "model": "anthropic:claude-3-opus"}

Retries re-attach instead of re-submitting
--------------------------------------------

A retry (or a manual **clear**) of this task computes the same identity key as the attempt
before it -- based on the Dag, task, run, and map index, never the try number -- and looks for
a recorded batch under ``{result_path}/_airflow_batch_state/``. If the input (prompts, model,
``output_type`` schema, ...) still matches, the task re-attaches to the existing batch instead
of submitting (and paying for) a new one. This is what makes ``retries`` safe to use here, unlike
the vendor ``AnthropicBatchOperator`` / ``OpenAITriggerBatchOperator``, whose docs recommend
``retries=0``.

Clearing a task from the UI keeps the same run and map index, so it re-attaches too -- to force
a fresh submission, delete the state file for that task instance.

**Changing the input is a new batch.** Editing the prompts, the model, or the ``output_type``
schema (even just a field description) and clearing the task is treated as a different batch,
never silently re-attached to results produced under the old input -- ``on_stale_state``
controls what happens: ``"cancel_and_resubmit"`` (default) cancels the stale batch and submits a
fresh one; ``"fail"`` raises instead.

A **cancelled** batch (whether cancelled by ``cancel_on_kill``/``cancel_on_timeout`` or out of
band) is not treated as a dead end: it may already have partial, billed results, so it is
fetched, validated, and landed exactly like a **completed** or **expired** one.
``fail_on_partial_error`` decides whether that outcome fails the task.

.. _howto/operator:llm_batch:orphan_recovery_and_duplicate_billing:

Recovering from a crash between submit and recording it (and the Anthropic gap)
-----------------------------------------------------------------------------------

If Airflow crashes after a batch has been submitted to the provider but before the response
confirming it was recorded, the next attempt tries to recover that orphaned batch from the
provider itself (by the recorded idempotency key **and** a fingerprint of the exact input, so it
can never recover a different, unrelated submission) instead of blindly submitting again. For
OpenAI, this works: the adapter records both values in the batch's own ``metadata`` at submit
time, and can look them back up.

**For Anthropic, this recovery is not possible** -- the SDK's batch-create call has no
``metadata`` parameter, and there is no way to list or search batches by the identifiers this
operator writes into ``custom_id``. A crash in that narrow window, for an Anthropic batch,
means the next attempt cannot tell "the original submit never reached the provider" apart from
"it succeeded, but recovery is impossible" -- ``on_orphaned_intent`` controls what happens next:

- ``"resubmit"`` (default) submits a new batch, same as if no crash had happened. If the
  original request actually reached the provider, this pays for both batches.
- ``"fail"`` raises instead of resubmitting, for callers who would rather stop and investigate
  (e.g. check the provider's own batch listing out of band) than risk a duplicate, billable
  submission.

Unlike ``on_stale_state``, there is no old batch id available to cancel as a loss-limiting step
here -- the crash happened before that id was ever recorded. The crash window itself (between the
provider accepting the request and this operator's own process durably recording that fact) is
inherently narrow and infrequent, but if you run Anthropic batches at a volume where even a rare
duplicate is unacceptable, ``on_orphaned_intent="fail"`` combined with out-of-band reconciliation
is the safer choice.

``cancel_on_kill`` and ``cancel_on_timeout``
----------------------------------------------

``cancel_on_kill`` cancels the batch if the task is killed. In deferrable mode this only takes
effect on **Airflow 3.3+** -- older triggerers have no way to call a trigger's ``on_kill``, so a
killed deferred task's batch is not cancelled automatically on those versions.

``cancel_on_timeout=False`` lets a batch keep running (and billing) past this task's own
``timeout`` -- the task still fails, but a later retry re-attaches to the batch and its eventual
results instead of paying for a second submission.

LiteLLM gateway passthrough
-----------------------------

If your organization already routes LLM traffic through a `LiteLLM
<https://docs.litellm.ai/>`__ gateway, pointing a ``pydanticai`` connection at it and calling
``LLMOperator`` today is a working alternative to waiting for a specific provider's batch
adapter here (e.g. Azure OpenAI, not yet supported -- see the connection type's error message
for the current status).
