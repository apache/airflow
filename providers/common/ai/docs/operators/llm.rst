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

.. _howto/operator:llm:

``LLMOperator``
===============

Use :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` for
general-purpose LLM calls — summarization, extraction, classification,
structured output, or any prompt-based task.

The operator sends a prompt to an LLM via
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` and
returns the output as XCom.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Provide a ``prompt`` and the operator returns the LLM's response as a string:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_basic]
    :end-before: [END howto_operator_llm_basic]

Structured Output
-----------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass. The LLM is instructed
to return structured data, and the model instance is pushed to XCom unchanged
so downstream tasks can type-hint the class directly
(``def downstream(result: MyModel)``) and use attribute access (``result.field``).

The declared ``output_type`` (and any ``BaseModel`` reachable from
``Union``/``Optional``/``list`` shapes) is registered for XCom deserialization by
the worker when it loads the Dag, before any task runs -- so no edit to
``[core] allowed_deserialization_classes`` is needed. The Pydantic class must be
defined at **module scope** and bound to an attribute matching its ``__name__``;
classes nested inside a function or ``@dag``-decorated body, parameterized
generics, and dynamically-built classes whose ``__name__`` does not match the
attribute they are bound to cannot be re-imported, so they are skipped with a
warning at worker startup and the value fails to deserialize at the consumer.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_structured_output_class]
    :end-before: [END howto_operator_llm_structured_output_class]

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_structured]
    :end-before: [END howto_operator_llm_structured]

Registration covers downstream tasks in the **same Dag**: every worker walks the
loaded Dag's tasks at startup and registers each declared class, so it also works
for mapped producers (``.expand(...)``) and for workers that load Dags from a
cache that bypasses operator construction.

The Airflow UI's XCom viewer renders Pydantic instances via the
``stringify`` path, which produces a representation like
``my_module.MyModel@version=1(field=value,...)`` without consulting the
allow-list. It is not pretty (no field-by-field rendering today), but the value
shows up; no configuration is required.

The remaining gap is **cross-Dag** ``xcom_pull`` -- a task in a different Dag
that pulls this XCom only parses its own Dag file, not the producer's, so the
class is not auto-registered. Add the class qualified name to
``[core] allowed_deserialization_classes`` (or a glob that matches it) to make
that pattern work.

If a downstream consumer needs the dict shape (e.g. forwarding to an external
system that expects JSON-style payloads), pass ``serialize_output=True`` and the
operator calls ``model_dump()`` before pushing to XCom. The pre-PR behavior is
available on demand without giving up the typed default.

Agent Parameters
----------------

Pass additional keyword arguments to the pydantic-ai ``Agent`` constructor
via ``agent_params`` — for example, ``retries``, ``model_settings``, or ``tools``.
See the `pydantic-ai Agent docs <https://ai.pydantic.dev/api/agent/>`__ for
the full list of supported parameters.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_agent_params]
    :end-before: [END howto_operator_llm_agent_params]

.. _howto/operator:llm_usage_limits:

Usage Limits
------------

Set ``usage_limits`` to a
`pydantic-ai UsageLimits <https://ai.pydantic.dev/api/usage/#pydantic_ai.usage.UsageLimits>`__
to fail the task when the run exceeds a configured budget — request count,
input/output tokens, or tool calls. The check happens inside pydantic-ai's
run loop, so the limit applies even when ``retries`` triggers multiple model
calls within a single task.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_usage_limits]
    :end-before: [END howto_operator_llm_usage_limits]

A plain ``dict`` can be passed instead of a ``UsageLimits`` instance, which lets
Jinja template individual fields -- e.g. a per-run cost cap driven by an Airflow
Variable so the budget can change per environment without editing the Dag:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_templated_usage_limits]
    :end-before: [END howto_operator_llm_templated_usage_limits]

Each dict value is rendered by Jinja like any other ``template_fields`` entry,
then coerced to that field's type (``Decimal``, ``int``, or ``bool``). A value
that doesn't parse -- a Variable that exists but is empty renders to ``""``, a
typo renders to a non-numeric string -- fails the task with a ``ValueError``
naming the field and the rendered value, instead of silently disabling the
limit. A ``UsageLimits`` instance passed directly is used as-is and is not
templated or validated.

Common knobs on ``UsageLimits``:

- ``request_limit`` — max model requests per run (caps retry/tool-loop blow-ups).
  pydantic-ai applies a default of ``50`` when ``UsageLimits()`` is constructed
  without an explicit value, so passing ``UsageLimits(input_tokens_limit=4_000)``
  (or the dict form ``{"input_tokens_limit": 4_000}``) silently inherits that
  50-request cap. Set ``request_limit=None`` explicitly when you only want a
  token cap.
- ``input_tokens_limit`` / ``output_tokens_limit`` — per-run token caps.
- ``total_tokens_limit`` — combined input + output cap.
- ``tool_calls_limit`` — max tool invocations (``AgentOperator`` only).
- ``cost_limit`` — a ``Decimal`` cap on the run's estimated USD cost. This is **not** a
  hard guarantee against overspend: the response that crosses the limit has already been
  produced and billed — pydantic-ai checks the accumulated cost *after* each response and
  then fails the run with ``UsageLimitExceeded``. It protects you from further spend, not
  from the request that broke the budget; even a single-request run fails as soon as that
  request's cost pushes the total over the limit. Pricing is looked up by model
  name, not by endpoint: a self-hosted deployment serving a model pydantic-ai
  recognizes is still priced, at that model's public list rates rather than at what
  the deployment actually costs you. That covers vLLM, whose only working prefix is
  ``openai:<model>`` (see :doc:`../self_hosted_models`).
  A model pydantic-ai cannot price (``ollama:llama3.2``, a private fine-tune)
  reports no cost at all, so ``cost_limit`` is not enforced there -- a
  ``CostNotFoundWarning`` is emitted instead of failing the run. And like the other
  knobs above, setting ``cost_limit``
  alone still inherits the ``request_limit=50`` default — see the ``request_limit`` note
  above. Note that ``cost_limit`` only caps the operator's own LLM calls --
  the meta-agent that ``LLMRetryPolicy`` runs to classify a failed task is a separate,
  uncapped LLM call; see :doc:`../retry_policies`.

When the limit is hit pydantic-ai raises ``UsageLimitExceeded``, which
propagates to Airflow as a task failure — Airflow's standard retry policy
applies on top. Every limit here bounds a single agent *run*, not a task: each
Airflow task retry re-renders ``usage_limits`` and starts a fresh count, and for
``AgentOperator`` so does each HITL regeneration. A ``cost_limit`` of
``Decimal("0.50")`` caps one run, so it is not a bound on what the task spends in
total.

TaskFlow Decorator
------------------

The ``@task.llm`` decorator wraps ``LLMOperator``. The function returns the
prompt string; all other parameters are passed to the operator:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_decorator_llm]
    :end-before: [END howto_decorator_llm]

With structured output:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_decorator_llm_structured]
    :end-before: [END howto_decorator_llm_structured]

Multimodal prompts
^^^^^^^^^^^^^^^^^^

``@task.llm`` accepts the same prompt shape as ``@task.agent`` -- the callable
may return either a ``str`` or a non-empty ``Sequence[UserContent]`` (e.g.,
``["Describe this:", ImageUrl(url="...")]``) for vision, audio, or document
inputs. See :ref:`@task.agent multimodal prompts <howto/operator:agent-multimodal>` for
the full example. ``require_approval=True`` is not currently supported with a
``Sequence`` prompt -- the approval session model expects a string -- and will
raise at the approval boundary; widening that path is tracked as a follow-up.


Classification with ``Literal``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set ``output_type`` to a ``Literal`` to constrain the LLM to a fixed set of
labels — useful for classification tasks:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_classification.py
    :language: python
    :start-after: [START howto_decorator_llm_classification]
    :end-before: [END howto_decorator_llm_classification]

Multi-task pipeline with dynamic mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Combine ``@task.llm`` with upstream and downstream tasks. Use ``.expand()``
to process a list of items in parallel:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_analysis_pipeline.py
    :language: python
    :start-after: [START howto_decorator_llm_pipeline]
    :end-before: [END howto_decorator_llm_pipeline]

.. seealso::
    :ref:`Dynamic System Prompt <howto/operator:agent-dynamic-system-prompt>` --
    ``system_prompt`` is templated identically on ``@task.llm``, so the same
    upstream-XCom pattern applies here.

Human-in-the-Loop Approval
--------------------------

Set ``require_approval=True`` to pause the task after the LLM generates its
output and wait for a human reviewer to approve or reject it via the Airflow
HITL interface.  Optionally allow the reviewer to edit the output before
approving with ``allow_modifications=True``, and set a deadline with
``approval_timeout``.

Human-in-the-loop review needs Airflow 3.1+, whether ``require_approval`` or a
``decision_policy`` with ``on_uncertain="review"`` opens it. On an older core the
operator raises ``AirflowOptionalProviderFeatureException`` when it is constructed, so the Dag file
fails to import, and with it every Dag defined in that file. A dynamically mapped
task (``.expand()``) is only constructed when it runs, so there the same error
surfaces as a task failure -- still before the model is called.

When ``approval_timeout`` expires without a review, the task fails by default.
Set ``on_approval_timeout="approve"`` to return the generated output instead, so
an unattended pipeline keeps moving.  ``"reject"`` answers the review with a
rejection, which still fails this operator; only
:class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
turns a rejection into a downstream skip.  The chosen option is also
pre-highlighted as the default in the review form, so ``"reject"`` makes
Reject the primary button:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_approval]
    :end-before: [END howto_operator_llm_approval]

A pending review is not surfaced as a notification.  Pass
``approval_notifiers`` to tell the reviewers about it through any Airflow
notifier (Slack, email, ...), the way
:class:`~airflow.providers.standard.operators.hitl.HITLOperator` does with
``notifiers``.  The notifiers run once the review is open and can reference
the review ``{{ task.subject }}`` and ``{{ task.body }}`` in their templates,
as the example above does.  The ``@task.llm`` decorator and the operator
subclasses accept the same parameter.  A notifier whose delivery fails is
logged and the task still waits for the review; a template error fails the
task.  A retry re-runs the LLM and re-notifies with the regenerated output,
while the open review keeps the original subject and body.

The default ``body`` contains the rendered prompt and the output.  Where either
is sensitive, template only ``{{ task.subject }}`` and a link to the review
into channels outside Airflow's auth boundary.

By default any user with the permission can answer the review.  Pass
``approval_assigned_users=[{"id": "<auth-manager-user-id>", "name": "<user-name>"}]``
to restrict it to named reviewers, the way
:class:`~airflow.providers.standard.operators.hitl.HITLOperator` does with
``assigned_users``.  ``id`` is the user id reported by the auth manager: with
the default ``SimpleAuthManager`` it is the username from
``simple_auth_manager_users``; under the FAB auth manager it is the numeric
user row id as a string, not the username.  This needs Airflow 3.1+.  On Airflow 3.1.0 through 3.1.5 both
``id`` and ``name`` must match what the auth manager reports, so a wrong
``name`` blocks the assigned reviewer as well as everyone else; from 3.1.6 only
``id`` is compared.  The list is stored when the review is first created:
clearing the task re-runs it against the existing review row, so a changed
list does not take effect.

Reviewing Uncertain Output
--------------------------

A classifier model such as TypeSafe's reports a confidence for every field
of a structured output, in ``provider_details`` on the model response. It is a
summary of how concentrated the model's probability distribution was, not the
probability that the field is right. ``decision_policy=DecisionPolicy(min_confidence=0.7)``
(import ``DecisionPolicy`` from ``airflow.providers.common.ai.operators.llm``)
is the bar the least confident field has to clear for the operator to return
the output by itself. Only fields that reported a confidence are compared: a
field whose type reports none (a bounded float, where the probability is the
answer) is not gated, and the record's ``confidence`` map shows which fields
were. Under the bar, the policy's ``on_uncertain`` applies: ``"review"``
(default) sends the output to human review through the same approval flow as
``require_approval``, with the same ``approval_timeout``,
``on_approval_timeout`` and notifier settings; ``"fail"`` fails the task with
``LowConfidenceError`` (from ``airflow.providers.common.ai.exceptions``), which
Airflow retries like any other failure unless a retry rule says otherwise. A
text model reports no confidence for any field, which counts as uncertain, so
switching the connection does not silently switch off a control you set.
``require_approval=True`` keeps its meaning and always asks, and
``on_uncertain="review"`` needs Airflow 3.1+ like it does. The policy is
honoured by ``LLMOperator`` and ``LLMBranchOperator``; the SQL, schema-compare
and file-analysis operators run their own ``execute`` and reject a policy with a
bar at construction.

With or without a bar, the operator pushes a ``decision`` XCom carrying the
model name, the per-field ``confidence`` and ``probabilities`` (empty for a text
model), the bar that applied, why the output went to review if it did, who
decided, and the gate configuration in force.
A pending record is checkpointed with the paused task and finalized from that
copy on resume, not from the XCom. See :ref:`LLMBranchOperator <howto/operator:llm_branch>` for the
record's fields; there ``proposed`` and ``action`` name the branches, while
here they are ``null`` and the output itself is the return value.

Parameters
----------

- ``prompt``: The prompt to send to the LLM (operator) or the return value of the
  decorated function (decorator).
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the connection's extra field.
- ``system_prompt``: System-level instructions for the agent. Supports Jinja templating.
- ``output_type``: Expected output type (default: ``str``). Set to a Pydantic ``BaseModel``
  for structured output.
- ``decision_policy``: A ``DecisionPolicy(min_confidence=..., on_uncertain=...)``.
  ``min_confidence`` is the confidence the least confident reporting field needs for
  the operator to return the output without a person, from 0 to 1; no reported
  confidence counts as uncertain. ``on_uncertain`` is ``"review"`` (default) or
  ``"fail"``. Default ``None``: no gate.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai ``Agent``
  constructor (e.g. ``retries``, ``model_settings``, ``tools``). Supports Jinja templating.
- ``usage_limits``: Optional pydantic-ai ``UsageLimits`` enforced on the run, or a
  ``dict`` of the same fields (templated via Jinja, then coerced per field type).
  Fails the task when token / request / tool-call budgets are exceeded, or when a
  templated dict value cannot be coerced.  Default ``None``.
- ``require_approval``: If ``True``, the task defers after generating output and waits
  for human review. Default ``False``. Needs Airflow 3.1+.
- ``approval_timeout``: Maximum time to wait for a review (``timedelta``).  ``None``
  means wait indefinitely.  Default ``None``.
- ``on_approval_timeout``: Outcome when ``approval_timeout`` expires without a
  review: ``"fail"`` (default), ``"approve"``, or ``"reject"``.  Requires a
  review path (``require_approval=True`` or a ``decision_policy`` that reviews)
  and a positive ``approval_timeout``.
- ``allow_modifications``: If ``True``, the reviewer can edit the output before
  approving.  Default ``False``.
- ``approval_notifiers``: Notifier, or list of notifiers, called once the review
  is open.  Default ``None``.
- ``approval_assigned_users``: Users allowed to answer the review, as
  ``{"id": ..., "name": ...}`` dicts where ``id`` is the auth manager's user id.
  ``None`` (default) lets any user with the permission respond.  Fixed at first
  run.  Needs Airflow 3.1+.

Logging
-------

After each LLM call, the operator logs a summary with model name, token usage,
and request count at INFO level. At DEBUG level, the LLM output is also logged
(truncated to 500 characters). See :ref:`AgentOperator — Logging <howto/operator:agent>`
for details on the log format.
