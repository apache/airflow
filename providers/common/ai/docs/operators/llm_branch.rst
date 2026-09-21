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

.. _howto/operator:llm_branch:

``LLMBranchOperator``
=====================

Use :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
for LLM-driven branching — where the LLM decides which downstream task(s) to
execute.

The operator discovers downstream tasks automatically from the Dag topology
and presents them to the LLM as a constrained enum via pydantic-ai structured
output. No text parsing or manual validation is needed.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Connect the operator to downstream tasks. The LLM chooses which branch to
execute based on the prompt:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_basic]
    :end-before: [END howto_operator_llm_branch_basic]

Describing the Branches
-----------------------

By default the model sees each branch as its task ID and nothing else. That is
enough when the IDs speak for themselves and the prompt clearly fits one of
them. It is not enough when two branches could plausibly own the same input:
in the example above, a missing password-reset email is a sign-in problem to
one team and an email problem to another, and nothing tells the model which
team owns it.

``branches`` maps a downstream task ID to what choosing that branch means. A
string is the description; a ``BranchOption`` carries the description and,
when the branch needs it, its own confidence bar (see below). The descriptions
travel in the output schema next to the option they describe, so the model
reads each option together with its meaning rather than matching prose in the
system prompt back to a task ID by name:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_descriptions]
    :end-before: [END howto_operator_llm_branch_descriptions]

Three fields, three roles. ``prompt`` is the thing being classified.
``system_prompt`` is the decision to make and the rules that apply across all
options, including how to break ties. Each ``branches`` entry is what
selecting that option means: its scope and its boundary cases. A rule that
applies to one branch belongs in that branch's description; a rule that
applies to the whole decision belongs in the system prompt. Say each thing
once, in one place.

A downstream task without an entry is presented by its ID alone, as before,
so a partial mapping is fine. A key that is not a downstream task ID fails the
task before the model is called, with the valid task IDs in the message; a
misspelled key silently turning into an option with no description is
exactly the problem this parameter exists to prevent. Descriptions support
Jinja templating, and the mapping works with ``allow_multiple_branches=True``
and with the ``@task.llm_branch`` decorator. Under the hood the options are
pydantic-ai's ``Choices`` type (2.46+), or an equivalent enum on older
releases, so the model can only answer with one of the task IDs.

Descriptions explain the choices; they do not make the model more certain,
and a text model's structured output carries no confidence to read. With a
classifier model such as TypeSafe's, the descriptions become the criteria of
its choice question, which is the text it weighs each option by.

Multiple Branches
-----------------

Set ``allow_multiple_branches=True`` to let the LLM select more than one
downstream task. All selected branches run; unselected branches are skipped:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_multi]
    :end-before: [END howto_operator_llm_branch_multi]

TaskFlow Decorator
------------------

The ``@task.llm_branch`` decorator wraps ``LLMBranchOperator``. The function
returns the prompt string; all other parameters are passed to the operator:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_decorator_llm_branch]
    :end-before: [END howto_decorator_llm_branch]

The callable may also return a non-empty ``Sequence[UserContent]`` for
multimodal inputs -- see
:ref:`@task.agent multimodal prompts <howto/operator:agent-multimodal>`.

With multiple branches:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_decorator_llm_branch_multi]
    :end-before: [END howto_decorator_llm_branch_multi]

Human-in-the-Loop Approval
--------------------------

Set ``require_approval=True`` to pause the task after the LLM chooses the
branch(es) and wait for a human reviewer to approve the choice before any
downstream task is skipped. The review form shows the LLM's choice and the
valid downstream task IDs. When ``allow_modifications=True``, the reviewer
can also change the choice — rendered as a dropdown of the downstream task
IDs, or a multi-select of them with ``allow_multiple_branches=True``. The
reviewed branch(es) are validated
against the downstream task IDs before branching:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_approval]
    :end-before: [END howto_operator_llm_branch_approval]

Rejecting the review **skips the direct downstream tasks except teardowns**,
matching
:class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`. The
teardown carve-out applies only to rejection: approving branches as usual,
so a teardown that is not among the chosen branch(es) is skipped like any
other unselected downstream task. Set ``fail_on_reject=True`` to fail the
task on rejection instead (generally discouraged), or
``ignore_downstream_trigger_rules=True`` to skip every downstream task rather
than only the direct ones, so a task whose trigger rule would still run it is
skipped too. Letting ``approval_timeout`` expire fails the task
(``HITLTimeoutError``) unless ``on_approval_timeout`` answers the review for
you; a timeout-driven rejection then skips downstream like any other rejection.

``require_approval=True`` requires a string prompt: a decorated callable
returning a ``Sequence[UserContent]`` raises ``TypeError`` before the LLM
call.

Apart from ``fail_on_reject`` and ``ignore_downstream_trigger_rules``, which
are specific to this operator, ``approval_timeout``, ``on_approval_timeout``,
``approval_notifiers``, ``approval_assigned_users``, and the rest of the approval
behaviour are inherited from :ref:`LLMOperator <howto/operator:llm>`.

Reviewing Uncertain Picks
-------------------------

A classifier model such as TypeSafe's returns a confidence with every pick,
a number from 0 to 1 that summarizes how concentrated its probability
distribution was: near 1 when one branch stood out, low when two or more
were close. It is not the probability that the pick is right. It is the
model saying how clear-cut the question was, and it is the signal you gate on.

``decision_policy`` says when the operator may branch by itself. Import it and
``BranchOption`` from ``airflow.providers.common.ai.operators.llm_branch``.
``DecisionPolicy(min_confidence=0.6)`` is the bar a pick has to clear, and
``on_uncertain`` is what happens under it: ``"review"`` (default) sends the pick
to human review, through the same approval flow as ``require_approval``;
``"fail"`` fails the task. A ``BranchOption`` in ``branches`` can raise or lower
the bar for its own branch, so a branch whose wrong pick costs more can demand
more certainty; every other branch takes the policy's bar, so a new downstream
task never slips past the gate by accident:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_branch.py
    :language: python
    :start-after: [START howto_operator_llm_branch_decision_policy]
    :end-before: [END howto_operator_llm_branch_decision_policy]

The review form shows the pick, the confidence, the bar it fell under and the
full distribution, so the reviewer sees what the model saw. With
``allow_multiple_branches=True`` the strictest bar among the picked branches
applies.

Four situations, each with a defined outcome:

- **Confidence at or above the bar**: the operator branches, as today.
- **Confidence below the bar**: ``on_uncertain`` applies. With ``"review"``
  the pick goes to a person: approve to branch on it, change it if
  ``allow_modifications=True``, or reject to skip downstream. With ``"fail"``
  the task fails with ``LowConfidenceError`` (from
  ``airflow.providers.common.ai.exceptions``) and the record says why. Airflow
  retries that like any other failure, so with ``retries`` set the model is
  asked again on each attempt; match the exception in a retry rule to fail
  fast instead.
- **No confidence reported**, because the model is a text model or the
  metadata was lost on the way: treated as uncertain, so switching the
  connection to a model that reports nothing does not silently switch off a
  bar you set. The record shows ``"missing_confidence"`` and the model that
  answered.
- **``require_approval=True``**: every pick goes to a person, whatever the
  confidence. ``decision_policy`` is the conditional setting and does not
  change what ``require_approval`` means.

Without a ``min_confidence`` on the policy nothing here applies and the
operator behaves as before. ``on_uncertain="review"`` needs Airflow 3.1+, like
``require_approval``, and is rejected at construction on an older core. The
review it opens is the same one ``require_approval`` opens: ``approval_timeout``,
``on_approval_timeout``, ``allow_modifications``, ``approval_notifiers`` and
``approval_assigned_users`` all apply to it.

TypeSafe's models need pydantic-ai 2.45 or later with its ``typesafe`` extra
(``pip install "pydantic-ai-slim[typesafe]"``) and a ``pydanticai`` connection
whose Model is ``typesafe:jev-1.13.0`` (or the ``model_id`` on the operator, as
in the example).

The decision record
~~~~~~~~~~~~~~~~~~~

Whether or not a bar is set, the operator pushes a ``decision`` XCom next to
its return value (suppressed by ``do_xcom_push=False`` like any other):

.. code-block:: json

    {
      "model": "jev-1.13.0",
      "proposed": "page_oncall",
      "action": "rerun",
      "confidence": {"response": 0.52},
      "probabilities": {"response": {"page_oncall": 0.52, "rerun": 0.46, "ignore": 0.02}},
      "min_confidence": 0.9,
      "review": "below_threshold",
      "decided_by": "human",
      "policy": {"min_confidence": 0.6, "on_uncertain": "review", "branches": {"page_oncall": 0.9}},
      "descriptions": {"page_oncall": "...", "rerun": "...", "ignore": "..."}
    }

``proposed`` is what the model picked and ``action`` what ran; they differ when
a reviewer changed the pick, and ``action`` is ``null`` when a review is still
open or ended in a rejection. ``confidence`` and ``probabilities`` are keyed by
output field, and a branch pick is the one field ``response``; both are empty
for a model that reports nothing. ``min_confidence`` is the bar that applied to
the pick, after any per-branch override. ``review`` is ``null``,
``"require_approval"``, ``"below_threshold"`` or ``"missing_confidence"``.
``decided_by`` is ``null`` while a review is open, then ``"model"``,
``"human"`` (an approval or a rejection), ``"timeout_default"`` when
``on_approval_timeout`` answered the review, ``"timeout"`` when it expired with
no default and the task failed, or ``"policy"`` when ``on_uncertain="fail"``
failed the task. ``model`` is the versioned name
that answered, so a bar tuned against one release can be tied to it. ``policy``
is the ``DecisionPolicy`` in force plus any per-branch bars, and
``descriptions`` the branch descriptions the model read
(``null`` when none were given), so the record explains a decision on its own
after the Dag file has changed.

When a pick goes to review, the pending record is also checkpointed with the
paused task, next to the proposed pick. The task finalizes the record from that
copy when it resumes, so an XCom that was edited or deleted while the review was
open does not change what is recorded. The XCom is for reading; the paused task
carries its own state.

How It Works
------------

At execution time, the operator:

1. Reads ``self.downstream_task_ids`` from the Dag topology.
2. Builds the option type (pydantic-ai's ``Choices`` on 2.46+, an equivalent
   enum before) with one option per downstream task ID, in sorted order so every
   worker presents the options the same way. With
   descriptions in ``branches``, the schema is an ``anyOf`` of ``{"const":
   <task_id>, "description": <text>}`` entries, which is the one schema shape that
   carries a description per value.
3. Passes that type as ``output_type`` to ``pydantic-ai``, constraining the LLM
   to valid task IDs only.
4. Reads the model's confidence for the pick from ``provider_details`` (a
   classifier model reports one; a text model does not), pushes the
   ``decision`` XCom, and if the ``decision_policy`` or ``require_approval`` says
   so, pauses for human review (or fails, with ``on_uncertain="fail"``).
5. Converts the LLM's structured output to task ID string(s) and calls
   ``do_branch()`` to skip non-selected downstream tasks.

Parameters
----------

- ``prompt``: The prompt to send to the LLM (operator) or the return value of the
  decorated function (decorator).
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the connection's extra field.
- ``system_prompt``: System-level instructions for the agent. Supports Jinja templating.
- ``branches``: Optional mapping of downstream task ID to a ``BranchOption`` (a
  description and, optionally, a ``min_confidence`` for that branch) or to a string as
  shorthand for the description. Sent to the model in the output schema next to the
  option. Unlisted tasks are presented by ID alone and take the policy's bar; a key that
  is not a downstream task ID fails the task before the model call. Descriptions support
  Jinja templating. Default ``None``.
- ``allow_multiple_branches``: When ``False`` (default) the LLM returns a single
  task ID. When ``True`` the LLM may return one or more task IDs.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai ``Agent``
  constructor (e.g. ``retries``, ``model_settings``). Supports Jinja templating.
- ``usage_limits``: Optional pydantic-ai ``UsageLimits`` (or a templated ``dict`` of
  the same fields) enforced on the run; the task fails when a budget is exceeded.
  Default ``None``. See :ref:`Usage Limits <howto/operator:llm_usage_limits>`.
- ``decision_policy``: A ``DecisionPolicy(min_confidence=..., on_uncertain=...)``.
  ``min_confidence`` is the confidence a pick needs for the operator to branch on it
  without a person, from 0 to 1; no reported confidence counts as uncertain.
  ``on_uncertain`` is ``"review"`` (default) or ``"fail"``. A ``BranchOption``'s own
  ``min_confidence`` overrides the policy's for that branch and requires the policy to
  set one. Default ``None``: no gate.
- ``require_approval``: If ``True``, the task pauses after the LLM chooses the
  branch(es) and waits for human review before branching, whatever the confidence.
  Default ``False``.
- ``approval_timeout``: Maximum time to wait for a review (``timedelta``).  ``None``
  means wait indefinitely.  Default ``None``.
- ``on_approval_timeout``: Outcome when ``approval_timeout`` expires without a
  review: ``"fail"`` (default), ``"approve"``, or ``"reject"``.  Requires a
  review path (``require_approval=True`` or a ``decision_policy`` that reviews)
  and a positive ``approval_timeout``.
- ``allow_modifications``: If ``True``, the reviewer can change the chosen
  branch(es) before approving.  Default ``False``.
- ``approval_notifiers``: Notifier, or list of notifiers, called once the review
  is open.  Default ``None``.
- ``approval_assigned_users``: Users allowed to answer the review.  ``None``
  (default) lets any user with the permission respond.  Needs Airflow 3.1+.
- ``fail_on_reject``: If ``True``, a rejected review fails the task instead of
  skipping the downstream tasks.  Generally discouraged.  Only takes effect
  when a review is opened.  Default ``False``.
- ``ignore_downstream_trigger_rules``: If ``True``, a rejected review skips every
  downstream task rather than only the direct ones.  Only takes effect when a
  review is opened.  Default ``False``.

Logging
-------

After each LLM call, the operator logs a summary with model name, token usage,
and request count at INFO level. See :ref:`AgentOperator — Logging <howto/operator:agent>`
for details on the log format.
