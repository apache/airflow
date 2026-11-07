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

.. _howto/tool_approval:

Approve an agent's tool calls
=============================

.. meta::
    :description: Pause an agent before it runs a tool you marked for approval, such as one that refunds an order or writes to a table, and let a person approve or reject that call from the Required Actions page.

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

.. seealso::
    To approve, edit or reject an LLM operator's output instead, see :doc:`approval_gates`;
    to review an agent's final answer over several rounds, see :doc:`hitl_review`.

An agent that can only read is easy to trust. The moment one of its tools does
something you cannot undo -- refunds an order, sends an email, writes to a
production table -- you want a person to see the call before it runs, without
giving up the agent's freedom to look things up on its own.

Mark those tools with pydantic-ai's approval API. ``AgentOperator`` and
``@task.agent`` then pause the task in front of a marked call and ask on the
**Required Actions** page:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_tool_approval.py
    :language: python
    :start-after: [START howto_agent_tool_approval]
    :end-before: [END howto_agent_tool_approval]

``lookup_order`` runs whenever the agent calls it. When the agent calls
``refund_order``, the task stops and the reviewer sees the tool name and its
arguments:

.. code-block:: text

    Approve tool call for task `handle_ticket`

    The agent wants to run:

    refund_order
    {
      "order_id": 7
    }

    [Approve]  [Reject]   reason: ______

``.approval_required()`` works on any toolset, including ``SQLToolset`` and
``MCPToolset``, and the function decides per call, so you can gate on the
arguments as well as the tool name. For a single function tool,
``Tool(refund_order, requires_approval=True)`` does the same.

Under ``airflow dags test`` the task waits until someone answers from Required
Actions in the UI of an api-server on the same metadata database;
``airflow standalone`` gives you one.

What each decision does
-----------------------

**Approve** runs the call and the agent carries on from where it stopped.

**Reject** does not fail the task. The agent is told the call was denied -- with
the reviewer's reason when one is given, such as "Refunds over $40 need a support
ticket number." -- and carries on without it, so it can explain the refusal,
ask for what is missing, or take another route.

While it waits, the task is in the ``awaiting_input`` state and holds no worker
slot. When the model calls several tools in one step, the ones that need
approval are decided together and the others run straight away, once.

``usage_limits`` covers both sides of the pause, so a ``cost_limit`` is not reset
by it.

An agent that needs approval more than once
-------------------------------------------

An agent usually decides its next call after it sees the result of the last one, so
a refund agent may ask to refund one order, read the result, and then ask to refund
another. On Airflow 3.4 and later, each step that calls a marked tool pauses the task
again, and the reviewer sees only that step's calls. ``usage_limits`` counts across
every pause, so a run that pauses many times can reach ``request_limit`` (50 by
default) partway through, after earlier approved calls have run.

Each request is numbered in its subject, and the review form sends the number back
with the answer in a hidden field. An answer from a form left open on an earlier
request, such as a second reviewer who never reloaded the page, is not applied to the
calls waiting now: the task asks about them again under a new number. An answer sent
through the REST API without that field is applied to the waiting request.

A retry or a clear asks again for the calls the new attempt makes, including ones a
reviewer approved in an earlier attempt that then failed. The approved call already
ran once, so keep gated tools idempotent.

On Airflow 3.3 a task instance can ask only once per Dag run, retries and clears
included. Airflow 3.3 keeps the first request's subject and body when the same task
instance asks again, so a second request would show the reviewer the earlier call
while asking about the new one. The task fails with
``ToolApprovalAlreadyRequestedError`` instead, and does not retry. On 3.3, have the
agent request the gated calls in one step, or give each irreversible action its own
task.

Timeouts and who decides
------------------------

``tool_approval_timeout`` bounds the pause; ``None`` (the default) waits for as
long as it takes. What a timeout does is set by ``on_tool_approval_timeout``:

- ``"fail"`` (default) fails the task.
- ``"deny"`` rejects the pending calls, and the agent carries on without them.
  The agent is told nobody answered in time, not that a person refused. ``"deny"``
  needs a ``tool_approval_timeout``.

There is no approve-on-timeout: a pause that approves itself when nobody answers
guards nothing.

``tool_approval_assigned_users`` limits who may decide, with the same user list
``require_approval`` on the LLM operators takes (see :doc:`approval_gates`).

The arguments shown to the reviewer pass through Airflow's secrets masker first,
so a value under a key such as ``api_key`` or ``password``, or a value already
registered as a secret, is shown masked.

.. _howto/tool_approval_judge:

Let a decision model approve the routine calls
----------------------------------------------

Most refunds are routine, and a person who approves every one of them stops reading
them. A :doc:`decision model <decision_models>` can answer "may this call run without
a person?" for each one, and pydantic-ai's ``HandleDeferredToolCalls`` capability is
the place to ask: its handler sees every call waiting for approval, with its
validated arguments, before the task pauses. It approves the calls the model is sure
about, denies the ones the model is sure should not run, and leaves the rest. The
task pauses on whatever it leaves, exactly as above.

A limit you can write down, such as "refunds over $50", needs no model: the
``.approval_required()`` function above can check the arguments itself. The model
earns its call on the question a rule cannot answer, whether the call fits the
request: the right order, an amount the customer's problem supports, an action
somebody asked for.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_tool_judge.py
    :language: python
    :start-after: [START howto_agent_tool_judge]
    :end-before: [END howto_agent_tool_judge]

What happens to each call:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - The model answers
     - Confidence at or above ``MIN_CONFIDENCE``
     - Below it, or none reported
   * - ``allow``
     - The call runs.
     - A person decides.
   * - ``deny``
     - The call does not run; the agent is told, as after a Reject.
     - A person decides.
   * - ``review``
     - A person decides.
     - A person decides.
   * - The model call raises
     - A person decides.
     - A person decides.

Once a person approves, the call runs without the model being asked again. Once a
call to a tool has been refused, by a person or by the model, later calls to that
tool in the same run go to a person: the model does not see why the earlier call was
refused, so it does not get to overrule that.

**Keep hard limits in the tool.** A decision model reads a tool argument as data, not
as an attempt to manipulate it, so an argument written to argue for its own approval
can move the answer (see :doc:`decision_models`). ``within_order_total`` is the
tool's ``args_validator``: it runs in the worker after the arguments validate and
before the call is marked for approval, so a refund above the order total goes back
to the agent without the model or a person being asked about it.

**Set the bar from your own calls.** Run the handler over calls your agent actually
makes, and raise ``MIN_CONFIDENCE`` until the calls it lets through are ones you
would have approved. A model that hesitates sends more calls to a person, which
costs time; one that is trusted too far costs whatever the tool does.

The Required Actions page shows the call, not the model's answer; the handler logs
the answer and its confidence to the task log, where the reviewer can look. A call the
handler leaves for a person follows every rule on this page, including how often one
task instance can ask in a Dag run. Where tool approval is not available, such a call
fails the task. An exception the handler does not catch, such as a missing
``decision_default`` connection, fails the task as well.

Requirements and limits
-----------------------

- Airflow 3.3 or later. On older versions a tool marked for approval fails the
  task, as it did before.
- Not together with ``durable=True``, ``enable_hitl_review=True``, a ``CodeMode``
  capability, or a ``SandboxToolset`` that
  provisions its own sandbox. Each
  assumes the run finishes in one go; that sandbox, for one, is destroyed when the
  run pauses. With any of them, a marked tool fails the task, except under code mode,
  where a marked tool called from ``run_code`` does not run and the model may carry on
  without it (see :doc:`code_mode`). A ``SandboxToolset``
  attached to a sandbox another task owns keeps its files through the pause, so it
  is allowed; the wait spends that sandbox's lifetime
  (:ref:`sandbox-attach`).
- Tools that hand work to an external system (pydantic-ai's ``CallDeferred``) are
  not supported; the task fails without retrying.
- The conversation so far, including tool results, is kept in the task's
  :doc:`task state store <apache-airflow:core-concepts/task-state-store>` while
  the task waits. It is deleted when the resumed run succeeds or fails, and replaced
  when it pauses again; a later try deletes one left behind by a try that ended
  while waiting. With a ``tool_approval_timeout``, it also expires a day after the
  timeout.
- If a templated connection id (see :ref:`sql-toolset-templated-connection`) renders
  differently when the task resumes, the task fails rather than run an approved
  call against a connection the reviewer did not see. The check compares toolset
  ids, which name the connection for ``SQLToolset``, ``MCPToolset`` and
  ``HookToolset``; it does not notice an edit to the connection itself.
- Each resumed run gets its own pydantic-ai ``run_id``,
  ``<task-instance id>-resumed-<n>`` when it resumes from pause ``n``. The ``run_id`` XCom
  holds the last one, and the ``usage`` XCom covers every part of the run.
