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

One approval per task instance
------------------------------

A task instance asks for approval at most once per Dag run, and that includes its
retries and clears. Airflow keeps a single approval request per task instance,
and a second request would show the reviewer the first one's tool call while
asking about the new one. So when the agent asks again -- later in the same run,
or in a retry after the first request -- the task fails with
``ToolApprovalAlreadyRequestedError`` and does not retry.

Have the agent request the gated calls in one step, or give each irreversible
action its own task. A task whose approved call ran and that then failed cannot
ask again in the same Dag run; trigger a new run, and keep gated tools
idempotent, since the approved call has already happened once.

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

Requirements and limits
-----------------------

- Airflow 3.3 or later. On older versions a tool marked for approval fails the
  task, as it did before.
- Not together with ``durable=True``, ``enable_hitl_review=True``,
  ``code_mode=True``, or a ``SandboxToolset``. Each assumes the run finishes in one
  go; a sandbox, for one, is destroyed when the run pauses. With any of them, a
  marked tool fails the task.
- Tools that hand work to an external system (pydantic-ai's ``CallDeferred``) are
  not supported; the task fails without retrying.
- The conversation so far, including tool results, is kept in the task's
  :doc:`task state store <apache-airflow:core-concepts/task-state-store>` while
  the task waits. It is deleted when the task resumes, whether the resumed run
  succeeds or fails, and a later try deletes one left behind by a try that ended
  while waiting. With a ``tool_approval_timeout``, it also expires a day after the
  timeout.
- If a templated connection id (see :ref:`sql-toolset-templated-connection`) renders
  differently when the task resumes, the task fails rather than run an approved
  call against a connection the reviewer did not see. The check compares toolset
  ids, which name the connection for ``SQLToolset``, ``MCPToolset`` and
  ``HookToolset``; it does not notice an edit to the connection itself.
- The resumed run gets its own pydantic-ai ``run_id``, ``<task-instance id>-resumed``.
  The ``run_id`` XCom holds that id, and the ``usage`` XCom covers both sides of
  the pause.
