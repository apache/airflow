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

.. _durable-execution:

Durable execution
=================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

An agent task makes several model calls and tool calls. When it fails partway (a
network error, a timeout, a provider outage), a plain Airflow retry runs every one of
them again: the model calls are paid for twice and the tools' side effects happen
twice. With durable execution, each step the agent completes is recorded in a journal
kept for the task instance, and the retry replays the recorded steps instead of running
them again. Only the steps the failed attempt never finished run live.

Durable execution only helps a task that has retries. To decide *whether* a failure is
worth retrying, see :doc:`retry_policies`; a retried ``LLMBatchOperator`` re-attaches to
its running batch instead (:ref:`llm-batch-reattach`).

Turn it on
----------

Set ``durable=True`` on ``AgentOperator``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_operator_agent_durable]
    :end-before: [END howto_operator_agent_durable]

The same flag works on ``@task.agent``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_decorator_agent_durable]
    :end-before: [END howto_decorator_agent_durable]

``durable=True`` attaches the
:class:`~airflow.providers.common.ai.durable.AirflowDurability` capability to the
agent. A pydantic-ai agent you build yourself in a plain ``@task`` takes the capability
directly:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_capability_airflow_durability]
    :end-before: [END howto_capability_airflow_durability]

The capability needs a name for the agent's steps: the agent's ``name``, or
``AirflowDurability(name=...)``. ``AgentOperator`` uses the agent's name when
``agent_params`` sets one, and the task id otherwise. Outside a running Airflow task
the capability does nothing, so the same agent runs normally in a test or a notebook.

Give every toolset an id
------------------------

pydantic-ai records each call of a ``FunctionToolset``, ``MCPToolset`` or
``DynamicToolset`` under the toolset's ``id``, and refuses to run one without it. Set
the id on the toolset itself:

.. code-block:: python

    orders = FunctionToolset(tools=[get_order], id="orders")

``AgentOperator(durable=True)`` checks this when the Dag is parsed and fails with:

.. code-block:: text

    ValueError: durable=True needs a unique id on every FunctionToolset, such as FunctionToolset(..., id='orders'): durable replay records each tool call under it. Set it on the toolset itself, and pass a function that builds a toolset as DynamicToolset(function, id=...); the id of a Toolset capability around a toolset does not reach it.

Keep an id stable once tasks run with it: a retry finds its recorded tool calls by it.
The provider's own toolsets (``SQLToolset``, ``HookToolset`` and the rest) and tools
passed as ``agent_params={"tools": [...]}`` need nothing. A capability that contributes
``@durable_operation`` methods needs an ``id`` for the same reason; pydantic-ai raises
``UserError`` naming it when it has none. pydantic-ai-harness ``SpendLimits`` has one by
default.

Where the journal is kept
-------------------------

On **Airflow 3.3 and later** the journal is kept in the
:doc:`task state store <apache-airflow:core-concepts/task-state-store>`, scoped to the
task instance. No configuration is needed. Each step is written to the Airflow metadata
database; for agents with large model responses or tool results, set
``[workers] state_store_backend`` to offload the values to external storage instead.

On **Airflow before 3.3** the journal is a JSON file on ObjectStorage, and the location
must be set:

.. code-block:: ini

    [common.ai]
    # Local filesystem, suitable for development
    durable_cache_path = file:///tmp/airflow_durable_cache

Any ObjectStorage URI works. In production use a shared store, so a retry on another
worker can read the journal:

.. code-block:: ini

    [common.ai]
    durable_cache_path = s3://my-bucket/airflow/durable-cache

Without the option, the task fails with:

.. code-block:: text

    ValueError: durable=True requires [common.ai] durable_cache_path to be set. Example: durable_cache_path = file:///tmp/airflow_durable_cache

How a retry replays
-------------------

Each step the agent completes is recorded under its position in the run, with its
name (which model or tool it was) and a fingerprint of what it was asked: the model,
the message history, the settings and the tool definitions for a model request; the
tool name, its arguments and the model-issued call id for a tool call.

The retry runs the agent from the start. At each position, a step with the same name
and the same fingerprint as the recorded one is replayed: the recorded result is
returned and the model or tool is not called. The first step that differs means the run
has taken another path, because the prompt, the model, a setting, a tool or the
conversation changed, so that step and every step after it run live and are recorded
again, and the task log says so:

.. code-block:: text

    Durable: the run took a different path from the previous attempt; this step and every step after it run again

A changed agent costs a re-run; it never replays results that belong to another
conversation. Settings that only affect transport, ``timeout`` and the prompt cache
settings, are left out of the fingerprint, so changing them does not cause a re-run.
Replay compares requests only: editing a tool's implementation does not invalidate a
recorded result for an identical call.

When a run fails, the journal notes where. The retry runs the step that failed again,
and runs live everything the failed attempt did after it, such as the error handling of
other capabilities. Tool calls already running alongside the failed one still replay. A
step that raised and that the run recovered from, such as a tool error the model was
told about, does not stop the steps after it from replaying.

Some steps have no request to fingerprint: another capability's
``@durable_operation``, or tool discovery. Such a step replays on its name, and only when
the step before it also replayed.

The journal records:

- model requests, including streamed ones;
- calls to tools from any toolset, including the provider's own toolsets, MCP servers,
  toolsets built per run, and toolsets supplied by capabilities;
- tool discovery for MCP and per-run toolsets;
- ``@durable_operation`` methods of other capabilities, such as the spend accounting of
  pydantic-ai-harness ``SpendLimits`` and the summarization of ``SummarizingCompaction``,
  so a retry neither charges the spend twice nor pays for a summary again.

A tool that raises ``ModelRetry``, ``ToolFailed``, ``ApprovalRequired`` or
``CallDeferred`` replays as the same signal. ``AgentOperator`` does not support tool
approval or deferred tools with ``durable=True``, so there such a call fails the task
with ``UnsupportedToolDeferralError``, as it would without ``durable``. A tool that raises
any other exception runs again on retry.

A task can run more than one agent, and an agent's tool can run another agent; each
agent run is replayed on its own, and one a tool started is replayed under that tool
call. The journal is deleted when the task succeeds, including any steps an earlier
attempt recorded beyond where the last one went, so a task cleared later starts fresh.
In a plain ``@task``, which has no such moment, the capability deletes a run's journal
as soon as that run succeeds: after a failure, only the run that failed replays, and
the agents that ran before it run again.

What the task log shows
-----------------------

``AgentOperator`` logs a summary at the end of every attempt, including the one that
fails. Here an agent asked for two tools in one step: ``charge_card`` succeeded and
``flaky_lookup`` raised, which failed the attempt. The failing attempt logged:

.. code-block:: text

    LLM run failed: requests=1, tool_calls=1, input_tokens=58, output_tokens=9, total_tokens=67
    Durable: replayed 0 steps (0 model, 0 tool, 0 other), recorded 2 new steps (1 model, 1 tool, 0 other)

The retry replayed the model response and ``charge_card``, and ran only ``flaky_lookup``
and the final model request:

.. code-block:: text

    Durable: replayed 2 steps (1 model, 1 tool, 0 other), recorded 2 new steps (1 model, 1 tool, 0 other)
    LLM run complete: model=test, requests=1, tool_calls=1, input_tokens=65, output_tokens=20, total_tokens=85

These lines come from a test agent with two tools, not from the examples above.
``charge_card`` ran once across both attempts, and the retry's usage counts only its own
two live calls. A step that ran live but could not be recorded is named in a WARNING
when it happens; the summary names such tool steps again and counts such model steps.
Per-step detail is logged at DEBUG.

The agent's ``run_id`` and ``conversation_id``
----------------------------------------------

Without ``durable``, each attempt's agent run gets a key for the attempt as its
pydantic-ai ``run_id``: the task instance id on Airflow 3, which changes on every retry. With ``durable=True`` every attempt keeps the **first**
attempt's id, and so does the run's ``conversation_id`` when there is no
``message_history``. Capabilities that key their own state on these ids, such as
pydantic-ai-harness ``SpendLimits`` and ``StepPersistence``, then find the records the
replayed steps refer to: with a per-attempt id, a retry that replays ``SpendLimits``'
accounting for a ``run`` window fails with a ``KeyError``. The ``run_id`` XCom, from a
failed attempt or a successful one, and the ``gen_ai.agent.call.id`` span attribute carry
the same id; the spans of each attempt still carry that attempt's own
``airflow.task_instance.id`` and ``try_number``. The id is stored with the journal and
deleted with it, so a task cleared after it succeeded starts a new one.

Usage and limits
----------------

A replayed step adds nothing to the usage the run counts against ``usage_limits`` or
reports in the ``usage`` XCom: not its request, tokens, cost or tool calls, nor the
usage a replayed ``@durable_operation`` brings back. Each attempt counts only the calls
it actually makes. This holds after clearing a failed task instance too: the clear
starts a fresh budget but keeps the journal, and what the rerun replays from it is free.

Side effects and idempotency
----------------------------

Durable execution records what a tool returned. A replayed tool's code does not run, so a
side effect it had (a file written, a message sent) is not repeated.
A tool that fails *before* its result is recorded runs again on retry, so a tool that
sent an email and then raised sends it again. The same holds when the worker dies
between a tool's side effect and the write of its result. Make tools with side effects
idempotent: check whether the work already happened, or let a database constraint
reject the duplicate.

Tool results are masked by Airflow's secret masker before they are recorded, so a
connection password in a result reaches the journal as ``***``.

A toolset can opt out of replay. ``ManagedAgentToolset`` does by default
(``replayable=False``), because a managed agent acts on a system Airflow cannot observe:
its calls run again on every attempt, while the steps around them still replay.

A result has to survive a JSON round trip to be recorded. One that cannot be encoded
fails the task with ``DurableJournalError`` and no retry, since it would fail the same
way every time; the model could not have read it either. A write the task state store
refuses does not fail the task: the task log names
the step, and a retry runs it again.

Limitations
-----------

- ``durable=True`` cannot be combined with ``enable_hitl_review``, a pydantic-ai-harness
  ``CodeMode`` capability, a ``SandboxToolset`` or tool approval. ``AgentOperator`` raises
  ``ValueError`` for the first three when the Dag is parsed.
- A task instance that is retried across an upgrade of this provider from a version
  without ``AirflowDurability`` runs its steps live once: the journal keys changed.

See also
--------

- :doc:`operators/agent`: the operator these settings apply to.
- :doc:`retry_policies`: let a model decide whether a failure is worth retrying.
- :doc:`troubleshooting`: the ``durable=True`` combinations the operator rejects.
