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

.. _howto/frameworks:strands:

Strands Agents
==============

You have an agent built with `Strands Agents <https://strandsagents.com/>`__ and
you want it to run as an Airflow task: after an upstream task, on a schedule, or
when an asset updates, reading data through connections your deployment already
manages. Keep the agent as it is. Build the model and the ``Agent`` yourself, and
give it Airflow's toolsets with the
:class:`~airflow.providers.common.ai.tools.strands.AirflowTools` plugin.

.. note::

    ``AirflowTools`` and the framework-neutral tool interface it builds on are
    experimental. They may change in a minor release of this provider.
    See :ref:`howto/stability`.

Run a Strands agent in a task
-----------------------------

The agent below answers a question about a database. Its model's API key comes
from an Airflow connection, and its tools come from a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`, so the agent can
list tables, read schemas and run read-only queries, with results bounded to
what fits in the model's context:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands_agent.py
    :language: python
    :start-after: [START example_strands_agent]
    :end-before: [END example_strands_agent]

Everything except ``AirflowTools`` is plain Strands. The agent's loop, model
features, hooks and session handling work as the Strands documentation describes,
and tools you wrote with Strands' own ``@tool`` decorator go in ``tools=`` as
usual, next to the plugin.

The model can come from any connection that holds its credentials. For Strands'
default Bedrock model, build ``BedrockModel(boto_session=...)`` from
``AwsBaseHook(aws_conn_id=...).get_session()`` in the Amazon provider, so the
model uses the same AWS connection as the rest of your Dags.

What the plugin gives the agent
-------------------------------

``AirflowTools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, such as
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`, and adds one
Strands tool per toolset tool. ``MCPToolset`` is the exception: connect Strands' own
``MCPClient`` to the server instead. Each tool:

- Keeps the toolset tool's name, description and argument schema.
- Runs through the toolset itself, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result bounds behave as they
  do in :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.
- Passes every result through Airflow's secret masker before it goes back to the
  model.
- Writes a ``Tool call: <name>`` group to the task log with the call's duration.

Failures follow the same rule as in ``AgentOperator``:

- A failure the model can correct, such as a rejected SQL statement or an invalid
  argument, reaches it as a Strands result with ``status="error"``, and the model
  can try again. The tool's retry limit bounds how many times in a row it can do so.
  ``SQLToolset`` treats every database error this way, so a query that fails because
  the database rejects the connection's credentials ends the run once that limit is
  reached.
- A refusal the tool reports as final, by raising pydantic-ai's ``ToolFailed``, also
  reaches the model as an error, and does not count against the retry limit. Bound the task with ``execution_timeout`` if a model could
  keep asking for it.
- Any other failure, such as a hook method raising, ends the agent run. Strands raises
  ``EventLoopException`` with the masked
  :class:`~airflow.providers.common.ai.tools.ToolCallError` as its
  ``original_exception``, the task fails, and Airflow's own retry takes over. Strands
  on its own would hand such an error to the model and let the run carry on.

Outside ``AgentOperator``, a toolset's connection ID is used as written: it is not
rendered as a template, so pass the value itself rather than a Jinja expression.

The plugin is built on the framework-neutral tool interface described in
:ref:`howto/frameworks`, which is also the starting point for an adapter to another
framework.

Why masking tool results matters
--------------------------------

Airflow masks connection passwords in task logs. Tool results are not logs:
they go to the model, to the model provider's API, into any traces the framework
records, and often into the agent's final answer, which your task may return as
an XCom. A tool whose client library puts a credentialed URL in its error message
would send the raw password to all of those places, even though the task log
shows ``***``.

Tools added by ``AirflowTools`` apply the masker to what they return, so a
registered secret is replaced with ``***`` before it leaves the tool. The masker
knows the secrets Airflow has registered, such as connection passwords and
sensitive connection extras. It does not recognize a credential that exists only
in the data a tool returns, so the permissions of the connection's database role
or cloud identity remain the real boundary on what the agent can reach.

Tools you write yourself with Strands' ``@tool`` decorator do not pass through
the masker. To give one of your own functions the same treatment, wrap it as an
:class:`~airflow.providers.common.ai.tools.AirflowTool` and pass it to
``AirflowTools`` alongside the toolsets:

.. code-block:: python

    import asyncio

    from airflow.providers.common.ai.tools import AirflowTool, ToolResult
    from airflow.providers.common.ai.tools.strands import AirflowTools


    async def lookup_customer(arguments: dict) -> ToolResult:
        # crm_client.get blocks, so run it off the agent's event loop.
        customer = await asyncio.to_thread(crm_client.get, arguments["customer_id"])
        return ToolResult(content=customer)


    lookup = AirflowTool(
        name="lookup_customer",
        description="Look up one customer in the CRM by id.",
        parameters={
            "type": "object",
            "properties": {"customer_id": {"type": "integer"}},
            "required": ["customer_id"],
        },
        function=lookup_customer,
    )
    agent = Agent(model=model, plugins=[AirflowTools(warehouse, lookup)])

Tracing
-------

The example runs the agent inside
:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing`, so Strands'
OpenTelemetry spans carry the task's identity and leave out prompts, completions and tool
inputs and outputs unless ``[common.ai] capture_content`` is on. Create the ``Agent``
inside the block: Strands reads the switch once per process, when it creates its one
tracer, so an ``Agent`` created earlier in the process, such as at module level, keeps
content capture on. See
:doc:`../observability`.

.. _howto/frameworks:strands-durable:

Durable execution with Strands
------------------------------

When a task that calls ``agent(question)`` fails and Airflow retries it, the agent
starts again from the prompt: every model call is paid for again, and every tool
call happens again. On a task with ``retries``, run the agent with
:func:`~airflow.providers.common.ai.durable.strands.invoke_durably` instead, and a
retry resumes it from the last cycle boundary it reached:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands_durable.py
    :language: python
    :start-after: [START example_strands_durable]
    :end-before: [END example_strands_durable]

The example needs Airflow 3.3 or later, ``strands-agents``, a connection holding an
Anthropic API key, and a database connection; its module docstring lists the details.

``invoke_durably`` takes a function that builds the agent, here
``functools.partial(Agent, ...)``, and calls it with two more arguments:
``checkpointing=True`` and a ``SnapshotSessionManager``. Strands
`pauses an agent built with checkpointing
<https://github.com/strands-agents/harness-sdk/blob/main/strands-py/src/strands/experimental/checkpoint/checkpoint.py>`__
at each boundary of a cycle that calls tools: once the model has asked for tools,
and once they have run. It leaves keeping the conversation to a session manager.
At each pause ``invoke_durably`` has the session manager save the conversation,
with where the agent paused in ``agent.state["airflow_checkpoint"]``, as one
snapshot in the task instance's
:doc:`task state store <apache-airflow:core-concepts/task-state-store>`, and the
agent carries on. Strands saves the snapshot on its own only to redact or trim the
conversation it already holds, so a failed invocation leaves the last pause's
snapshot as it was. On a retry, the agent is rebuilt from the snapshot and resumed
from the pause. The task log says where. This line comes from a test Dag whose
model failed in its second cycle; ``cycle`` counts from 0:

.. code-block:: text

    [2026-10-07 11:03:38] INFO - Resuming the Strands agent from its last checkpoint session_id=f711ace605c22a218c598a0f9e9a19a3d392fbcf6551055d8af473bdff33dd72 position=after_tools cycle=0

``invoke_durably`` returns at the agent's first stop that is not a pause: its final
answer, or an interrupt or a cancellation, which it does not resume. It deletes the
saved state then, so clearing the task later starts the agent over. If that delete
fails, the task still succeeds and logs ``Could not delete the Strands agent's saved
state``, and a later clear of the whole Dag run resumes from the last pause.

A retry runs some work again:

- It resumes from a cycle boundary. If the task fails while a cycle's tools run,
  the retry runs all of that cycle's tools again, the finished ones included, so
  tools that change data must still be safe to repeat.
- A task that fails before the model's first request for tools starts over.
- A retry whose prompt differs from the first try's starts over.
- The state is deleted when ``invoke_durably`` returns, not when the task succeeds.
  If the task fails after that, for example in a second agent or while returning
  the answer, the retry runs the finished agent again from its prompt.
- A ``SandboxToolset`` without ``attach_to`` is destroyed when the try ends, so a
  resumed agent works from a conversation that describes files the retry's new,
  empty sandbox does not have. Attach the toolset to a sandbox another task owns,
  or keep sandbox work out of a durable agent.

A retry resumes the saved conversation with the system prompt it was saved with.
So does clearing the whole Dag run, which keeps task state. To run a changed system
prompt on a failed task, clear the task itself without keeping its task state.

An agent whose model keeps the conversation server-side (``model.stateful``) raises
``ValueError``: Strands' `session manager
<https://github.com/strands-agents/harness-sdk/blob/main/strands-py/src/strands/session/snapshot_session_manager.py>`__
drops a restored conversation for such a model, so it cannot be resumed.

The snapshot holds the whole conversation (the prompt, the model's replies, every
tool call and result), the system prompt and ``agent.state``. It goes to the
metadata database unless ``[workers] state_store_backend`` sends task state to
external storage, and anyone who can read the task instance's task state can read
it. Each pause rewrites it whole, and once it is larger than
``[state_store] max_value_storage_bytes`` (64 KB by default) every write logs a
warning; the write still goes through. ``AirflowTools`` masks
registered secrets in its tools' results; results of your own ``@tool`` functions
are saved as returned. A task that never succeeds leaves its state in the task
state store until the Dag run is deleted. A snapshot offloaded to a custom
``[workers] state_store_backend`` stays in that storage after the rows are gone,
and clearing the task does not reach it.

Within one try, a tool's retry limit in ``AirflowTools`` counts across the agent's
pauses; a task retry starts the count again. The ``AgentResult`` that
``invoke_durably`` returns comes from the agent's last invocation. Its metrics
cover every invocation of the current try, one per resume in
``metrics.agent_invocations``, and none of the earlier tries.

When a task runs more than one agent with ``invoke_durably``, give each a
``session_id`` of its own that stays the same across tries.

The task state store needs Airflow 3.3 or later. On an earlier version,
``invoke_durably`` without ``storage`` raises
``AirflowOptionalProviderFeatureException``. Pass a Strands storage instead, such
as ``S3Storage``, inside the task; ``AwsBaseHook`` comes from the Amazon provider:

.. code-block:: python

    from strands.storage import S3Storage

    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    storage = S3Storage(
        "my-agent-state",
        prefix="strands/",
        boto_session=AwsBaseHook(aws_conn_id="aws_default").get_session(),
    )
    result = invoke_durably(agent, question, storage=storage)

The default ``session_id`` is derived from the Dag id, run id, task id and map
index, so it differs between Dag runs. If you pass your own ``session_id`` and the
storage is shared across Dag runs, as a bucket is, it must differ between runs too. ``invoke_durably`` deletes the
state when the agent finishes or a try starts over; a task that never succeeds
leaves it in the bucket.

Strands marks checkpointing experimental (``strands.experimental.checkpoint``). It
needs ``strands-agents`` 1.51 or later, which the version in
:ref:`howto/frameworks:strands-install` covers. For Pydantic AI agents, see
:doc:`../durable_execution`.

Differences from ``AgentOperator``
----------------------------------

The agent is Strands', so features that ``AgentOperator`` implements on top of
Pydantic AI work differently or do not apply:

- Durable execution resumes from a cycle boundary rather than replaying each
  model call and tool call. See :ref:`howto/frameworks:strands-durable`.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``.

.. _howto/frameworks:strands-install:

Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-common-ai "strands-agents>=1.56.0"

Pin the ``strands-agents`` lower bound as above; 1.56 is the release this
integration is tested with. Without a bound, a resolver that cannot satisfy Strands'
dependencies may pick the ``0.0.1`` placeholder release instead of reporting a
conflict. For ``AnthropicModel``, install ``anthropic`` through this provider's
``anthropic`` extra rather than through Strands' own ``anthropic`` extra, which
requires an ``anthropic`` release older than 1.0. Strands also pins ``mcp`` below 2.2, so
installing it downgrades a newer ``mcp``, which ``MCPToolset`` uses.
