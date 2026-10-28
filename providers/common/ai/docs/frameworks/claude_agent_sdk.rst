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

.. _howto/frameworks:claude_agent_sdk:

Claude Agent SDK
=================

If your agent is a loop on the `Claude Agent SDK
<https://platform.claude.com/docs/en/api/agent-sdk/overview>`__'s ``query()``, which runs
the Claude Code CLI as a subprocess, you can run it in an Airflow task as it is and give
it the toolsets this provider ships.
:class:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools` serves
``SQLToolset``, ``HookToolset`` and most other toolsets to the CLI through an in-process
MCP server. If you have no loop yet, ``AgentOperator`` runs one for you, with durable
replay and human review; see :doc:`index`.

.. note::

    ``AirflowTools`` is experimental: it may change in a minor release of this provider.
    The framework-neutral tool interface it builds on is stable.
    See :ref:`howto/stability`.

Run a loop in a task
---------------------

This task answers a question about a database. The client's API key comes from an
Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_claude_agent_sdk.py
    :language: python
    :start-after: [START example_claude_agent_sdk]
    :end-before: [END example_claude_agent_sdk]

The model can call ``list_tables``, ``get_schema``, ``query`` and ``check_query``, the
tools ``SQLToolset`` serves. Each call writes a group to the task log. This is the log of
one run against a temporary SQLite database, captured as the task logger wrote it, for
the question "Inspect the columns of each table first with get_schema, then tell me
which customer name has the highest total order amount and in which city they live."
Which tools the model calls, in what order, and how long each takes depend on the model
and the question:

.. code-block:: text

    2026-10-19T07:56:54.324694Z [info     ] ::group::Tool call: list_tables [airflow.providers.common.ai.tools] loc=__init__.py:116
    2026-10-19T07:56:54.603210Z [info     ] Tool list_tables returned in 0.28s [airflow.providers.common.ai.tools] loc=__init__.py:130
    2026-10-19T07:56:54.603326Z [info     ] ::endgroup::                   [airflow.providers.common.ai.tools] loc=__init__.py:132
    2026-10-19T07:56:56.308886Z [info     ] ::group::Tool call: get_schema [airflow.providers.common.ai.tools] loc=__init__.py:116
    2026-10-19T07:56:56.309896Z [info     ] Tool get_schema returned in 0.00s [airflow.providers.common.ai.tools] loc=__init__.py:130
    2026-10-19T07:56:56.310010Z [info     ] ::endgroup::                   [airflow.providers.common.ai.tools] loc=__init__.py:132
    2026-10-19T07:56:56.476896Z [info     ] ::group::Tool call: get_schema [airflow.providers.common.ai.tools] loc=__init__.py:116
    2026-10-19T07:56:56.478181Z [info     ] Tool get_schema returned in 0.00s [airflow.providers.common.ai.tools] loc=__init__.py:130
    2026-10-19T07:56:56.478343Z [info     ] ::endgroup::                   [airflow.providers.common.ai.tools] loc=__init__.py:132
    2026-10-19T07:56:58.700718Z [info     ] ::group::Tool call: query      [airflow.providers.common.ai.tools] loc=__init__.py:116
    2026-10-19T07:56:58.704926Z [info     ] Running statement: SELECT c.name, c.city, SUM(o.amount) AS total_amount
    FROM customers c
    JOIN orders o ON o.customer_id = c.id
    GROUP BY c.id, c.name, c.city
    ORDER BY total_amount DESC
    LIMIT 5, parameters: None [airflow.task.hooks.airflow.providers.sqlite.hooks.sqlite.SqliteHook] loc=sql.py:896
    2026-10-19T07:56:59.495567Z [info     ] Tool query returned in 0.79s   [airflow.providers.common.ai.tools] loc=__init__.py:130
    2026-10-19T07:56:59.495691Z [info     ] ::endgroup::                   [airflow.providers.common.ai.tools] loc=__init__.py:132

Only ``AirflowTools`` and ``tools.run(query(...))`` come from Airflow. ``query()``, the
system prompt, the model and ``max_turns`` are the SDK's own.
:meth:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.build_options` builds
``ClaudeAgentOptions`` with the Airflow tools wired in and safe defaults for the fields
that matter most for this adapter; see `Built-in tools run on the worker host`_. You can
also build ``ClaudeAgentOptions`` yourself and add
:attr:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.server` and
:attr:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.allowed_tools` to
it. Put the server in ``mcp_servers`` under the key ``tools.server_name`` (``"airflow"`` by
default). The SDK names a server after its dict key, so ``{"warehouse": tools.server}``
serves ``mcp__warehouse__*`` tools while ``allowed_tools`` lists ``mcp__airflow__*`` and
the names do not match.

What each tool does
--------------------

``AirflowTools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, and individual
:class:`~airflow.providers.common.ai.tools.AirflowTool` objects. ``MCPToolset`` is the
exception; give the SDK the MCP server directly instead. Each tool:

- Keeps the toolset tool's name, description and argument schema, served as an MCP tool
  named ``mcp__<server_name>__<tool name>``.
- Runs through the toolset, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result limits behave as they do in
  :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`. Because the
  built-in tools are off, the model cannot open a result the SDK writes to a file, so each
  tool is marked with 500,000 characters as the size the SDK should keep inline. The bundled
  CLI caps that value at 500,000 (observed on CLI 2.1.286) and counts it in UTF-16 code units,
  so an emoji counts as two. A single result longer than that
  is truncated to fit and ends with a notice asking the model to narrow the request; the
  model still reads it as a normal result, not an error. Without the truncation the CLI would
  spill the result to a file the model cannot read. The CLI also writes each result to stdout
  twice, JSON-escaped, and the SDK fails the stream on any line over its ``max_buffer_size``
  (1 MiB by default), which a result well under 500,000 characters can exceed.
  ``build_options()`` therefore sets ``max_buffer_size`` to 16 MiB.
- Passes its result through Airflow's secret masker before the model sees it. A
  connection password that turns up in a query result reaches the model as ``***``.
- Writes a ``Tool call: <name>`` group to the task log. Calls to a toolset are also
  counted in the ``common_ai.tool_calls`` metric with ``framework=claude_agent_sdk`` (see
  :doc:`../observability`).

When a tool fails
-------------------

A failure the model can correct, such as a query naming a table outside
``allowed_tables`` or one the database rejects, comes back to it as an MCP tool result
with ``isError`` set. This is what the model read when it asked a ``SQLToolset`` to run a
``DELETE`` statement, which the read-only validation rejects. It is the ``tool_result``
block from the ``UserMessage`` in the SDK stream of a real run (indented here;
``tool_use_id`` is the value from that run), so it shows the API-level message the model
reads, not the handler's return value or the MCP wire format:

.. code-block:: json

    {
      "type": "tool_result",
      "tool_use_id": "toolu_01Sitdzj84kaH1CKYC8LFW2b",
      "content": "The query tool failed: Statement type 'Delete' is not allowed. Allowed types: Select, Union, Intersect, Except, Describe, Show\nUse the list_tables and get_schema tools to inspect the database, then fix the query and try again.",
      "is_error": true
    }

``SQLToolset``, ``HookToolset`` and ``ObjectStorageToolset`` allow one correction: a tool
that keeps failing ends the run with
``ToolCallError: query kept failing after 1 correction(s): ...``. The retry budget is
counted per run of
:meth:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.run`, and a model
turn counts as one failure however many of its calls fail, the same way the Anthropic and
Strands adapters count them. The turn is the ``AssistantMessage.message_id``; without one,
calls that run at the same time count as one failure. A call whose arguments fail the SDK's own JSON Schema validation never
reaches Airflow's tools, so it is not counted against the budget and not recorded in the
``common_ai.tool_calls`` metric; see `max_turns`_ below.

Any other failure, such as a hook raising, ends the run. The Claude Agent SDK's own tool
dispatch catches every exception a handler raises and turns it into an error result the
model reads, so
:meth:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.run` has to work around
that to still fail the task: it raises
:class:`~airflow.providers.common.ai.tools.ToolCallError` once the model has read the
(masked) failure, rather than letting the stream continue to a later
``ResultMessage``. Airflow's own retry takes over once the task fails.

A run can also end with a ``ResultMessage`` whose ``is_error`` is set, for example when
``max_turns`` is reached or the API call fails. ``run()`` does not return that result as
a success. Which exception you get depends on the stream:

* ``query()``: the CLI exits non-zero right after an error result, and the SDK raises
  ``claude_agent_sdk.ResultError`` (a ``ProcessError``) from the stream on the next
  iteration, before ``run()`` reaches its own check. ``ResultError`` carries the result's
  ``subtype``, ``terminal_reason`` and the rest of its payload.
* ``ClaudeSDKClient.receive_response()``: the stream ends at the ``ResultMessage``, so
  ``run()`` raises ``RuntimeError`` with the result's ``subtype``, ``result``,
  ``errors``, ``stop_reason``, ``api_error_status`` and ``terminal_reason`` where they are
  set, with long values shortened.

For a streamed prompt, ``run()`` raises ``RuntimeError`` whenever any result message has
``is_error`` set, even if a later turn succeeds.

If a tool failure of the kind above was also recorded, ``ToolCallError`` is raised first
on either stream. ``RuntimeError`` is also raised when the stream ends without any
``ResultMessage``.

.. _max_turns:

Set ``max_turns``
-------------------

Because an argument-validation failure from the SDK itself is invisible to Airflow's
tools, a model that keeps sending malformed arguments is not stopped by the retry
budget above. Always set ``max_turns`` on ``build_options()`` (or on
``ClaudeAgentOptions`` directly) so such a loop still ends.

Drive the loop with ``tools.run``
-----------------------------------

``run()`` takes the stream that ``query()`` or ``ClaudeSDKClient.receive_response()``
returns. ``ClaudeSDKClient.receive_messages()`` is not supported, because it never ends.

The Claude Agent SDK's tool dispatch hands every handler exception to the model as an
error result. If you iterate ``query()`` yourself instead of using
:meth:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.run`,
a hook that raises becomes one more thing for the model to read, and the task can succeed
with an answer written around the failure. Airflow's tools log a warning when a failure
goes to the model this way.

An ``AirflowTools`` instance drives one query at a time: build a new instance per task,
or call
:meth:`~airflow.providers.common.ai.tools.claude_agent_sdk.AirflowTools.run` again
only once the previous call has returned. A tool call that arrives after a run has ended
is refused with an ``is_error`` result until the next run starts, including a handler
called directly between two runs.

Built-in tools run on the worker host
----------------------------------------

``ClaudeAgentOptions()`` on its own loads every settings source on the worker
(``~/.claude``, project settings, a project ``.mcp.json``) and leaves the CLI's built-in
tools on, including a worker-host ``Bash`` -- the bundled CLI runs as a subprocess on the
same worker that runs the task, with its environment variables. ``build_options()`` closes all
of that by default: no built-in tools, no settings sources, and ``strict_mcp_config=True``
so only the MCP servers passed in the options are used: the one ``AirflowTools`` builds
plus any ``mcp_servers`` you pass to ``build_options()``. It also sets
``verbatim_prompts=True``, so the CLI does not expand ``@path`` file mentions in a prompt.
Otherwise anyone who controls the prompt could send any file the worker user can read to the
model, and that content would not pass through the secret masker. It sets
``max_buffer_size`` to 16 MiB (the SDK's default is 1 MiB) so a result near the 500,000
character limit does not break the stream; a value you pass wins.

``verbatim_prompts=True`` has side effects:

- Slash commands in the prompt are not dispatched.
- The CLI's turn-start attachment pass is off.
- The SDK expects CLI 2.1.248 or later; an older CLI only logs a warning.

When you pass ``can_use_tool`` to ``build_options()``:

- The Airflow tools are not added to ``allowed_tools``, because an allowed tool never reaches
  the callback. Your callback decides on every Airflow tool call.
- ``permission_mode`` defaults to ``"default"``; a value you pass wins.
- ``AirflowTools.allowed_tools`` still lists the tool names if you want to allow some by hand.

If you build ``ClaudeAgentOptions`` yourself instead, set those fields the same way.

Tracing
---------

:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing` does not cover
the Claude Agent SDK: it instruments spans created in this Python process, but the agent
loop runs in the bundled CLI's own subprocess. The SDK passes the active span's W3C trace
context to the CLI, but whether the CLI emits its own telemetry from it is not something
this provider verifies or relies on.

Differences from ``AgentOperator``
-------------------------------------

The loop is the SDK's, so what ``AgentOperator`` builds on top of Pydantic AI does not
apply:

- A task retry runs the loop again from the start. Tools that change data must be safe
  to repeat, because there is no step-level replay as with
  ``AgentOperator(durable=True)``.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``.
- A toolset's connection ID is used as written. It is not rendered as a template, so
  pass the value rather than a Jinja expression.

Installation
--------------

Install this provider with its ``claude-agent-sdk`` extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[claude-agent-sdk]"

The extra installs the SDK, which bundles the Claude Code CLI. The adapter needs
``claude-agent-sdk`` 0.2.160 or later, which the extra requires, and is tested with
0.2.168.

.. seealso::

    - :doc:`../toolsets/index`, for what each toolset exposes and its limits.
    - :doc:`index`, for the framework-neutral tool interface ``AirflowTools`` is built on.
    - `Claude Agent SDK overview
      <https://platform.claude.com/docs/en/api/agent-sdk/overview>`__, in Anthropic's
      documentation, for the SDK and the CLI it bundles.
