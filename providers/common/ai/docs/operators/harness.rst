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

.. _howto/operator:harness:

Native agent harnesses
=======================

``AgentOperator`` builds and drives a Pydantic AI agent itself.
:class:`~airflow.providers.common.ai.operators.harness.HarnessOperator` is for the other
direction: a vendor's own agent loop ("harness") runs in the task, and Airflow gives it
tools, credentials and collects the result. The first (and, this version, only) backend is
the `Claude Agent SDK <https://platform.claude.com/docs/en/api/agent-sdk/overview>`__, which
bundles the Claude Code CLI and runs it as a subprocess.

.. note::

    Experimental: this operator and the harness backend interface under it can change or
    be removed in a minor release of this provider. See :ref:`howto/stability`.

A task answering a question about a database, with the model restricted to the tools a
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` gives it:

.. code-block:: python

    from airflow.providers.common.ai.operators.harness import HarnessOperator
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    warehouse = SQLToolset("warehouse", allowed_tables=["orders"])

    answer = HarnessOperator(
        task_id="ask",
        prompt="How many orders shipped last week?",
        llm_conn_id="anthropic_default",
        toolsets=[warehouse],
    )

Connection
----------

``llm_conn_id`` must point at a ``pydanticai`` connection whose ``password`` is an
Anthropic API key. ``model_id``, or the connection's ``Model`` extra field when
``model_id`` is unset, names the model; an ``anthropic:`` prefix is stripped, and both
unset is an error rather than falling back to the CLI's own default, so cost is always
predictable. **Bedrock, Vertex, Azure and a connection with a custom host are not
supported** -- the harness refuses them rather than silently sending the request
somewhere other than what the connection says.

Built-in tools are off by default
----------------------------------

The backend starts every run with ``tools=[]``, ``allowed_tools=["mcp__airflow__*"]`` and
``setting_sources=[]``: no built-in tool (including the shell), only the Airflow tools in
``toolsets=`` through an in-process MCP bridge, and no ``~/.claude`` or project settings
file read. This version gives no way to change any of that. Three reasons:

- Claude Code's built-in shell and file tools run on the **worker host**, which is not a
  sandbox -- a model with ``Bash`` could read or change anything the worker process can.
- The bundled CLI subprocess inherits the worker's own environment variables; with no
  shell tool it cannot read them, but turning one on would expose them.
- Not reading settings files keeps the run's behavior a function of the Dag and the
  request alone, not of whatever a particular worker's ``~/.claude`` happens to contain.

XCom
----

``return_value`` is the agent's final text, unmasked (as ``AgentOperator`` returns its
output). ``session_id`` and ``usage`` (``num_turns``, ``cost_usd``, the vendor's own
``subtype``, and its raw ``usage`` accounting) are also pushed, and pushed *before* a
failed run raises, so a downstream task or failure callback can read what a failed
attempt spent.

Failures
--------

A run the vendor itself reports as failed raises
:class:`~airflow.providers.common.ai.harness.base.HarnessRunError`, carrying the vendor's
``subtype``. A tool call failure the model cannot correct
(:class:`~airflow.providers.common.ai.tools.ToolCallError`) ends the run the same way an
unhandled tool error ends an ``AgentOperator`` run. Any other SDK exception (connection,
process, CLI) propagates unchanged, so Airflow's own retry handles it.

Not supported this version
---------------------------

Durable execution (session resume across task retries), human-in-the-loop review, tool
approval gates, sandbox integration, a usage budget, structured output, and an
``options=`` escape hatch into the backend's own SDK options. A task retry starts a new
run from scratch -- any tool that changes data must be safe to run again. There is also
no ``@task.harness`` decorator and no example Dag yet.

Installation
------------

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[claude-agent-sdk]"

The wheel bundles the Claude Code CLI; no separate Node.js install is required.
