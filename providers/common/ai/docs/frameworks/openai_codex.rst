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

.. _howto/frameworks:openai_codex:

OpenAI Codex SDK
=================

If your agent is a loop on the `OpenAI Codex SDK <https://pypi.org/project/openai-codex/>`__,
which runs the ``codex app-server`` binary as a subprocess, you can run it in an Airflow
task as it is and give it the toolsets this provider ships.
:class:`~airflow.providers.common.ai.tools.openai_codex.AirflowTools` serves ``SQLToolset``,
``HookToolset`` and most other toolsets to Codex through an MCP server it starts on
``127.0.0.1`` in the task's event loop. If you have no loop yet, ``AgentOperator`` runs one
for you, with durable replay and human review; see :doc:`index`.

.. note::

    ``AirflowTools`` is experimental: it may change in a minor release of this provider,
    and it has **not been run against a model**. What a test without a model cannot show is
    listed under `Not yet verified`_. Read that section before you rely on the defaults
    below. The framework-neutral tool interface the adapter builds on is stable.
    See :ref:`howto/stability`.

Run a loop in a task
---------------------

This task answers a question about a database. The client's API key comes from an
Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_openai_codex.py
    :language: python
    :start-after: [START example_openai_codex]
    :end-before: [END example_openai_codex]

Only ``AirflowTools`` comes from Airflow: ``serve()`` starts the tool server and yields the
``CodexConfig`` for ``AsyncCodex``, ``build_thread_options()`` builds the keyword arguments of
``thread_start()``, and ``run()`` drives the turn. Login, the model, the instructions and the
prompt are the SDK's own. ``run()`` returns the SDK's ``TurnResult``, built from the turn's
notifications; ``final_response`` is the model's last answer.

The model can call ``list_tables``, ``get_schema``, ``query`` and ``check_query``, the
tools ``SQLToolset`` serves. Each call writes a ``Tool call: <name>`` group to the task log.

Why only ``AsyncCodex``
-------------------------

Codex has no in-process tools and no way to register a Python function, so a tool call
goes from the ``codex app-server`` child process back to the worker over a real HTTP
connection to the MCP server ``serve()`` started. That server runs in the same event loop as
the tools, which is what lets a toolset's async hook run on the loop it was built for and
share state with the run. If you drive the synchronous ``Codex`` client from that loop's
thread, the loop is blocked, the server cannot answer, and the call waits until Codex gives
up on the tool. Use ``AsyncCodex``, and ``AsyncThread`` and ``AsyncTurnHandle`` with it.

What each tool does
--------------------

``AirflowTools`` accepts any toolset that implements
:class:`~airflow.providers.common.ai.tools.ToolProvider`, and individual
:class:`~airflow.providers.common.ai.tools.AirflowTool` objects. ``MCPToolset`` is the
exception; give Codex that MCP server directly instead. Each tool:

- Keeps the toolset tool's name, description and argument schema.
- Runs through the toolset, so connection resolution, SQL validation,
  ``allowed_tables``, ``allowed_methods`` and result limits behave as they do in
  :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`. A result longer than
  100,000 characters is cut to fit and ends with a notice asking the model to narrow the
  request. The limit is this adapter's own choice, and Codex's ``tool_output_token_limit`` is
  set to the same number so that Codex, which counts tokens, is unlikely to cut first; some text
  has more than one token per character. Whether Codex applies that setting to MCP results is
  not verified.
- Passes its result through Airflow's secret masker before the model sees it.
- Writes a ``Tool call: <name>`` group to the task log. Calls to a toolset are also
  counted in the ``common_ai.tool_calls`` metric with ``framework=openai_codex`` (see
  :doc:`../observability`).

The tool server only accepts requests that carry a bearer token. ``127.0.0.1`` is not a
boundary between tasks: other processes on the same worker can connect to it. ``serve()``
generates a new random token for each block, compares it in constant time, answers ``401``
to anything else, and gives it to Codex through the child process's environment, because a
value in ``codex --config`` would be visible in ``ps``.

The token keeps out other OS users and blind probes of the port. It does not separate tasks
that run as the same Unix user as the worker: such a process can read ``/proc/<pid>/environ``
of the ``codex`` child (on Linux) and use the token. Likewise the ``codex`` process itself
holds the worker's full environment, as ``CodexConfig.env`` only adds to it;
``shell_environment_policy.inherit="none"`` protects only the commands Codex starts.

When a tool fails
-------------------

A failure the model can correct, such as a query naming a table outside ``allowed_tables`` or
one the database rejects, comes back to it as an MCP tool result with ``isError`` set.
``SQLToolset``, ``HookToolset`` and ``ObjectStorageToolset`` allow one correction: a tool
that keeps failing ends the run with
``ToolCallError: query kept failing after 1 correction(s): ...``.

Any other failure, such as a hook raising, ends the run:
:meth:`~airflow.providers.common.ai.tools.openai_codex.AirflowTools.run` raises
:class:`~airflow.providers.common.ai.tools.ToolCallError` after the model has read the (masked)
failure, and Airflow's own retry takes over once the task fails. Before it raises, ``run()``
interrupts the turn: closing the stream does not stop Codex, which would otherwise keep
calling tools. If the interrupt itself fails, that is logged and the tool failure is still
raised. A call that arrives after the run has ended, or after a failure, is refused with an
``isError`` "Not run" result and does not reach the toolset.

``run()`` raises ``RuntimeError`` when the turn fails or is interrupted from outside, with
Codex's error message shortened to 500 characters, and when the stream ends without a
``turn/completed`` notification.

The retry budget counts per run. A Codex turn spans the whole agent loop, many model
responses, so the adapter cannot use it as the model turn that the Anthropic and Strands
adapters count one failure per. Calls that run at the same time count as one failure, and
calls one after another count separately, so a model that keeps sending a failing call
exhausts the budget sooner here than it would per model turn. The adapter sets no limit on
how many model responses a turn may take: bound the loop yourself, for example with the
task's ``execution_timeout``.

Tool timeout
-------------

Codex stops waiting for a tool after ``tool_timeout_sec``, which is 60 seconds unless set.
That is too short for many SQL queries, so ``AirflowTools(..., tool_timeout=600)`` sets it to
600 seconds, and you can change it. What happens to the hook when Codex gives up is not
verified: Codex's result is discarded either way, but the call may be cancelled, or a hook that
runs in a thread may run on.

Drive one turn at a time
--------------------------

An ``AirflowTools`` instance serves one ``serve()`` block and drives one turn at a time:
build a new instance per task. Entering ``serve()`` or calling ``run()`` a second time while
the first is still active raises ``RuntimeError``. Leave the ``serve()`` block after the
``AsyncCodex`` that uses it: leaving it stops the server and deletes the temporary directory
Codex works in.

Safe defaults
---------------

Left alone, ``codex app-server`` reads the worker's ``~/.codex`` (``config.toml``,
``AGENTS.md``, skills, MCP servers), finds project settings in the working directory, writes
its state under ``~/.codex`` and inherits every environment variable of the worker. Codex
silently ignores a ``--config`` key it does not know, so a misspelled key does nothing.
``serve()`` and ``build_thread_options()`` set the following. A unit test starts the
``codex app-server`` that ``openai-codex-cli-bin`` ships, with no login and no model, and
reads each setting back from its ``config/read``. That shows Codex knows the key and took the
value; it does not show what the model can do.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Setting
     - Why
   * - ``CODEX_HOME``, ``CODEX_SQLITE_HOME`` and ``HOME`` in the child's environment point at
       empty temporary directories
     - Codex reads no user-level settings from the worker (``~/.codex``, a worker ``CODEX_HOME``)
       and writes nothing to them. The system layer, ``/etc/codex/config.toml``, is still read if
       it exists. The environment
       variables have to be overridden one by one, because the child inherits the worker's:
       a ``CODEX_SQLITE_HOME`` set on the worker sends the state database outside the temporary
       directory unless it is overridden. Whether the ``sqlite_home`` config key alone would do
       is not verified. The temporary directory, about eighty files once Codex has started, is
       deleted when ``serve()`` ends.
   * - ``cwd`` is a further empty temporary directory
     - Codex looks for a project ``.codex/config.toml`` and ``AGENTS.md`` upward from its
       working directory. Pass ``cwd`` yourself and that protection is yours to keep.
   * - ``cli_auth_credentials_store="ephemeral"``
     - A login with ``login_api_key`` is meant to stay in memory instead of an auth file.
       The key is accepted; that no file is written after a login is not verified.
   * - ``features.shell_tool``, ``features.unified_exec``, ``features.view_image``,
       ``features.multi_agent``, ``features.apps``, ``features.plugins`` and
       ``features.image_generation`` are ``false``
     - The model must not get a shell on the worker, read an image file from it or use
       capabilities that have nothing to do with Airflow's tools.
   * - ``web_search="disabled"``
     - Data leaves the worker only through the Airflow tools.
   * - ``shell_environment_policy.inherit="none"``
     - If a command still ran, it would get none of the worker's environment, which holds
       ``AIRFLOW_CONN_*`` values.
   * - ``history.persistence="none"``, ``analytics.enabled=false``, ``feedback.enabled=false``,
       ``check_for_update_on_startup=false`` and ``otel.metrics_exporter="none"``
     - No session history, usage analytics, feedback, update check or metrics.
   * - ``approval_mode=ApprovalMode.deny_all`` and ``sandbox=Sandbox.read_only`` in
       ``build_thread_options()``
     - ``AsyncCodex`` always accepts a request to run a command or change a file, so the only
       way to refuse one is for Codex never to ask. See `Removing Codex's file-editing tool`_.
   * - ``ephemeral=True`` in ``build_thread_options()``
     - Codex accepts it; that it keeps the thread off disk is assumed, not verified.
   * - ``mcp_servers.<name>.required=true``
     - The thread does not start when the tool server is unreachable. Without it Codex would
       run the turn with no tools. Tested: ``thread/start`` fails with "required MCP servers
       failed to initialize" when the URL is wrong. ``mcpServerStatus/list`` does not raise in
       that case; it reports the failure in ``tools_error``.
   * - ``mcp_servers.<name>.supports_parallel_tool_calls=false``
     - Hook calls run one at a time anyway; this keeps Codex from sending them in parallel.
       Codex does not echo a ``false`` value, so a test can show only that the key is known
       (``true`` is echoed), and whether ``false`` is the same as the default is not
       confirmed.
   * - ``mcp_servers.<name>.tool_input_schema_max_bytes``
     - Codex compresses an MCP server's tool schemas past 5,000 bytes and drops parameter
       descriptions. The adapter raises the limit to at least 65,536 bytes, or the size of the
       largest tool schema.

``build_thread_options()`` lets a value you pass win, and raises ``ValueError`` if ``config``
already defines an MCP server with the same name as ``server_name``. ``serve()`` puts nothing
of yours in the environment: add what you need to ``CodexConfig.env`` yourself, or build a
modified copy with ``dataclasses.replace``.

Removing Codex's file-editing tool
-----------------------------------

We found no user-level setting that removes ``apply_patch``, Codex's file-editing tool: the
feature flag that used to control it is gone and which form the model sees is a field of each
model's catalog. Overriding that catalog (``model_catalog_json``) was not investigated. So even
with every default above, the model may still see a tool that writes files. The adapter does not remove it. It makes it harmless: the sandbox is read-only, and Codex is
never allowed to ask for an exception, so a write should be refused without anything to
approve. Codex's own sandbox is what does this. It depends on the host operating system, and
whether it is available in a container, and what Codex does when it is not, is unverified.
``sandbox`` and ``approval_mode`` are defaults that you can override; loosening either
reopens this.

Not yet verified
------------------

Everything above that needs a model has not been run. Someone with an OpenAI API key has to check
the following before this adapter is relied on. Until then, treat the corresponding defaults as
intended, not proven:

1. **Login.** ``login_api_key`` succeeds, and after the run neither the temporary
   ``CODEX_HOME`` nor the worker's ``~/.codex`` holds a new auth file.
2. **Tool round trip.** Run the example Dag. The model calls a ``SQLToolset`` tool through MCP,
   the task log shows the call with ``framework=openai_codex``, and a result containing a
   registered secret reaches the model masked. Also check a result near the 100,000 character
   limit, and that ``deny_all`` does not make Codex refuse the Airflow tool calls; if it does,
   the MCP server's tool approval setting is the place to look.
3. **Built-in tools are gone.** Ask the model to list its tools and to run a shell command.
   The stream must contain no ``commandExecution`` item. If it does, ``features.shell_tool``
   and ``features.unified_exec`` do not remove the shell, and the adapter is not safe to use,
   because a read-only sandbox still lets the model read any file the worker can, such as
   ``airflow.cfg``, without passing the secret masker.
4. **File writes are refused.** Ask the model to create a file in its working directory.
   The write must be refused, no approval may be accepted automatically, and the temporary
   directory must stay empty.
5. **A tool failure ends the turn.** Make a hook raise. The task must fail with
   ``ToolCallError`` and Codex must make no further tool calls.

Other things not checked: whether Codex reads ``@path`` file mentions in a prompt (Claude's
``verbatim_prompts`` guards against this; Codex's behaviour is unknown, so do not pass a prompt
you do not control), the largest message ``codex app-server`` accepts on its stdio connection,
and whether several tasks on one worker, each with its own server and directory, can run
side by side.

Tracing
---------

:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing` does not cover the
OpenAI Codex SDK: the agent loop runs in the ``codex`` subprocess, outside this Python process.

Differences from ``AgentOperator``
-------------------------------------

The loop is Codex's, so what ``AgentOperator`` builds on top of Pydantic AI does not apply:

- A task retry runs the loop again from the start. Tools that change data must be safe
  to repeat, because there is no step-level replay as with
  ``AgentOperator(durable=True)``.
- There is no human review step like ``AgentOperator(enable_hitl_review=True)``, and no
  per-call approval: the adapter has no equivalent of the Claude adapter's ``can_use_tool``.
- A toolset's connection ID is used as written. It is not rendered as a template, so
  pass the value rather than a Jinja expression.

Installation
--------------

Install this provider with its ``openai-codex`` extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[openai-codex]"

The extra installs the SDK, ``openai-codex-cli-bin`` (the ``codex`` binary, 119 to 152 MB
per platform), ``mcp`` 2.0 or later, whose server API the adapter uses, and ``uvicorn``. The
adapter needs ``openai-codex`` 0.163.0 or later and is tested with 0.163.0. The SDK is at an early version and
the ``codex app-server`` protocol it wraps marks many fields unstable; every default above was
observed with 0.163.0 and is not a documented guarantee. Strands Agents pins ``mcp`` below 2.2
while this adapter needs 2.0 or later; installing both together was not tested. The adapter's
tests were run on ``mcp`` 2.2.0 in a local environment; Airflow's lock, and so CI, has 2.3.0.

.. seealso::

    - :doc:`../toolsets/index`, for what each toolset exposes and its limits.
    - :doc:`index`, for the framework-neutral tool interface ``AirflowTools`` is built on.
    - `openai-codex on PyPI <https://pypi.org/project/openai-codex/>`__, for the SDK and the
      ``codex`` binary it installs.
