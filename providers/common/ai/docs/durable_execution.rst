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

An agent task makes several model calls and tool calls. When it fails partway (a
network error, a timeout, a provider outage), a plain Airflow retry runs every one of
them again: the model calls are paid for twice and the tools' side effects happen
twice. With durable execution, each step the agent completes is recorded in a journal
kept for the task instance, and the retry replays the recorded steps instead of running
them again. Only the steps the failed attempt never finished run live.

Durable execution only helps a task that has retries. To decide *whether* a failure is
worth retrying, see :doc:`retry_policies`; a retried ``LLMBatchOperator`` re-attaches to
its running batch instead (:ref:`llm-batch-reattach`). To resume a Strands Agents agent on a
retry, see :ref:`howto/frameworks:strands-durable`; for an OpenAI Agents SDK agent, see
:ref:`howto/frameworks:openai-agents-durable`.

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

.. _durable-toolset-ids:

Toolset ids
-----------

pydantic-ai names the recorded steps of a ``FunctionToolset``, ``MCPToolset`` or
``DynamicToolset`` after the toolset's ``id``. ``AgentOperator`` and ``@task.agent`` fill
in the ids you leave out of a ``FunctionToolset`` and of a function that builds a toolset
for each run passed in ``toolsets`` or ``agent_params["toolsets"]``, and of a
``FunctionToolset`` held directly by a ``Toolset`` capability, so this agent needs none:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_agent_durable_toolsets_without_id]
    :end-before: [END howto_agent_durable_toolsets_without_id]

A tool call of a ``FunctionToolset`` without an id is recorded under the tool's name
alone, ``durable_order_status__function_toolset.call_tool:get_order`` above, so a retry
of the same agent finds the calls the failed attempt recorded. A function that builds a
toolset, such as ``carrier_tools``, is named by its module and qualified name
(``example_agent_durable.carrier_tools``). A toolset you give an ``id`` keeps it in its
step names. Adding a tool to a toolset, reordering the toolsets, or renaming a function
or an id between attempts changes what the model is offered, so the retry runs again
from the first model request (:ref:`durable-replay`). A function that builds a toolset
per run is the exception for its own tools: its tool list is a recorded step, so the
steps a retry replays, and the request that failed, keep the tools the failed attempt
saw, and later steps get the new list. A clear that deletes the journal
(:ref:`durable-clear`) starts over with the new list.

An ``MCPToolset`` from pydantic-ai still needs an id from you. Its calls are named after
the toolset alone, and no id is derived from the server's URL or command, which can carry
credentials. The Dag fails to load with:

.. code-block:: text

    ValueError: durable=True needs a unique id on every MCPToolset, such as MCPToolset(..., id='weather'): durable replay records each tool call under it, and none is derived from the server's URL or command, which can carry credentials. Set it on the toolset itself; the id of a Toolset capability around a toolset does not reach it.

So does a callable that has no ``__qualname__`` to name it by, such as a
``functools.partial``; pass it as ``DynamicToolset(function, id=...)``:

.. code-block:: text

    ValueError: durable=True names the steps of a toolset built per run after the function that builds it, but functools.partial(<function carrier_tools at 0x...>, carrier='dhl') has no __qualname__. Pass it as DynamicToolset(function, id=...).

Two different toolsets with the same id are refused, since their calls would be filed
under one name, and so are two functions with the same qualified name, such as two
lambdas in one Dag file:

.. code-block:: text

    ValueError: durable=True needs a unique id on each toolset, but more than one toolset has the id 'orders_dag_lambdas.<lambda>': durable replay records each tool call under its toolset's id, so a retry could not tell their calls apart.

Passing one toolset object in two places is fine. The provider's own toolsets
(``SQLToolset``, ``HookToolset``, its ``MCPToolset`` and the rest) and tools passed as
``agent_params={"tools": [...]}`` need nothing. ``AirflowDurability`` in a plain ``@task``
does not fill in ids: there pydantic-ai raises ``UserError`` for a ``FunctionToolset``,
``MCPToolset`` or ``DynamicToolset`` without one. A capability that contributes
``@durable_operation`` methods needs an ``id`` for the same reason, and pydantic-ai raises
``UserError`` naming it when it has none. pydantic-ai-harness ``SpendLimits`` has one by
default.

Where the journal is kept
-------------------------

On **Airflow 3.3 and later** the journal is kept in the
:doc:`task state store <apache-airflow:core-concepts/task-state-store>`, scoped to the
task instance. No configuration is needed. Each step is written to the Airflow metadata
database; for agents with large model responses or tool results, set
``[workers] state_store_backend`` to offload the values to external storage instead.
Without it, a step larger than ``[state_store] max_value_storage_bytes`` (64 KB by
default) is still written to the database, and the task log warns that it exceeds the
limit.

When the agent run succeeds, its journal is deleted; with ``enable_hitl_review``, once the
reviewer approves (:ref:`hitl-review-durable`). On 3.3 and later, every step the
agent completes is one write to the task state store, a call the task makes to the API
server while the agent waits. A first attempt reads back only the list of agent runs the
task started (and, in ``AgentOperator``, the run id), since no earlier attempt recorded
anything; a retry also looks up each step of a run an earlier attempt started, and stops
looking once the run takes a different path from that attempt. Deleting the journal is
one call per entry; before 3.3 it rewrites the journal file instead, or removes it once
nothing is left in it.

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

    ValueError: Durable execution on Airflow before 3.3 needs [common.ai] durable_cache_path to be set, to say where its journal is kept. Example: durable_cache_path = file:///tmp/airflow_durable_cache

.. _durable-replay:

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

A run can also fail on the model's answers rather than in a step: pydantic-ai raises
``UnexpectedModelBehavior`` when the model keeps giving answers a tool rejects with
``ModelRetry``, or output that fails validation, past its retry limit. Those answers are
recorded, so replaying them would only reach the same error. The retry instead replays
the steps before the model's last request and asks the model again from there. The
earlier rejected answers still count against the retry limit, so each Airflow retry gives
the model one more answer, with the rejections it already got in its history. Tool calls
the last answer made run again if the model asks for them again, including calls that
succeeded alongside the one that was rejected. The task log names the step:

.. code-block:: text

    Durable: the run took a different path from the previous attempt; this step and every step after it run again position=2 step=analyst__model.request reason=the model's answers from this step on failed the previous attempt, so it is asked again

A tool call that fails the run on two attempts in a row, with an exception other than
``ModelRetry``, ``ToolFailed``, ``ApprovalRequired`` or ``CallDeferred``, is also handled
by asking the model again. The first retry replays the model's answer and runs the call
again with the same arguments, in case the failure was transient, such as a dropped
connection. If the call fails the run on that retry too, the arguments are taken to be the
problem: the next retry replays the steps before the model request that chose the call,
and asks the model again from there. That takes ``retries=2`` or more. A call from the new
answer gets its own retry with the same arguments before the model is asked again, and
other calls of the old answer run again if the new answer repeats them. When the failing
call belongs to an agent that a tool runs, only that agent's model is asked again. The
attempt that failed the second time logs:

.. code-block:: text

    Durable: the same tool call failed on consecutive attempts; a retry asks the model again from the request that chose it position=1 step=slogan_agent__function_toolset__marketing.call_tool:publish_slogan attempts=2

The model is asked with the conversation it had before, which does not include the
exception, so it can choose the same arguments again. To let the model see what went
wrong and correct the call within the same attempt, raise ``ModelRetry`` from the tool
instead.

Some steps have no request to fingerprint: another capability's
``@durable_operation``, or tool discovery. Such a step replays on its name, and only when
the step before it also replayed. A step that settles tool calls waiting for approval is
fingerprinted by those calls, so it replays only for them.

The journal records:

- model requests, including streamed ones;
- calls to tools from any toolset, including the provider's own toolsets, MCP servers,
  toolsets built per run, and toolsets supplied by capabilities, except the tools you
  :ref:`set to run on every attempt <durable-tool-opt-out>`;
- tool discovery for MCP and per-run toolsets;
- ``@durable_operation`` methods of other capabilities, such as the spend accounting of
  pydantic-ai-harness ``SpendLimits`` and the summarization of ``SummarizingCompaction``,
  so a retry neither charges the spend twice nor pays for a summary again, unless the
  summary follows a tool set to run on every attempt.

A tool that raises ``ModelRetry``, ``ToolFailed``, ``ApprovalRequired`` or
``CallDeferred`` replays as the same signal. With ``AgentOperator`` on Airflow 3.3 and
later, a tool that needs approval pauses the task (except with code mode or
``enable_hitl_review``), and the reviewer's answer is recorded as a step, so a retry does
not ask again, and replays an approved call that returned (:ref:`tool-approval-durable`
has the exceptions). A deferred tool (``CallDeferred``) fails the task with
``UnsupportedToolDeferralError``, as it would without ``durable``. A tool that raises
any other exception runs again on retry; once it has failed the run on two attempts in a
row, the next retry asks the model again, as described above. ``SQLToolset`` uses that
split: a statement the database rejects is a ``ModelRetry`` and replays, while failing to
reach the database raises, so the retry queries it again rather than handing the model the
old failure.

With ``enable_hitl_review``, the first output and each regeneration after feedback are
runs in the same journal, and on Airflow 3.3 and later a retry applies the feedback the
reviewer already gave instead of asking again; a review that ends in a reject starts over
with a new answer from the model (:ref:`hitl-review-durable`).

A task can run more than one agent, and an agent's tool can run another agent; each
agent run is replayed on its own, and one a tool started is replayed under that tool
call. The journal is deleted when the task succeeds, including any steps an earlier
attempt recorded beyond where the last one went.
In a plain ``@task``, which has no such moment, the capability deletes a run's journal
as soon as that run succeeds: after a failure, only the run that failed replays, and
the agents that ran before it run again.

Values that are not plain JSON
------------------------------

Plain JSON tool arguments and settings are hashed as they are. Anything else is
fingerprinted from pydantic's JSON rendering, so ordinary types that are not JSON, such
as a ``datetime`` or ``Decimal`` tool argument, a dataclass in ``tool_choice``, the
bytes in a ``BinaryContent`` or a dict keyed by date, fingerprint normally. Bytes in
tool arguments and settings are rendered as base64, so binary data that is not valid
UTF-8 fingerprints too; bytes inside a pydantic model follow that model's
``ser_json_bytes`` setting instead, which renders them as UTF-8 text by default. Because
a tool call is fingerprinted from how its arguments render, a field excluded from
serialization and a secret value, which renders masked, take no part in it.

The members of a ``set`` are ordered before hashing. A set of strings iterates in an
order that changes from one process to the next, and every attempt is a new process, so
without this a step holding one would never match on retry. A list that a serializer
produces is hashed as the serializer produced it, so a set that a serializer turns into
a list may not match on retry. The one list that is re-sorted is the output of a
JSON-only serializer (``when_used="json"``) that returns the members of a set: it cannot
be told from the set itself, so ``["z-first", "a"]`` hashes as ``["a", "z-first"]``.

A request cannot be fingerprinted when:

- two distinct dict keys render alike, such as ``1`` and ``"1"``, since the fingerprint
  could then not tell those payloads apart;
- a set whose order changes between processes cannot be found in the rendering, because
  its members have different shapes, or because a JSON-only serializer renamed or
  reshaped it, even one that also sorts it;
- a value would render through an iterator, since rendering would consume it. A
  parameter annotated ``Iterable[...]`` is the usual case: pydantic validates it lazily,
  and reading it to hash it would consume input the tool has not read yet. Tool
  arguments are rendered from copies, so fingerprinting never changes what the tool
  receives, and an argument that cannot be copied is not fingerprinted either;
- pydantic cannot render a value at all.

Such a step is handled like a step with no request to fingerprint: it replays on its
name, and only when the step before it replayed. The task log names it with a
``could not fingerprint model request`` or ``could not fingerprint tool call`` warning.
On the model path the cause is rarely confined to one step. ``model_settings`` and the
tool definitions travel with every request, and a value that enters the message
history, for example in a tool return, is carried into every later request, so every
model step from that point on is replayed without verification.

A set of models or dataclass instances, and from pydantic 2.13 a dict keyed by a model
or dataclass instance, is a special case. In a tool argument or a setting it cannot be
fingerprinted. In the message history it is rendered, and the history is then hashed
from its JSON rendering alone: nothing in it is ordered and nothing is refused, so a set
of strings elsewhere in that history can make the run diverge at that step on retry.

.. _durable-code-mode:

Code mode
---------

With pydantic-ai-harness's ``CodeMode`` (:doc:`code_mode`), the model sends Python to one
``run_code`` tool, and the snippet calls the agent's tools as functions. ``run_code`` is
not a step of its own: the retry runs each snippet again, and the tool calls the snippet
makes replay from the journal one by one, so a snippet that called three tools is three
recorded steps. When the snippet returns what it returned before, the model request after
it replays too.

A call the snippet makes replays only when the call at that position in the journal has
the same tool, arguments and call id. Code mode numbers a snippet's calls in the order the
snippet makes them, so when calls under ``asyncio.gather`` reach the journal in another
order on the retry, the first call out of place and every step after it run live. A call
never gets the result recorded for another one.

The snippet's own code is not recorded. Whatever it does outside a tool call happens again
on the retry, such as writing to a directory shared through ``mount`` or calling an
``os_access`` handler, and if what it reads that way has changed, the tool calls it makes
next differ and run live. Keep side effects in tools, whose results are recorded.

``CodeMode``'s ``eager`` and ``speculate`` options start tool calls while the model is
still writing the snippet. Both are off while the run is durable, so a retry runs the
finished snippet the way the previous attempt did.

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
deleted with it, so a rerun after a clear that deleted the journal (see
:ref:`durable-clear`) gets a new one.

Usage and limits
----------------

A replayed step adds nothing to the usage the run counts against ``usage_limits`` or
reports in the ``usage`` XCom: not its request, tokens, cost or tool calls, nor the
usage a replayed ``@durable_operation`` brings back. Each attempt counts only the calls
it actually makes. Clearing a finished task instance starts a fresh budget; if the
rerun replays from the journal (see :ref:`durable-clear`), the replayed steps cost nothing.

.. _durable-clear:

Clearing a task
---------------

On Airflow 3.4 and later, clearing a task instance (its Clear dialog in the UI, or
``POST /api/v2/dags/{dag_id}/clearTaskInstances``) deletes its task state, the journal
included, so the rerun starts the agent over. This holds for the default task state store
backend; a custom ``[state_store] backend`` keeps its entries. To resume from the journal
instead, tick **Keep task state and resume** in the dialog, or send
``"keep_task_state": true`` in the request. The **Keep task state on clear** UI setting
ticks that box by default. Clearing the whole Dag run from the Clear Run dialog keeps task
state; "Retries resume, clearing starts over" in
:ref:`apache-airflow:concepts-resumable-tasks` lists the other ways of clearing and which
of them keep it.

Before Airflow 3.4 no clear deletes the journal (the task state store on 3.3, the file
under ``durable_cache_path`` earlier), so a cleared task replays.

A task that ran to success has no journal left, so clearing it starts over either way. For
``AgentOperator`` and ``@task.agent``, the rerun's ``Durable:`` line in the task log
counts the steps it replayed; a count above zero means it resumed from the journal.

Side effects and idempotency
----------------------------

Durable execution records what a tool returned. A replayed tool's code does not run, so a
side effect it had (a file written, a message sent) is not repeated.
A tool that fails *before* its result is recorded runs again on retry, so a tool that
sent an email and then raised sends it again. The same holds when the worker dies
between a tool's side effect and the write of its result, and for the tool calls of a
model answer that the retry asks for again, after pydantic-ai gave up on the model's
answers (``UnexpectedModelBehavior``) or a tool call failed the run on two attempts in a
row. Make tools with side effects
idempotent: check whether the work already happened, or let a database constraint
reject the duplicate.

Tool results are masked by Airflow's secret masker before they are recorded, so a
connection password in a result reaches the journal as ``***``.

A result has to survive a JSON round trip to be recorded. Bytes a tool returns, on their
own or as values anywhere inside dicts and lists, are recorded as base64, and a retry
replays the same bytes. Only ``bytes`` is converted. ``bytearray``, ``memoryview``, a
``set`` or ``frozenset`` of bytes, and bytes in a field of a dataclass or Pydantic model
are not:
depending on the pydantic-ai version and the toolset, such a result fails the task, or
it comes back changed (as text, a list or a dict), which can make the steps after that
tool call run again. Put bytes in a dict instead. A result the journal cannot encode,
such as an object with no JSON form, fails the task with ``DurableJournalError`` and no
retry, since it would fail the same way every time. A write the storage refuses or
fails does not fail the task: the task log names the step, and a retry runs it again.

.. _durable-tool-opt-out:

Run a tool on every attempt
---------------------------

Some tools must run again on a retry rather than replay: one whose answer has to be
current, such as today's date or the state of a job, and one whose effect does not outlive
the attempt, such as a file in a temporary directory. Give such a tool the metadata
``{"airflow": False}``. For a tool in a toolset you do not build yourself, such as
``SQLToolset``, select it by name with pydantic-ai's ``SetToolMetadata`` capability:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_agent_durable_tool_opt_out]
    :end-before: [END howto_agent_durable_tool_opt_out]

On a retry, ``today`` and every ``query`` call run again, and the model requests and the
other tool calls still replay. A live call that returns what it returned on the last
attempt leaves the model requests and tool calls after it replaying. One that returns
something new changes the conversation the model sees, so the model requests after it,
and the tool calls they make, run again. A capability that writes into the conversation
right after the live call (a compaction summary, say) runs again too: its
``@durable_operation`` has no request to compare, so it replays only after a step that
replayed. The summary it writes can then make the steps after it run again.

You can turn the opt-out on or off between attempts. The call keeps its place in the
journal and the setting is left out when a model request is compared, so later steps still
replay. Failures still count, too. Two in a row with the same arguments, and the next retry
asks the model again.

Where the setting goes depends on the toolset:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Toolset
     - How to opt a tool out
   * - ``FunctionToolset``, tools passed to the agent
     - ``metadata={"airflow": False}`` on the tool, or ``SetToolMetadata``.
   * - ``SQLToolset``, ``HookToolset``, the provider's ``MCPToolset``, and any other toolset
     - ``SetToolMetadata``. On a toolset built on ``AirflowToolset``, setting
       ``replayable = False`` opts all its tools out; ``ManagedAgentToolset`` does by
       default, because a managed agent acts on a system Airflow cannot observe.
   * - ``DynamicToolset``
     - ``SetToolMetadata``. Its tool list replays from the journal, so metadata set inside
       the toolset can keep the value an earlier attempt saw.
   * - ``pydantic_ai.mcp.MCPToolset``
     - Not supported; pydantic-ai raises the error below when an opted-out tool is called.

The error from pydantic-ai's ``MCPToolset``:

.. code-block:: text

    pydantic_ai.exceptions.UserError: Airflow durable config for MCP tool 'get_forecast' has been explicitly set to `False` (durable execution disabled), but MCP tools perform I/O and cannot run outside a durable step. Remove the metadata so the call stays durable.

Any value other than ``True``, ``False`` or ``None`` fails the task before the model is first
offered the tool, and the task is not retried:

.. code-block:: text

    airflow.providers.common.ai.exceptions.DurableToolMetadataError: Tool 'today' has invalid 'airflow' metadata: expected True or False, got str.

Limitations
-----------

- ``durable=True`` cannot be combined with a ``SandboxToolset``. ``AgentOperator`` raises
  ``ValueError`` when the Dag is parsed.
- A task instance that is retried across an upgrade of this provider from a version
  without ``AirflowDurability`` runs its steps live once: the journal keys changed.

See also
--------

- :doc:`operators/agent`: the operator these settings apply to.
- :doc:`retry_policies`: let a model decide whether a failure is worth retrying.
- :doc:`troubleshooting`: the ``durable=True`` combinations the operator rejects.
