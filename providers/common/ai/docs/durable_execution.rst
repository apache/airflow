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

Agent tasks can involve multiple LLM calls and tool invocations. If a task
fails mid-run (network error, timeout, transient API failure), a plain retry
re-executes every LLM call and tool call from scratch -- repeating work that
already succeeded and incurring additional cost.

Setting ``durable=True`` caches each LLM response and tool result as it
completes. On retry, completed steps are replayed from the cache and only the
remaining steps run against the live model and tools. The cache is deleted
after successful completion.

Durable execution only helps when the task has retries configured. Without
retries there is nothing to replay.

This page is about making an ``AgentOperator`` retry cheap. Deciding *whether* a task
should retry at all is :doc:`retry_policies`; a retried ``LLMBatchOperator`` re-attaches
to its running batch instead of resubmitting (:ref:`llm-batch-reattach`).

**Configuration**

On **Airflow >= 3.3** the cache is stored in the
:doc:`task state store <apache-airflow:core-concepts/task-state-store>`,
scoped to the task instance. No configuration is required; the store handles
persistence across retries.

By default each cached step is written to the Airflow metadata database. Model
responses and large tool results can be sizable, so for agents with large
payloads configure ``[workers] state_store_backend`` to offload step values to
external storage (e.g. object storage) instead of the metadata database; the
provider then stores only a reference in the database.

On **Airflow < 3.3** the cache is persisted to ObjectStorage and the location
must be set in ``airflow.cfg``. The task raises ``ValueError`` at runtime if
``durable=True`` and the option is missing.

.. code-block:: ini

    [common.ai]
    # Local filesystem -- suitable for development
    durable_cache_path = file:///tmp/airflow_durable_cache

The value is an ObjectStorage URI, so any supported backend works. For
production, use a shared store so retries on a different worker can read the
cache:

.. code-block:: ini

    [common.ai]
    durable_cache_path = s3://my-bucket/airflow/durable-cache

**Operator example**

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_operator_agent_durable]
    :end-before: [END howto_operator_agent_durable]

**Decorator example**

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_decorator_agent_durable]
    :end-before: [END howto_decorator_agent_durable]

**How it works**

1. On first execution, each LLM response and tool result is saved as the agent
   progresses, together with a fingerprint of the request that produced it
   (model, message history, settings, and tools for LLM steps; tool name,
   arguments, and call id for tool steps).
2. If the task fails and Airflow retries it, completed steps are loaded from
   the cache and returned without calling the model or tool. Steps not yet in
   the cache proceed normally.
3. Before a step is replayed, its stored fingerprint is compared against the
   current request. If anything changed between attempts -- the system
   prompt, the model, the toolset, model settings, or the conversation so
   far -- the stale entry is discarded, a warning is logged, and the step
   re-runs live. A divergence also invalidates the steps after it: re-running
   an LLM step produces fresh tool call ids, so tool results recorded under
   the old conversation no longer match. A changed agent costs a re-run; it
   never replays responses that belong to a different conversation.
4. After successful completion, the cached steps are deleted.

Plain JSON arguments and settings are fingerprinted exactly as before. Anything
else is fingerprinted from pydantic's JSON rendering, so ordinary types that are
not JSON -- a ``datetime`` or ``Decimal`` tool argument, a dataclass in
``tool_choice``, the bytes in a ``BinaryContent``, a dict keyed by date --
fingerprint normally. Bytes in tool arguments and settings are rendered as base64,
so binary data that is not valid UTF-8 fingerprints too; bytes inside a pydantic
model follow that model's ``ser_json_bytes`` setting instead, which renders them as
UTF-8 text by default. Because a tool call is fingerprinted from how its arguments
render, a field excluded from serialization and a secret value, which renders
masked, take no part in it.

A step whose request cannot be fingerprinted is not cached, and on retry it runs
live rather than replaying an unverified entry. That happens when pydantic cannot
render a value at all, which includes a set of models and a model used as a dict
key; when two distinct dict keys render alike, such as ``1`` and ``"1"``, because
the fingerprint would then be unable to tell those payloads apart; and when a value
would render through an iterator, since rendering it would consume it. A parameter
annotated ``Iterable[...]`` is the usual iterator: pydantic validates it lazily,
and reading it in order to hash it would consume the input the tool itself has not
read yet. Tool arguments are rendered from copies, so fingerprinting never changes
what the tool receives, and an argument that cannot be copied is not fingerprinted
either. Such a step counts among the steps the end-of-run summary reports as not
cached.

Apart from those, a step that an earlier version could already fingerprint hashes
the same way as before, so its cached entry still matches, with one exception: the
members of a ``set`` are now ordered before hashing, so that a set matches on a
later attempt. A history holding a set whose order was already stable, such as a
set of integers returned by a tool, can therefore re-run from that step once, on
the first retry after upgrading. A list that a serializer produces is hashed as the
serializer produced it, so a set that a serializer turns into a list may not match
on retry.

On the model path a request that cannot be fingerprinted is rarely confined to a
single step. The value at fault is usually in ``model_settings``, which is attached
to every request, in the tool definitions the request carries, or in the message
history, which every later request carries forward, so it usually degrades every
model step from that point on, and the retry re-runs the agent from there at full
cost. For the settings and the tool definitions that point is normally the first
request, though settings given as a function of the run context, or a tool's
``prepare`` function, can bring such a value in later and drop it again; for the
history it is the step where the value entered, for example in a tool return. Each
``could not fingerprint model request`` warning names its step, so the first one
shows where this began.

A tool call is fingerprinted from its name, arguments and call id alone, so none
of those causes reaches it. One that cannot be fingerprinted is reported
as ``could not fingerprint tool call``. It costs that call, and -- if the live
re-run returns something different from the first attempt -- the model steps
after it, because the result becomes part of the message history they
fingerprint, the same cascade step 3 describes for a changed agent.

Replay verification compares the **requests** sent to models and tools, not
the code behind them. Editing a tool's implementation between attempts does
not invalidate an already-cached result for an identical call, and pointing
``llm_conn_id`` at a different endpoint serving the same model name does not
invalidate cached responses -- clear the cache to force a fully fresh run.

When the run ends, successfully or not, an INFO line reports how many steps
were replayed from the cache and how many new steps were cached. Per-step
detail is available at DEBUG level.

The cache is scoped to a single task instance (Dag id, run id, task id, and
map index), so each run replays only its own steps. On Airflow >= 3.3 the cache
lives in the task state store and is removed when the Dag run is cleaned up; on
Airflow < 3.3 it is a JSON file named ``{dag_id}_{task_id}_{run_id}.json`` (with
``_{map_index}`` appended for mapped tasks) under the configured
``durable_cache_path``.

.. note::

    Runs that fail permanently (exhaust all retries) leave their cached steps
    behind. These do not affect future Dag runs (each run is scoped separately).
    On Airflow >= 3.3 they are reclaimed when the Dag run is removed; on Airflow
    < 3.3 the orphaned JSON files consume storage until cleaned up, so add a
    lifecycle policy to the storage backend or remove them periodically.

**Side effects and idempotency**

Durable execution caches **return values**, not side effects. When a step is
replayed, the tool's code does not run -- only the stored return value is
returned. Two things follow from this:

- If a tool completed successfully and its result was cached, the tool will
  **not** run again on retry. Any side effect it produced (writing a file,
  sending a message) already happened during the original run and is not
  repeated.
- If a tool fails *before* its result is cached, it **will** run again on
  retry. A tool that partially completed (e.g. sent an email then raised an
  exception) may produce the side effect a second time.

All built-in toolsets (``SQLToolset`` with ``allow_writes=False``,
``HookToolset`` in read-only mode) are read-only and replay safely. For custom
tools with non-idempotent side effects, design the tool to be idempotent. For
example, check whether the operation already completed before acting, or
use database constraints to prevent duplicate writes.

Tool results must be JSON-serializable to be cached. If a tool returns a
non-serializable value (e.g. ``BinaryContent`` from MCP tools), or a write to
the task state store fails, the step is not cached and runs again on retry
instead of replaying. The step itself still succeeds. Each such step logs a
WARNING naming the tool, and the end-of-run summary lists every tool that was
not cached. A step that re-runs can change what the model sees next, so the
steps after it may re-run too.

See also
--------

- :doc:`operators/agent`: the operator these settings apply to.
- :doc:`retry_policies`: let a model decide whether a failure is worth retrying.
- :doc:`troubleshooting`: the ``durable=True`` combinations the operator rejects.
