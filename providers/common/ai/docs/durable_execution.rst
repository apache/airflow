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

Replay verification compares the **requests** sent to models and tools, not
the code behind them. Editing a tool's implementation between attempts does
not invalidate an already-cached result for an identical call, and pointing
``llm_conn_id`` at a different endpoint serving the same model name does not
invalidate cached responses -- clear the cache to force a fully fresh run.

After the run, a single INFO summary line reports how many steps were
replayed vs executed fresh. Per-step detail is available at DEBUG level.

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
non-serializable value (e.g. ``BinaryContent`` from MCP tools), that step is
skipped with a warning and will re-execute on retry instead of replaying from
cache. The task itself still succeeds.

See also
--------

- :doc:`operators/agent`: the operator these settings apply to.
- :doc:`retry_policies`: let a model decide whether a failure is worth retrying.
- :doc:`troubleshooting`: the ``durable=True`` combinations the operator rejects.
