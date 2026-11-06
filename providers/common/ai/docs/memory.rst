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

.. _howto/memory:

Agent memory
============

Memory lets an agent keep notes that outlive the Dag run. A triage agent that runs
every morning can record a cause it diagnosed today and recognize it tomorrow; a
support agent can remember a customer's setup between tickets. The agent decides
what to write, and the next run reads it back.

This provider does not ship a memory layer of its own. Agents run on Pydantic AI, so
they use `Pydantic AI's Memory capability <https://pydantic.dev/docs/ai/harness/memory/>`__
from ``pydantic-ai-harness``, passed in ``capabilities=`` like any other capability.
Strands and ADK agents use their framework's own memory; see
:ref:`howto/memory:other-frameworks`.

Memory is not the right tool when you need the conversation itself:

- To continue a conversation turn by turn, pass the transcript with
  :doc:`message_history`.
- To avoid repeating model calls when a task retries, use :doc:`durable_execution`.
  It replays one task instance's steps and deletes its cache when the task succeeds.

Install
-------

``Memory`` is in ``pydantic-ai-harness`` 0.24.0 and later, the version the
``code-mode`` extra already requires:

.. code-block:: bash

    pip install "pydantic-ai-harness>=0.24.0"

``pydantic-ai-harness`` is pre-1.0, and its API can change between minor releases.

How it works
------------

``Memory`` gives the agent a notebook of Markdown files. At the start of each model
request it adds a bounded excerpt of the main file, ``MEMORY.md``, to the prompt, and it
gives the model four tools: ``write_memory``, ``read_memory``, ``search_memory`` and
``delete_memory``. The notebook is stored in the store you pass to ``Memory``.

Pick the store by where your tasks run. Each task runs in its own process, and with the
Celery or Kubernetes executor the next run can be on a different machine, so a store on
the worker's local disk is not there for the next run:

.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Store
      - Use it when
    * - ``SqliteMemoryStore(database=...)``
      - Every run of the task executes on the same machine, such as a single-machine
        deployment. The file's directory must exist.
    * - ``PostgresMemoryStore(pool)``
      - Tasks run on several machines, or several tasks write the same memory at once.
    * - ``InMemoryStore()``
      - Tests. It is lost when the task's process exits.

Remember across Dag runs
------------------------

This Dag runs every day and gives a triage agent the failed tasks an upstream task collects. The
system prompt tells the agent to check its notes for known causes and to record new
ones. The notebook lives in a SQLite file, so tomorrow's run sees what today's run
wrote:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_memory.py
    :language: python
    :start-after: [START howto_agent_memory_sqlite]
    :end-before: [END howto_agent_memory_sqlite]

On the next run, ``Memory`` adds what earlier runs wrote to the model's request, next
to the prompt:

.. code-block:: text

    <memory>
    ### MEMORY.md

    sync_crm hits HTTP 429 at 02:00 (first seen 2026-10-13)
    </memory>

One memory per customer
-----------------------

When one agent serves many customers, give each one its own notes with a namespace.
``namespace`` is resolved by your code from the run's dependencies, not by the model, so
the agent cannot read or write another customer's notes through its tools.

``PostgresMemoryStore`` takes an ``asyncpg`` pool, which you create with ``await`` and
close when the run is done. A Dag file cannot do either around an ``AgentOperator`` run,
so this example runs its own Pydantic AI agent in a ``@task``, and creates the pool,
runs the agent and closes the pool in one event loop. The database credentials come from an Airflow connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_memory.py
    :language: python
    :start-after: [START howto_agent_memory_postgres]
    :end-before: [END howto_agent_memory_postgres]

The mapped task runs once per customer, and each run reads and writes only its
customer's notes. ``asyncpg`` is installed with the Postgres provider. See
:doc:`frameworks/pydantic_ai` for what a task running its own agent gets from this
provider and what it does not.

What to watch for
-----------------

- Memory is written by the model, and it goes back into later prompts. A prompt
  injection that reaches the agent once can persist in its notes. Set
  ``inject_memory=False`` when less-trusted input reaches the agent, so the model reads
  memory only when it calls a tool.
- Do not put secrets in prompts to an agent with memory: anything the model sees, it can
  write to the store.
- Namespaces separate customers in the agent's tools. They are not access control on
  the database: anyone with the connection's credentials can read every namespace.
- The stores have no expiry. What is injected is bounded: the guidance, ``MEMORY.md``
  and the list of other files share ``max_tokens`` (about 2,000 tokens by default), and
  ``MEMORY.md`` is cut at ``max_lines`` (200). The model reaches the rest with
  ``read_memory`` or ``search_memory``. Prune notes from the store when they stop being
  useful.

For every ``Memory`` parameter, the injection budget and the search limits, see the
`Pydantic AI Memory documentation <https://pydantic.dev/docs/ai/harness/memory/>`__.

.. _howto/memory:other-frameworks:

Strands and ADK
---------------

Strands and ADK agents run in your own ``@task`` (see :doc:`frameworks/index`), so they
use the memory their framework ships. The same rule about where the store lives applies.

A Strands agent takes a ``MemoryManager`` with one or more stores in its
``memory_manager`` argument. Strands ships ``FileMemoryStore``, which writes under
``./.strands/memory/<store-name>/`` on the worker's disk by default, and
``BedrockKnowledgeBaseStore``, which keeps memory in an Amazon Bedrock knowledge base.
See the `Strands memory module <https://github.com/strands-agents/harness-sdk/tree/main/strands-py/src/strands/memory>`__.

An ADK agent gets memory from the ``Runner``: pass a memory service as
``memory_service``, add sessions to it with ``add_session_to_memory``, and give the
agent the ``load_memory`` or ``preload_memory`` tool. ``InMemoryMemoryService`` is lost
when the task's process exits; ``VertexAiMemoryBankService`` keeps memory in Vertex AI.
See the `ADK memory documentation <https://adk.dev/sessions/memory/>`__.
