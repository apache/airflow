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

.. _sdk-mapped-tasks-vs-iterable-tasks:

Mapped tasks vs iterable tasks
==============================

.. versionadded:: 3.4.0

Airflow provides two complementary ways to process collections of data:

- **Mapped tasks** distribute work **across multiple workers**.
  Each item becomes a separate Task Instance that can run on a different worker,
  giving you horizontal scalability and per-item observability.

- **Iterable Tasks (IT)** improves concurrency **within a single task**.
  All items are processed inside one Task Instance on one worker, eliminating
  scheduling overhead and — when combined with async operators — enabling true
  I/O multiplexing through a shared event loop.

In short: **mapping spreads load across workers; IT speeds up work within one worker.**

While both approaches allow you to apply an operation over a collection,
they differ significantly in execution model, scheduler impact, and observability.
This page explains the trade-offs and when to use each.

Real-World Motivation
---------------------

Consider a workflow that downloads ~17,000 XML files from an SFTP server and loads
them into a data warehouse. Community benchmarks compare a mapped operator with a loop
written by hand inside a single ``@task``, the pattern IT runs for you (none of these rows
uses ``iterate()`` itself):

.. list-table::
   :header-rows: 1

   * - Approach
     - Execution Time
   * - Mapped ``SFTPOperator``
     - 3 h 25 m
   * - Sync ``@task`` with ``SFTPHook`` (sequential loop)
     - 1 h 21 m
   * - Async ``@task`` with ``SFTPHookAsync`` (concurrent loop)
     - 8 m 29 s
   * - Async ``@task`` with ``SFTPHookAsync`` and connection pooling
     - 3 m 32 s

The ~60× improvement comes from eliminating per-item scheduling overhead and
sharing a single event loop for concurrent I/O. This is the kind of workload
IT is built for: many small, I/O-bound operations processed within one task.

Mapped tasks
------------

Task mapping allows you to expand a single task definition into multiple
Task Instances (TIs).

For more details, see :ref:`task mapping <sdk-dynamic-task-mapping>`.

Key characteristics:

- Each item in the iterable creates a separate Task Instance.
- The scheduler is responsible for creating and managing all mapped tasks.
- Tasks can run in parallel across multiple worker slots.
- Fine-grained retry, logging, and observability per item.
- Well suited for workloads where each item should be independently scheduled and tracked.

The following example fetches Pokémon data from a REST API. Each Pokémon becomes
a separate Task Instance, individually scheduled, retried, and visible in the UI:

.. code-block:: python

    from datetime import datetime

    from airflow.providers.http.operators.http import HttpOperator
    from airflow.sdk import DAG, task

    with DAG(dag_id="dtm-http-pokemon-example", start_date=datetime(2026, 1, 1)):
        list_pokemon_task = HttpOperator(
            task_id="list_pokemon",
            http_conn_id="pokeapi",
            method="GET",
            endpoint="api/v2/pokemon?limit=100",
            response_filter=lambda response: [
                pokemon["url"].replace("https://pokeapi.co/", "") for pokemon in response.json()["results"]
            ],
            log_response=False,
        )

        get_pokemon_task = HttpOperator.partial(
            task_id="get_pokemon",
            http_conn_id="pokeapi",
            method="GET",
        ).expand(endpoint=list_pokemon_task.output)

        list_pokemon_task >> get_pokemon_task


With 100 Pokémon the scheduler creates 100 Task Instances, each occupying
a worker slot. This is fine for small lists, but for thousands of items the
scheduler and database overhead becomes significant.

Iterable Tasks (IT)
----------------------------

Iterable Tasks allows you to iterate over an iterable (typically an XCom result)
*within a single Task Instance*, applying an operator multiple times without creating
separate Task Instances.

This means that iteration happens inside the task execution itself rather than at the
scheduler level.

Key characteristics:

- A single Task Instance processes all items in the iterable.
- No task expansion; the scheduler manages only one task.
- Lower scheduler overhead compared to mapping.
- Iterations share the same execution context (e.g., memory, event loop).
- Particularly well suited for async operators and high-throughput workloads.

The same Pokémon fetching problem can be solved with IT. Here, a single Task
Instance processes all Pokémon concurrently using the sync
:class:`~airflow.providers.http.operators.http.HttpOperator`:

.. code-block:: python

    from datetime import datetime

    from airflow.providers.http.operators.http import HttpOperator
    from airflow.sdk import DAG, task

    with DAG(dag_id="it-http-pokemon-example", start_date=datetime(2026, 1, 1)):
        list_pokemon_task = HttpOperator(
            task_id="list_pokemon",
            http_conn_id="pokeapi",
            method="GET",
            endpoint="api/v2/pokemon?limit=100",
            response_filter=lambda response: [
                pokemon["url"].replace("https://pokeapi.co/", "") for pokemon in response.json()["results"]
            ],
            log_response=False,
        )

        get_pokemon_task = HttpOperator.partial(
            task_id="get_pokemon",
            http_conn_id="pokeapi",
            method="GET",
        ).iterate(endpoint=list_pokemon_task.output)

        list_pokemon_task >> get_pokemon_task


The scheduler only manages a single task. With a sync operator, iterations run in
a pool of up to ``task_concurrency`` threads, so blocking I/O such as these HTTP
requests overlaps up to that many at a time. Async operators scale further: a
coroutine waiting on I/O costs far less than a thread, so ``task_concurrency`` can
be set much higher. CPU-bound Python code speeds up with neither, since the
iterations share one process and its GIL.

To **multiplex** many I/O-bound operations on one event loop, use an async task with
:class:`~airflow.providers.http.hooks.http.HttpAsyncHook`:

.. code-block:: python

    from datetime import datetime

    from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook
    from airflow.sdk import dag, task


    @dag(
        dag_id="it-async-http-pokemon-example",
        start_date=datetime(2026, 1, 1),
    )
    def it_async_http_pokemon_example():
        @task
        def list_pokemon() -> list[str]:
            response = HttpHook(
                http_conn_id="pokeapi",
                method="GET",
            ).run(
                endpoint="api/v2/pokemon?limit=100",
            )

            return [pokemon["url"].replace("https://pokeapi.co/", "") for pokemon in response.json()["results"]]

        @task(
            retries=3,
            task_concurrency=2,
            show_return_value_in_logs=False,
        )
        async def get_pokemon(url: str):
            async with HttpAsyncHook(
                http_conn_id="pokeapi",
                method="GET",
            ).session() as session:
                response = await session.run(endpoint=url)
                return await response.json()

        get_pokemon.iterate(
            url=list_pokemon(),
        )


    it_async_http_pokemon_example()


When ``iterate()`` is used with an async task, all iterations share the same
event loop, enabling true multiplexing of I/O-bound operations without any
manual concurrency management by the DAG author. For a handful of items the
difference is negligible, but for hundreds or thousands of items the
concurrent approach is dramatically faster — see the
:ref:`benchmarks above <sdk-mapped-tasks-vs-iterable-tasks>`.

.. note::

    ``multiple_outputs`` is ignored by ``iterate()``. Each iteration's return value is pushed
    whole as ``return_value_<index>`` and the task's own return value is the lazy sequence over
    them, so a ``dict`` return annotation on the task does not fan its keys out into separate
    XComs the way it does with ``expand()``. Every key an iteration pushes or stores carries its
    index the same way: ``ti.xcom_push("foo", v)`` in iteration 2 lands under ``foo_2``, and so
    does ``task_state_store.set("foo", v)``, so iterations never overwrite each other's values.
    Reading them back follows the same rule: ``ti.xcom_pull(key="foo")`` in iteration 2, or with
    its own ``task_ids``, reads ``foo_2``, as ``task_state_store.get("foo")`` does; a pull from
    another task keeps its key. To read another iteration's value, use ``XCom.get_one``
    (``XCom.aget_one`` in an async operator) with the suffixed key, or read every value downstream
    through the task's lazy sequence.
    This holds in the iteration's own thread or coroutine. A ``threading.Thread`` the task starts,
    or ``loop.run_in_executor()``, does not inherit it: ``get_current_context()`` there returns
    the task's own context, whose ``ti`` and ``task_state_store`` add no index, so the iterations'
    keys overwrite each other. Use ``context["ti"]`` passed to the task, or start helpers with
    ``asyncio.to_thread()`` or ``contextvars.copy_context().run()``, which carry the iteration's
    context over.

.. warning::

    Inputs are shared between iterations. All iterations run in one process, so a value handed
    to several of them is the same object in each: every value passed through ``partial()``, and
    with ``iterate(a=..., b=...)`` every element of ``a`` and of ``b``, which the cross product
    combines more than once. With ``expand()`` each task instance runs in its own process and
    gets its own copy. Treat inputs as read-only, or copy what the task changes in place.

.. note::

    When the input of ``iterate()`` is the output of a mapped task, its items are fetched from the
    API server in chunks of ``[core] xcom_sequence_chunk_size`` items (32 by default), one request
    per chunk, with one chunk held in memory at a time.

Why Iterable Tasks?
---------------------------

IT is designed to address limitations of task mapping in specific scenarios:

- **Scheduler scalability**:
  Mapping creates one Task Instance per item, which can put pressure on the scheduler
  for very large datasets. IT avoids this by keeping execution within a single task.

- **Async multiplexing**:
  With Python-native async support in Airflow 3.2, IT allows multiple
  operations to share the same event loop within a single Task Instance.
  This enables efficient multiplexing of I/O-bound workloads.

- **Lower overhead**:
  No need to serialize, schedule, and track thousands of Task Instances.

- **Triggerer and deferrable-operator bottleneck**:
  Deferrable operators delegate async work to triggerers, which store yielded
  events directly in the Airflow metadata database. Unlike workers, triggerers
  cannot leverage a custom XCom backend to offload large payloads. This makes
  triggerers a bottleneck for sustained high-load async execution or workloads
  that return large results. Mapping deferrable operators
  amplifies the problem further. IT sidesteps triggerers entirely — iterations
  execute on workers, which scale more effectively and support custom XCom
  backends.

  A custom XCom backend is not the whole story for IT, though: each item's result
  is also written to its checkpoint in the task state store, so that a retry can
  replay it instead of running the item again. Those checkpoints land in the
  ``task_state_store`` table of the metadata database and stay there for the
  store's retention (``[state_store] default_retention_days``, 30 days unless
  configured) unless a ``[workers] state_store_backend`` is configured, in
  which case the table only holds a reference to the payload. Iterating over
  large results therefore needs both a custom XCom backend and a state store
  backend; with only the first, the payloads move from the XCom table to the
  task state store table.

  For more on deferred vs async trade-offs, see :doc:`deferred-vs-async-operators`.

IT is especially useful for patterns such as:

- API pagination
- Bulk HTTP or database calls
- High-throughput async workloads
- Streaming or lazily-evaluated XCom results

Hooks as Building Blocks
^^^^^^^^^^^^^^^^^^^^^^^^

IT encourages a pattern where DAG authors call **hooks** directly from
``@task``-decorated functions rather than relying on operators. Operators are
wrappers around hooks and sometimes expose only a subset of the hook's
capabilities. By calling hooks directly, users gain full control over
concurrency, error handling, and batching.

For example, instead of using ``HttpOperator`` in deferrable mode (which
delegates to the triggerer for a single request at a time), an async
``@task`` can call :class:`~airflow.providers.http.hooks.http.HttpAsyncHook`
directly to perform many concurrent requests. With IT, the framework
handles the iteration, concurrency, and event-loop management
automatically — the DAG author only writes the per-item logic and decides
which strategy it wants to use.

This "hooks as building blocks" approach is especially powerful with async
hooks, where the shared event loop enables concurrent I/O without any
manual ``asyncio.gather`` or ``asyncio.Semaphore`` management.

For more examples of calling async hooks directly from tasks, see
:doc:`deferred-vs-async-operators`.

Callbacks
---------

With IT the callbacks of the wrapped operator run per item, against the item's own context, but
not all at the same moment:

* ``on_success_callback`` and ``on_skipped_callback`` run once the item's checkpoint is written, so they
  speak for work a retry will not run again; a checkpoint write that fails fires nothing, and the
  attempt that runs the item again reports it then.
* ``on_failure_callback`` and ``on_retry_callback`` of a failed item wait until every item has run,
  because whether the task is retried depends on all of them: an ``AirflowFailException`` in one
  item fails the whole task without a retry. Once the task's fate is known, every failed item gets
  the callback that matches it, one after another: ``on_retry_callback`` when the task is retried,
  ``on_failure_callback`` when it is not.
* A failure that belongs to no item fires no callback: an error while resolving the input, before
  any item exists, or items cancelled because the task's ``execution_timeout`` ran out (the item
  the timeout struck is reported like the other failures), or items pulled into a free slot before
  a kill and never started. The iterated task carries no task-level callbacks of its own.
* Listeners (``on_task_instance_running``, ``on_task_instance_success``,
  ``on_task_instance_failed``) fire once, for the iterated task instance, when the runner reports
  its state; an item is not a task instance and fires none. With ``.expand()`` they fire once per
  mapped task instance.

Comparison
----------

.. list-table::
   :header-rows: 1

   * - Aspect
     - Mapped tasks
     - Iterable Tasks (IT)
   * - Task Instances
     - One per item
     - Single Task Instance
   * - Scheduler load
     - High for large iterables
     - Minimal
   * - Execution model
     - Distributed across workers
     - In-process iteration
   * - Concurrency
     - Parallel tasks
     - Sync or async within one task
   * - Async support
     - Limited (per task)
     - Strong (shared event loop, multiplexing)
   * - Retry behavior
     - Per item
     - Whole task retries, but checkpointed items are skipped
   * - Skipped items
     - A skipped task instance pushes no XCom. Downstream tasks with ``all_success`` are skipped;
       with ``none_failed`` they run over the other items
     - The same: a skipped iteration is left out of the result, downstream tasks with
       ``all_success`` are skipped, and with ``none_failed`` they run over the other items
   * - Items that return ``None``
     - The task instance pushes no XCom and is not counted: a downstream ``.expand()`` over the
       output runs over the values that exist, and positions shift
     - The iteration pushes no XCom but keeps its position: the result reads ``None`` there, and a
       downstream ``.expand()`` over it runs over that ``None``
   * - Asset events
     - One event per mapped task instance that emits to an asset
     - One event per asset for the whole task instance: items emitting to the same asset are
       merged into it, their ``extra`` with the last item to finish winning, partitions and alias
       events accumulated. A per-file ``Metadata`` pattern from ``.expand()`` produces one event here
   * - Empty input
     - The mapped task is skipped, and so are downstream tasks with ``all_success``
     - The same: the task is skipped and pushes no result
   * - Observability
     - Per item in UI
     - Aggregated in a single task
   * - Triggerer dependency
     - Deferrable mapped tasks rely on triggerers
     - No triggerers involved
   * - Deferral / reschedule
     - Supported (each item has its own task instance)
     - Not supported (raises a non-retryable failure)
   * - XCom backend
     - Workers support custom XCom backends
     - Workers support custom XCom backends (triggerers do not); each item's result is also
       checkpointed in the task state store, so large results need a ``state_store_backend`` too
   * - Use case
     - Independent, trackable units of work
     - High-throughput or streaming workloads

The following table illustrates these differences using the Pokémon example from above:

.. list-table::
   :header-rows: 1

   * - Pattern
     - Task Instances
     - Work Per Task
   * - ``get_pokemon.expand(url=urls)``
     - 100
     - 1 Pokémon
   * - ``get_pokemon.iterate(url=urls)``
     - 1
     - 100 Pokémon

When to Use Mapped Tasks
------------------------

Prefer mapped tasks when:

- Each item must be independently tracked in the UI.
- You need fine-grained retries per item.
- Tasks are long-running or resource-intensive.
- Work should be distributed across multiple workers.
- Scheduling decisions should be made per item.
- You need deferrable operators or reschedule-mode sensors — these work natively with mapped tasks,
  since each mapped item has its own task instance to defer or reschedule.

When to Use Iterable Tasks
-----------------------------------

Prefer IT when:

- You are processing large numbers of small items.
- Scheduler overhead becomes a concern.
- You are using async operators and want to leverage a shared event loop.
- Workloads are I/O-bound and benefit from multiplexing.
- Fine-grained observability per item is not required.

When **not** to use IT
-----------------------

Avoid Iterable Tasks when:

- Each item represents a long-running or CPU-bound computation: the iterations share one
  process, so the GIL keeps CPU-bound Python code from running in parallel.
- You require detailed visibility per item in the Airflow UI.
- Work must be distributed across multiple worker nodes.
- Sub-tasks need to defer (deferrable operators) or reschedule (reschedule-mode sensors) — a
  sub-task index has no task instance of its own to defer or reschedule against, so either raises
  a non-retryable failure instead of pausing.
- The operator finds its remote work by the task instance's identity. Every item runs as the one
  iterated task instance, with its ``dag_id``, ``task_id``, ``run_id`` and ``map_index``: a
  ``KubernetesPodOperator`` that reattaches (``durable``, or ``reattach_on_restart`` before it)
  looks a running pod up by those labels and adopts a sibling item's pod, or fails on finding
  several; an ``EcsRunTaskOperator`` with ``reattach=True`` builds its ``startedBy`` from the same
  fields. Where the operator takes labels of its own, one that names the item tells the jobs apart:
  ``labels={"index": "{{ ti.index }}"}`` on the ``KubernetesPodOperator``, as every template of an
  item renders against the item's own ``ti``. Otherwise turn reattachment off, or map the task with
  ``.expand()``.

.. tip::

   IT is a **third execution option** alongside task mapping and
   deferrable operators. It is not intended as a replacement for either.
   Triggerers remain the right choice for long-running polling or waiting tasks
   (e.g., monitoring a remote job or waiting for a Kubernetes pod to complete).

Relationship with Async Operators
----------------------------------

IT complements async operators introduced in Airflow 3.2 and is the natural next step in that
evolution: async operators make a single I/O call non-blocking, while IT applies that same
non-blocking call repeatedly across a dataset within one task.

- Async operators allow concurrent I/O within a single task.
- IT allows you to *apply an operator repeatedly* over a dataset within that same task.

Together, they enable patterns such as:

- Efficient API pagination
- Concurrent request batching
- Streaming data processing

Unlike task mapping, where each mapped task runs in its own execution context,
IT allows all iterations to share the same event loop, enabling true multiplexing.

Because IT executes on workers rather than triggerers, it also benefits from the
full worker environment: custom XCom backends, Edge Worker support, and the
scalability of execution frameworks such as Celery.

For more details on async execution, see :doc:`deferred-vs-async-operators`.

Future Outlook
--------------

As Python's async ecosystem evolves, IT tasks will benefit from improved
introspection and tooling. For example, Python 3.14 introduces new
`asyncio introspection capabilities <https://docs.python.org/3/whatsnew/3.14.html#whatsnew314-asyncio-introspection>`_
that could eventually enable structured progress reporting in the Airflow UI
for IT tasks — providing per-item visibility without the overhead of per-item
task instances.
