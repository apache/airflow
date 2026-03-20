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

.. _sdk-dynamic-task-mapping-vs-iteration:

Dynamic Task Mapping vs Dynamic Task Iteration
================================================

.. versionadded:: 3.2.0

Airflow provides two complementary ways to process collections of data:

- **Dynamic Task Mapping (DTM)** distributes work **across multiple workers**.
  Each item becomes a separate Task Instance that can run on a different worker,
  giving you horizontal scalability and per-item observability.

- **Dynamic Task Iteration (DTI)** improves concurrency **within a single task**.
  All items are processed inside one Task Instance on one worker, eliminating
  scheduling overhead and — when combined with async operators — enabling true
  I/O multiplexing through a shared event loop.

In short: **DTM spreads load across workers; DTI speeds up work within one worker.**

While both approaches allow you to apply an operation over a collection,
they differ significantly in execution model, scheduler impact, and observability.
This page explains the trade-offs and when to use each.

Real-World Motivation
---------------------

Consider a workflow that downloads ~17,000 XML files from an SFTP server and loads
them into a data warehouse. Community benchmarks demonstrate the dramatic performance
difference between the two approaches:

.. list-table::
   :header-rows: 1

   * - Approach
     - Execution Time
   * - Dynamic Task Mapping with mapped ``SFTPOperator``
     - 3 h 25 m
   * - Sync ``@task`` with ``SFTPHook`` (sequential loop)
     - 1 h 21 m
   * - Async ``@task`` with ``SFTPHookAsync`` (concurrent loop)
     - 8 m 29 s
   * - Async ``@task`` with ``SFTPHookAsync`` and connection pooling
     - 3 m 32 s

The ~60× improvement stems from eliminating per-item scheduling overhead and
sharing a single event loop for concurrent I/O. This is the kind of workload
where DTI excels: many small, I/O-bound operations processed within one task.

Dynamic Task Mapping (DTM)
--------------------------

Dynamic Task Mapping allows you to expand a single task definition into multiple
Task Instances (TIs).

For more details, see :ref:`dynamic task mapping <sdk-dynamic-task-mapping>`.

Key characteristics:

- Each item in the iterable creates a separate Task Instance.
- The scheduler is responsible for creating and managing all mapped tasks.
- Tasks can run in parallel across multiple worker slots.
- Fine-grained retry, logging, and observability per item.
- Well suited for workloads where each item should be independently scheduled and tracked.

The following example fetches user data from a REST API. Each user ID becomes
a separate Task Instance, individually scheduled, retried, and visible in the UI:

.. code-block:: python

   from datetime import datetime

   from airflow.providers.http.operators.http import HttpOperator
   from airflow.sdk import DAG, task


   @task
   def list_user_ids():
       return [1, 2, 3, 4, 5]


   with DAG(dag_id="dtm-http-example", start_date=datetime(2022, 1, 1)) as dag:
       HttpOperator(
           task_id="fetch_user",
           http_conn_id="api_default",
           method="GET",
           endpoint="/users/{{ item }}",
       ).expand(item=list_user_ids())

With five user IDs the scheduler creates five Task Instances, each occupying
a worker slot. This is fine for small lists, but for thousands of items the
scheduler and database overhead becomes significant.

Dynamic Task Iteration (DTI)
----------------------------

Dynamic Task Iteration allows you to iterate over an iterable (typically an XCom result)
*within a single Task Instance*, applying an operator multiple times without creating
separate Task Instances.

This means that iteration happens inside the task execution itself rather than at the
scheduler level.

Key characteristics:

- A single Task Instance processes all items in the iterable.
- No task expansion; the scheduler manages only one task.
- Lower scheduler overhead compared to DTM.
- Iterations share the same execution context (e.g., memory, event loop).
- Particularly well suited for async operators and high-throughput workloads.

The same user-fetching problem can be solved with DTI. Here, a single Task
Instance processes all user IDs sequentially using the sync
:class:`~airflow.providers.http.hooks.http.HttpHook`:

.. code-block:: python

   from datetime import datetime

   from airflow.providers.http.hooks.http import HttpHook
   from airflow.sdk import DAG, task


   @task
   def list_user_ids():
       return [1, 2, 3, 4, 5]


   @task
   def fetch_user(user_id: int):
       hook = HttpHook(http_conn_id="api_default", method="GET")
       response = hook.run(endpoint=f"/users/{user_id}")
       return response.json()


   with DAG(dag_id="dti-sync-http-example", start_date=datetime(2022, 1, 1)) as dag:
       fetch_user.iterate(user_id=list_user_ids())

The scheduler only manages a single task. With sync tasks, iterations are
executed in a multi-threaded fashion, which eliminates scheduling overhead
and can speed up compute-bound workloads. However, for I/O-bound operations
like HTTP requests, multi-threading alone does not provide the same
performance benefits as async multiplexing — threads still block on each
request individually rather than sharing a single event loop.

To truly **multiplex** I/O-bound operations, use an async task with
:class:`~airflow.providers.http.hooks.http.HttpAsyncHook`:

.. code-block:: python

   from datetime import datetime

   from airflow.providers.http.hooks.http import HttpAsyncHook
   from airflow.sdk import DAG, task


   @task
   def list_user_ids():
       return [1, 2, 3, 4, 5]


   @task
   async def fetch_user(user_id: int):
       hook = HttpAsyncHook(http_conn_id="api_default", method="GET")
       async with hook.session() as session:
           response = await session.run(endpoint=f"/users/{user_id}")
           return await response.json()


   with DAG(dag_id="dti-async-http-example", start_date=datetime(2022, 1, 1)) as dag:
       fetch_user.iterate(user_id=list_user_ids())

When ``iterate()`` is used with an async task, all iterations share the same
event loop, enabling true multiplexing of I/O-bound operations without any
manual concurrency management by the DAG author. For five user IDs the
difference is negligible, but for hundreds or thousands of items the
concurrent approach is dramatically faster — see the
:ref:`benchmarks above <sdk-dynamic-task-mapping-vs-iteration>`.

Why Dynamic Task Iteration?
---------------------------

DTI is designed to address limitations of Dynamic Task Mapping in specific scenarios:

- **Scheduler scalability**:
  DTM creates one Task Instance per item, which can put pressure on the scheduler
  for very large datasets. DTI avoids this by keeping execution within a single task.

- **Async multiplexing**:
  With Python-native async support in Airflow 3.2, DTI allows multiple
  operations to share the same event loop within a single Task Instance.
  This enables efficient multiplexing of I/O-bound workloads.

- **Lower overhead**:
  No need to serialize, schedule, and track thousands of Task Instances.

- **Triggerer and deferrable-operator bottleneck**:
  Deferrable operators delegate async work to triggerers, which store yielded
  events directly in the Airflow metadata database. Unlike workers, triggerers
  cannot leverage a custom XCom backend to offload large payloads. This makes
  triggerers a bottleneck for sustained high-load async execution or workloads
  that return large results. Dynamic Task Mapping with deferrable operators
  amplifies the problem further. DTI sidesteps triggerers entirely — iterations
  execute on workers, which scale more effectively and support custom XCom
  backends.

  For more on deferred vs async trade-offs, see :doc:`deferred-vs-async-operators`.

DTI is especially useful for patterns such as:

- API pagination
- Bulk HTTP or database calls
- High-throughput async workloads
- Streaming or lazily-evaluated XCom results

Hooks as Building Blocks
^^^^^^^^^^^^^^^^^^^^^^^^

DTI encourages a pattern where DAG authors call **hooks** directly from
``@task``-decorated functions rather than relying on operators. Operators are
wrappers around hooks and sometimes expose only a subset of the hook's
capabilities. By calling hooks directly, users gain full control over
concurrency, error handling, and batching.

For example, instead of using ``HttpOperator`` in deferrable mode (which
delegates to the triggerer for a single request at a time), an async
``@task`` can call :class:`~airflow.providers.http.hooks.http.HttpAsyncHook`
directly to perform many concurrent requests. With DTI, the framework
handles the iteration, concurrency, and event-loop management
automatically — the DAG author only writes the per-item logic.

This "hooks as building blocks" approach is especially powerful with async
hooks, where the shared event loop enables concurrent I/O without any
manual ``asyncio.gather`` or ``asyncio.Semaphore`` management.

For more examples of calling async hooks directly from tasks, see
:doc:`deferred-vs-async-operators`.

Comparison
----------

.. list-table::
   :header-rows: 1

   * - Aspect
     - Dynamic Task Mapping (DTM)
     - Dynamic Task Iteration (DTI)
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
     - Entire task retries
   * - Observability
     - Per item in UI
     - Aggregated in a single task
   * - Triggerer dependency
     - Deferrable mapped tasks rely on triggerers
     - No triggerers involved
   * - XCom backend
     - Workers support custom XCom backends
     - Workers support custom XCom backends (triggerers do not)
   * - Use case
     - Independent, trackable units of work
     - High-throughput or streaming workloads

When to Use Dynamic Task Mapping
--------------------------------

Prefer DTM when:

- Each item must be independently tracked in the UI.
- You need fine-grained retries per item.
- Tasks are long-running or resource-intensive.
- Work should be distributed across multiple workers.
- Scheduling decisions should be made per item.

When to Use Dynamic Task Iteration
-----------------------------------

Prefer DTI when:

- You are processing large numbers of small items.
- Scheduler overhead becomes a concern.
- You are using async operators and want to leverage a shared event loop.
- Workloads are I/O-bound and benefit from multiplexing.
- Fine-grained observability per item is not required.

When **not** to use DTI
-----------------------

Avoid Dynamic Task Iteration when:

- You need per-item retries or failure isolation.
- Each item represents a long-running or heavy computation.
- You require detailed visibility per item in the Airflow UI.
- Work must be distributed across multiple worker nodes.

Note that DTI retries the **entire task** on failure — all items are reprocessed
from the beginning. This trade-off is generally acceptable when total processing
time is short (e.g., minutes rather than hours), but it may be undesirable for
workloads where individual items are expensive to reprocess.

.. tip::

   DTI is a **third execution option** alongside Dynamic Task Mapping and
   deferrable operators. It is not intended as a replacement for either.
   Triggerers remain the right choice for long-running polling or waiting tasks
   (e.g., monitoring a remote job or waiting for a Kubernetes pod to complete).

Combining DTM and DTI (Dynamic Task Partitioning)
--------------------------------------------------

.. note::

   Dynamic Task Partitioning is a planned future feature that will build on
   top of Dynamic Task Iteration once DTI is fully implemented. The pattern
   described here is not yet available.

DTM and DTI are not mutually exclusive in principle. A future *Dynamic Task
Partitioning* pattern could use DTM to split a large dataset into
coarse-grained chunks, where each mapped task processes its chunk using DTI.

For example, downloading 17,000 files could be partitioned into 17 chunks of
1,000 files each. DTM would create one task per chunk, and DTI would iterate
within each chunk using a shared event loop for concurrent I/O.

This pattern would provide:

- **Coarse-grained retry**: if a chunk fails, only that chunk is retried — not all 17,000 items.
- **Reduced scheduler load**: the scheduler manages chunks (e.g., 17 tasks) instead of individual items (17,000 tasks).
- **High throughput within each chunk**: async I/O processes items concurrently inside each task.

Relationship with Async Operators
----------------------------------

DTI complements async operators introduced in Airflow 3.2.

- Async operators allow concurrent I/O within a single task.
- DTI allows you to *apply an operator repeatedly* over a dataset within that same task.

Together, they enable patterns such as:

- Efficient API pagination
- Concurrent request batching
- Streaming data processing

Unlike Dynamic Task Mapping, where each mapped task runs in its own execution context,
DTI allows all iterations to share the same event loop, enabling true multiplexing.

Because DTI executes on workers rather than triggerers, it also benefits from the
full worker environment: custom XCom backends, Edge Worker support, and the
scalability of execution frameworks such as Celery.

For more details on async execution, see :doc:`deferred-vs-async-operators`.

Future Outlook
--------------

As Python's async ecosystem evolves, DTI tasks will benefit from improved
introspection and tooling. For example, Python 3.14 introduces new
`asyncio introspection capabilities <https://docs.python.org/3/whatsnew/3.14.html#whatsnew314-asyncio-introspection>`_
that could eventually enable structured progress reporting in the Airflow UI
for DTI tasks — providing per-item visibility without the overhead of per-item
task instances.
