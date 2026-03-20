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
=============================================

.. versionadded:: 3.2.0

Airflow provides multiple ways to process collections of data in parallel or concurrently.
This page explains the difference between **Dynamic Task Mapping (DTM)** and
**Dynamic Task Iteration (DTI)**, and when to use each.

While both approaches allow you to apply an operation over a collection,
they differ significantly in execution model, scheduler impact, and observability.

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

Example:

.. code-block:: python

   from datetime import datetime
   from airflow.sdk import DAG, task

   @task
   def make_list():
       # This could fetch data from an API, database, etc.
       return ["a", "b", "c"]


   @task
   def consume(item: str):
       print(item)


   with DAG(dag_id="dynamic-map-generated", start_date=datetime(2022, 1, 1)) as dag:
       consume.expand(item=make_list())

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

Conceptual example:

.. code-block:: python

   import asyncio
   from datetime import datetime
   from airflow.sdk import DAG, task

   @task
   async def make_list():
      result = []
      for item in ["a", "b", "c"]:
          await asyncio.sleep(0.1)  # Simulate async I/O
          result.append(item)
      return result


   @task
   def consume(item: str):
       print(item)


   with DAG(dag_id="dynamic-map-generated", start_date=datetime(2022, 1, 1)) as dag:
       consume.iterate(item=make_list())

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

DTI is especially useful for patterns such as:

- API pagination
- Bulk HTTP or database calls
- High-throughput async workloads
- Streaming or lazily-evaluated XCom results

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
   * - Use case
     - Independent, trackable units of work
     - High-throughput or streaming workloads

When to Use Dynamic Task Mapping
-------------------------------

Prefer DTM when:

- Each item must be independently tracked in the UI.
- You need fine-grained retries per item.
- Tasks are long-running or resource-intensive.
- Work should be distributed across multiple workers.
- Scheduling decisions should be made per item.

When to Use Dynamic Task Iteration
---------------------------------

Prefer DTI when:

- You are processing large numbers of small items.
- Scheduler overhead becomes a concern.
- You are using async operators and want to leverage a shared event loop.
- Workloads are I/O-bound and benefit from multiplexing.
- Fine-grained observability per item is not required.

When **not** to use DTI
----------------------

Avoid Dynamic Task Iteration when:

- You need per-item retries or failure isolation.
- Each item represents a long-running or heavy computation.
- You require detailed visibility per item in the Airflow UI.
- Work must be distributed across multiple worker nodes.

Relationship with Async Operators
--------------------------------

DTI complements async operators introduced in Airflow 3.2.

- Async operators allow concurrent I/O within a single task.
- DTI allows you to *apply an operator repeatedly* over a dataset within that same task.

Together, they enable patterns such as:

- Efficient API pagination
- Concurrent request batching
- Streaming data processing

Unlike Dynamic Task Mapping, where each mapped task runs in its own execution context,
DTI allows all iterations to share the same event loop, enabling true multiplexing.

For more details on async execution, see :doc:`deferred-vs-async-operators`.
