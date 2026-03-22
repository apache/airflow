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

.. _sdk-deferred-vs-async-operators:

Deferred vs Async Operators
===========================

.. versionadded:: 3.2.0

Airflow 3.2 introduces Python-native async support for tasks, allowing concurrent I/O within a single worker slot.
This page explains how async operators differ from deferred operators and when to use each.

Deferred Operators
------------------

A deferred operator is an operator that can pause its execution until an external trigger event occurs,
without holding a worker slot. For more details see :doc:`airflow:authoring-and-scheduling/deferring`.
Examples include the HttpOperator in deferrable mode, sensors or operators integrated with triggers.

Key characteristics:

- Execution is paused while waiting for external events or resources.
- Worker slots are freed during the wait, improving resource efficiency.
- Ideal for scenarios where a single external event or a small number of events dictate task completion.
- Typically simpler to use, as the deferred operator handles all async logic.

Async Python Operators
----------------------

Python native async operators allow you to write tasks that leverage Python's asyncio:

- Tasks can perform many concurrent I/O operations efficiently within a single worker slot sharing the same event loop.
- Task code uses async/await syntax with async-compatible hooks, such as HttpAsyncHook or the SFTPHookAsync.

Ideal when you need to perform high-throughput operations (e.g., many HTTP requests, database calls, or API interactions) within a single task instance,
or when there is no deferred operator available but there is an async hook available.

When to Use Deferred Operators
------------------------------

Prefer a deferred operator when:

- There is an existing deferrable operator that covers your use case (e.g., HttpOperator deferrable mode).
- The task waits for a single or limited external events.
- You want to free worker resources while waiting for triggers.
- You don't need to loop over the same operator multiple times (e.g. multiplexing).

.. code-block:: python

   from airflow.sdk import dag
   from airflow.providers.http.operators.http import HttpOperator


   @dag(schedule=None)
   def deferred_http_operator_dag():

       get_op_task = HttpOperator(
           http_conn_id="http_conn_id",
           task_id="get_op",
           method="GET",
           endpoint="get",
           data={"param1": "value1", "param2": "value2"},
           deferrable=True,
       )


   deferred_http_operator_dag()

When to Use Async Python Operators
----------------------------------

Use async Python operators when:

- The task needs to perform many concurrent requests or operations within a single task.
- You want to take advantage of the shared event loop to improve throughput.
- There is simply no deferred operator available.
- The logic depends on custom Python code (e.g. callables or lambdas) that cannot easily be implemented in a trigger, since triggers must be serializable and do not have access to DAG code at runtime.

.. note::

   The :class:`~airflow.providers.http.hooks.http.HttpAsyncHook` depends on ``aiohttp``,
   which is installed automatically with the `apache-airflow-providers-http <https://airflow.apache.org/docs/apache-airflow-providers-http>`_ provider.

Simple Async Example
~~~~~~~~~~~~~~~~~~~~

The following example demonstrates the basic syntax for writing an async task
using ``async``/``await``.

.. note::

   For a single request like this, a deferrable operator (such as
   :class:`~airflow.providers.http.operators.http.HttpOperator` with
   ``deferrable=True``) is usually preferred. Deferred operators release the
   worker slot while waiting for the external request to complete.

   This example is provided mainly to illustrate the structure of an async
   task. Async operators become most useful when performing many concurrent
   operations within the same task (see the multiplexing example below), or
   when implementing logic such as pagination where multiple requests need to
   be executed sequentially or concurrently within a single task instance.

.. code-block:: python

   from aiohttp import ClientSession
   from airflow.providers.http.hooks.http import HttpAsyncHook
   from airflow.sdk import dag, task


   @dag(schedule=None)
   def async_http_operator_dag():

       @task
       async def get_op():
           hook = HttpAsyncHook(http_conn_id="http_conn_id", method="GET")

           async with ClientSession() as session:
               response = await hook.run(
                   session=session,
                   endpoint="get",
                   data={"param1": "value1", "param2": "value2"},
               )
               return await response.json()

       get_op()


   async_http_operator_dag()

Async Multiplexing Example
~~~~~~~~~~~~~~~~~~~~~~~~~~

Async operators become particularly useful when making many concurrent requests
within a single task. The following example executes multiple HTTP requests
concurrently using ``asyncio.gather`` while limiting concurrency with a semaphore.

.. code-block:: python

   import asyncio
   from aiohttp import ClientSession
   from airflow.providers.http.hooks.http import HttpAsyncHook
   from airflow.sdk import dag, task

   parameters = [
       {"param1": "value1", "param2": "value2"},
       {"param1": "value3", "param2": "value4"},
       {"param1": "value5", "param2": "value6"},
       {"param1": "value7", "param2": "value8"},
   ]


   @dag(schedule=None)
   def async_http_multiplex_dag():

       @task
       async def get_op(parameters: list[dict[str, str]]):
           hook = HttpAsyncHook(http_conn_id="http_conn_id", method="GET")

           # Limit concurrent requests to avoid overwhelming downstream services
           semaphore = asyncio.Semaphore(5)

           async def fetch(session, params):
               async with semaphore:
                   response = await hook.run(
                       session=session,
                       endpoint="get",
                       data=params,
                   )
                   return await response.json()

           async with ClientSession() as session:
               tasks = [fetch(session, params) for params in parameters]

               # Run requests concurrently in the shared event loop
               return await asyncio.gather(*tasks)

       get_op(parameters)


   async_http_multiplex_dag()

.. note::

   The upcoming *Dynamic Task Iteration* feature will simplify patterns like this.
   Instead of manually managing concurrency with constructs such as
   ``asyncio.gather`` and ``asyncio.Semaphore``, authors will be able to iterate
   over asynchronous results directly in downstream tasks while still benefiting
   from a shared event loop. This will make high-throughput patterns such as
   pagination or request multiplexing easier to implement.

MS Graph Async Example
~~~~~~~~~~~~~~~~~~~~~~

Another example using the :class:`~airflow.providers.microsoft.azure.hooks.msgraph.KiotaRequestAdapterHook`,
which is async-only, to fetch all users from the `Microsoft Graph API <https://learn.microsoft.com/en-us/graph/azuread-users-concept-overview/>`__.

In this example, multiple paginated requests are expected in order to retrieve all users.
Using an async Python task is appropriate here because the :class:`~airflow.providers.microsoft.azure.hooks.msgraph.KiotaRequestAdapterHook`
handles pagination internally and performs the requests asynchronously.
This allows multiple paginated requests to be performed efficiently within a single task instance and worker slot.

.. code-block:: python

   from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
   from airflow.sdk import dag, task


   @dag(schedule=None)
   def async_msgraph_dag():

       @task
       async def get_users():
           hook = KiotaRequestAdapterHook.get_hook(conn_id="msgraph_default")

           return await hook.paginated_run(url="users")

       get_users()


   async_msgraph_dag()

When **not** to use Deferred vs Async Operators
-----------------------------------------------

While the previous sections explain when to prefer deferred or async operators,
it is equally important to understand scenarios where one may **not** be appropriate:

- **Avoid async operators for long waits**
  Async operators keep the task process alive while waiting. If your task involves long-running
  operations, such as slow APIs or external triggers, this can waste worker resources.
  In such cases, prefer a deferred operator, which releases the worker slot while waiting.

- **Avoid deferred operators for many short or repeated waits**
  Deferred operators pause and resume tasks each time they defer. If deferrals occur frequently
  or last only a short time, the overhead of stopping and restarting the task can reduce efficiency.
  For high-frequency short waits, an async operator may be more suitable.

- **Operator availability should not dictate choice**
  Having a built-in deferrable operator can simplify implementation, but the decision
  should be driven by the use case, not just what Airflow provides.
  If your workflow is better suited for deferring but no operator exists yet,
  consider implementing a custom deferred operator rather than defaulting to async.

Comparison with Dynamic Task Mapping
------------------------------------

Async operators and Dynamic Task Mapping solve different problems and have different trade-offs.

.. list-table::
   :header-rows: 1

   * - Aspect
     - Async ``@task``
     - Dynamic Task Mapping (deferrable)
   * - Worker slots
     - 1 worker slot (shared event loop)
     - N worker slots (one per mapped task instance)
   * - Concurrency model
     - Async I/O inside a single task
     - Parallel task instances scheduled by the scheduler
   * - Retry behavior
     - Whole task retries
     - Individual mapped tasks can retry independently
   * - UI visibility
     - Appears as a single task
     - Each mapped task is visible separately
   * - Scheduler overhead
     - Minimal
     - Scheduler must manage N task instances

For more details about Dynamic Task Mapping, see the
:ref:`dynamic task mapping <sdk-dynamic-task-mapping>` page.
