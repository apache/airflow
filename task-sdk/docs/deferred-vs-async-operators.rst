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

Airflow 3.2 introduces Python native async support, enabling task authors to leverage asynchronous I/O for high-throughput workloads.
It is important to understand how this differs from deferred operators.

Deferred Operators
------------------

A deferred operator is an operator that can pause its execution until an external trigger event occurs, without holding a worker slot.
Examples include the HttpOperator in deferrable mode, sensors or operators integrated with triggers.

Key characteristics:

  - Execution is paused while waiting for external events or resources.
  - Worker slots are freed during the wait, improving resource efficiency.
  - Ideal for scenarios where a single external event or a small number of events dictate task completion.

Typically simpler to use, as no custom async logic is required as this is all handled by the deferred operator.

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

   from airflow.providers.http.operators.http import HttpOperator

   task_get_op = HttpOperator(
       http_conn_id="http_conn_id",
       task_id="get_op",
       method="GET",
       endpoint="get",
       data={"param1": "value1", "param2": "value2"},
       deferrable=True,
   )

When to Use Async Python Operators
----------------------------------

Use async Python operators when:

  - The task needs to perform many concurrent requests or operations within a single task.
  - You want to take advantage of the shared event loop to improve throughput.
  - There is simply no deferred operator available.

.. code-block:: python

   import asyncio
   from airflow.sdk import task

   parameters = [
       {"param1": "value1", "param2": "value2"},
       {"param1": "value3", "param2": "value4"},
       {"param1": "value5", "param2": "value6"},
       {"param1": "value7", "param2": "value8"},
   ]

   @task
   async def get_op(parameters: list[dict[str, str]]):
       import asyncio
       from aiohttp import ClientSession
       from airflow.providers.http.hooks.http import HttpAsyncHook

       hook = HttpAsyncHook(http_conn_id="http_conn_id", method="GET")

       async with ClientSession() as session:
           tasks = [
               hook.run(session=session, endpoint="get", data=params)
               for params in parameters
           ]
           # Run all requests concurrently in the shared event loop for high throughput
           responses = await asyncio.gather(*tasks)
           return [await r.json() for r in responses]

   get_op(parameters)

Dynamic Task Mapping with many deferrable operators would create unnecessary overhead, as each mapped task would run its own event loop instead of sharing a single loop.
Dynamic Task Mapping with async operators also don't make sense, as they won't share the same event loop and thus won't improve throughput.
For more details, see the :ref:`dynamic task mapping <sdk-dynamic-mapping>` page.
