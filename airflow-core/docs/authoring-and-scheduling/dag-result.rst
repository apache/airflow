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

.. _dag-result:

==========
DAG Result
==========

|experimental|

.. versionadded:: 3.3

Airflow supports designating one or more tasks as *result tasks* for a DAG. When a DAG has a result task, its return value is surfaced by the ``/dags/{dag_id}/dagRuns/{dag_run_id}/wait`` API, making it straightforward to embed Airflow DAGs in API endpoints, chat agents, or inference services that need a response value without polling or custom glue code.


Marking the result task
=======================

There are two equivalent ways to designate a result task.

Using ``@result`` (TaskFlow)
----------------------------

The :func:`airflow.sdk.result` decorator marks a TaskFlow task as the DAG's
result. It must be applied *on top of* a ``@task`` decorator:

.. code-block:: python

    from airflow.sdk import dag, result, task


    @dag
    def my_dag():
        @task
        def fetch_data():
            return {"answer": 42}

        @result
        @task
        def compute(data):
            return data["answer"] * 2

        compute(fetch_data())


    my_dag()

A task decorated with ``@result`` can ``expand()`` or ``expand_kwargs()`` into a
mapped task.

Returning from a ``@dag`` function
-----------------------------------

When using the ``@dag`` decorator, returning a task's ``XComArg`` directly from
the function body automatically designates that task as the result; no explicit
``@result`` is needed:

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def my_dag():
        @task
        def fetch_data():
            return {"answer": 42}

        @task
        def compute(data):
            return data["answer"] * 2

        return compute(fetch_data())  # 'compute' marked as the result task.


    my_dag()

Only a plain ``XComArg`` (the direct call result of a ``@task`` function) is
accepted. Airflow silently ignores any other return value such as a plain Python
object, so returning a literal integer or string has no effect.

.. note::

    Only the plain ``return_value`` XCom (i.e., what the Python function returns)
    can be designated as a DAG result. XComs pushed under other keys or produced
    with ``multiple_outputs=True`` are not eligible.


Retrieving the result
=====================

Once a DAG has result tasks, call the ``GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/wait``
endpoint to block until the run finishes and collect results in a single request:

.. code-block:: bash

    curl -s "http://localhost:8080/api/v2/dags/my_dag/dagRuns/<run_id>/wait"

The endpoint streams newline-delimited JSON (NDJSON). Each line reports the
current DAG run state, and the final line includes a ``results`` key containing
the return value of the result task, keyed by task ID:

.. code-block:: json

    {"state": "running"}
    {"state": "success", "results": {"compute": 84}}

The ``result`` query parameter overrides the default behaviour and lets callers
choose which task IDs to collect XCom from, regardless of whether a result task
was declared:

.. code-block:: bash

    # Collect XCom from a specific task regardless of @result marking
    curl -s "http://localhost:8080/api/v2/dags/my_dag/dagRuns/<run_id>/wait?result=some_task_id"

    # Suppress all result collection
    curl -s "http://localhost:8080/api/v2/dags/my_dag/dagRuns/<run_id>/wait?result="

When ``result`` is omitted, the API returns the author-declared result task's
XCom. When ``result`` is set to one or more task IDs, those task IDs are used
instead. When ``result`` is set to an empty string, no XCom is collected.

When the result task is dynamically mapped, the entry in ``results`` is a list
containing one value per mapped instance, ordered by task ID and map index.
