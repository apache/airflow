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

.. _concepts:task-state-store:

.. spelling:word-list::

   intra
   Intra
   checkpointing
   Checkpointing
   accessors

Task State Store
================

.. versionadded:: 3.3

Task store is a persistent key/value store scoped to a single task instance (``dag_id`` + ``run_id`` + ``task_id`` + ``map_index``). It survives worker crashes and task retries within the same Dag run, making it suitable for storing external job IDs, intra-task checkpoints, and progress metadata.

Data persisted via task state store is accessed through the task context via ``context["task_state_store"]`` and exposes the synchronous methods ``get``, ``set``, ``delete``, and ``clear``, plus the async counterparts ``aget``, ``aset``, ``adelete``, and ``aclear`` for use inside ``async`` tasks.


Accessing task state store
--------------------------

Inside any ``@task``-decorated function or ``BaseOperator.execute()`` method, task state store is available through the ``context`` dictionary via the ``task_state_store`` key. From there, it can be used to retrieve, set, delete, or clear data for a specific key-value pair. In this example, the ``job_id`` is retrieved from task state store, then updated, before being deleted. All data for that task is then removed using the ``clear`` method.

.. code-block:: python

    from airflow.sdk import task
    import random


    @task
    def my_task(**context):
        # Retrieve task_state_store from context
        task_state_store = context["task_state_store"]
        my_value = task_state_store.get("my_key", default="my_default_key")

        # Set the new value
        new_value = f"It is {random.randint(1, 12 + 1)} o'clock"
        task_state_store.set("my_key", new_value)

        # Delete the value
        task_state_store.delete("my_key")

        # Clear all store entries for the task
        task_state_store.clear()

Reference
---------

``get(key, default)``
~~~~~~~~~~~~~~~~~~~~~

Returns the stored JSON value, or the ``default`` value if the key does not exist.

.. code-block:: python

    value = task_state_store.get(
        "job_id", default="123456789"
    )  # returns the value associated with `job_id` or the default value

``set(key, value, *, retention=None)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Writes or overwrites a value for the specified key. Note, ``value`` can be any JSON-compatible type, except for ``None``. This includes:

* ``str``
* ``int``
* ``float``
* ``bool``
* ``list``
* ``dict``

The optional ``retention`` argument controls when the key expires:

* ``timedelta(...)``: expire after the given duration from the time of the write (e.g. ``timedelta(hours=6)``). The expiry timestamp is computed on the worker before the value is sent to the API server.
* ``NEVER_EXPIRE``: the key never expires and is skipped during garbage collection, regardless of the global ``[state_store] default_retention_days`` setting.
* ``None`` (default): fall back to the global ``[state_store] default_retention_days`` config.

.. important::

   ``retention`` accepts only a :class:`~datetime.timedelta`, not a plain integer number of days. Passing an integer raises a ``TypeError``.

   .. code-block:: python

       # correct
       task_state_store.set("key", "val", retention=timedelta(days=7))

       # wrong — raises TypeError
       task_state_store.set("key", "val", retention=7)

``NEVER_EXPIRE`` sentinel
^^^^^^^^^^^^^^^^^^^^^^^^^

Import ``NEVER_EXPIRE`` from ``airflow.sdk``:

.. code-block:: python

    from airflow.sdk import NEVER_EXPIRE

    task_state_store.set("job_id", job_id, retention=NEVER_EXPIRE)

``delete(key)``
~~~~~~~~~~~~~~~

Deletes a single key. No-op if the key does not exist.

.. code-block:: python

    task_state_store.delete("job_id")

``clear()``
~~~~~~~~~~~

Deletes *all* task state store keys for this task instance.

.. code-block:: python

    task_state_store.clear()

``aget``, ``aset``, ``adelete``, ``aclear``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Async counterparts of ``get``, ``set``, ``delete``, and ``clear`` for use inside ``async`` tasks. They take the same arguments and behave identically to their synchronous siblings, but ``await`` the round-trip to the API server instead of blocking the event loop, so a coroutine can checkpoint its progress without stalling other concurrent work.

.. code-block:: python

    value = await task_state_store.aget("job_id", default="123456789")
    await task_state_store.aset("job_id", job_id, retention=NEVER_EXPIRE)
    await task_state_store.adelete("job_id")
    await task_state_store.aclear()

Calling the synchronous ``get``/``set``/``delete``/``clear`` from inside an ``async`` task blocks the event loop and defeats the concurrency the coroutine was written for. Use the ``a``-prefixed methods there instead.

Some Example Use Cases
----------------------

External job resumption
~~~~~~~~~~~~~~~~~~~~~~~

A common pattern for long-running external jobs: check whether a job ID is already stored before submitting, and use ``NEVER_EXPIRE`` so the key outlives
the default retention window.

.. code-block:: python

    from datetime import timedelta

    from airflow.sdk import DAG, task
    from airflow.sdk import NEVER_EXPIRE

    with DAG("spark_job_dag", schedule=None):

        @task
        def run_spark_job(**context):
            task_state_store = context["task_state_store"]

            # Check for an already-submitted job from a previous attempt.
            job_id = task_state_store.get("job_id")
            if job_id is None:
                job_id = spark_client.submit_job(...)
                # Store with NEVER_EXPIRE so the key is not garbage-collected before the job finishes
                task_state_store.set("job_id", job_id, retention=NEVER_EXPIRE)

            # Reattach to the job and wait for completion.
            result = spark_client.wait_for_completion(job_id)
            return result

On a retry, the task finds the stored ``job_id`` and reattaches instead of submitting a duplicate job. Another example of this sort of logic can be found in `example_task_state_store.py <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/example_dags/example_task_state_store.py>`_.

For ``BaseOperator`` subclasses, the :class:`~airflow.sdk.bases.resumablemixin.ResumableJobMixin` encapsulates this pattern. It persists the external job ID to task state store after submission and, on retry, reconnects to an active job or resubmits if the prior job reached a terminal failure state.

Intra-task checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~

For tasks that process paginated or batched data, store the last-completed offset so a retry can resume mid-stream rather than restarting from the beginning.

.. code-block:: python

    from airflow.sdk import DAG, task

    with DAG("paginated_ingest", schedule="@daily"):

        @task
        def ingest_pages(**context):
            # Retrieve the task_state_store
            task_state_store = context["task_state_store"]
            raw = task_state_store.get("last_page")

            start_page = raw + 1 if raw is not None else 1

            for page in range(start_page, total_pages + 1):
                fetch_and_load(page)
                task_state_store.set("last_page", page)  # Update the task_state_store for reuse later


On a retry, the task reads ``last_page`` and skips pages that were already processed.

Checkpointing from an async task
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An ``async`` task gains concurrency by awaiting I/O instead of blocking on it. To keep that benefit while checkpointing, use the async accessors ``aget``/``aset`` so the task store round-trip does not stall the event loop. This is the same paginated-ingest pattern as above, written for a coroutine that paginates through an external API.

.. code-block:: python

    from airflow.sdk import DAG, task

    with DAG("async_paginated_ingest", schedule="@daily"):

        @task
        async def ingest_pages(**context):
            task_state_store = context["task_state_store"]
            last_page = await task_state_store.aget("last_page")

            start_page = last_page + 1 if last_page is not None else 1

            for page in range(start_page, total_pages + 1):
                await fetch_and_load(page)
                await task_state_store.aset("last_page", page)

After a crash the task resumes from the stored ``last_page`` instead of restarting at page 1, without ever blocking the event loop.

Progress metadata
~~~~~~~~~~~~~~~~~

Task store can expose in-progress metrics for observability — row counts, status strings, or lightweight JSON payloads — without requiring XCom or an external system.

.. code-block:: python


    from airflow.sdk import DAG, task

    with DAG("row_ingest", schedule="@hourly"):

        @task
        def ingest_rows(**context):
            task_state_store = context["task_state_store"]
            total = 0

            for batch in get_batches():
                load(batch)
                total += len(batch)
                task_state_store.set(
                    "progress",
                    {"rows_loaded": total, "status": "running"},
                )

            task_state_store.set(
                "progress",
                {"rows_loaded": total, "status": "done"},
            )

The ``progress`` key is visible through the REST API and the Airflow UI while the task is running.


Sync vs. deferrable tasks
-------------------------

Task store behaves slightly differently depending on whether a task runs synchronously or uses the deferral mechanism.

Synchronous tasks
~~~~~~~~~~~~~~~~~

If the worker process crashes, the task instance is retried. Task store data written before the crash is preserved, so the retry can pick up where the previous attempt left off (see the `External job resumption`_ pattern above).

Deferrable tasks
~~~~~~~~~~~~~~~~

Once a task defers, the Triggerer handles continuity across poke cycles. Use task state store in deferrable tasks only when you need to survive an operator-initiated clear, not for normal poke continuity.


Mapped tasks
------------

When a task is dynamically mapped (``task.expand(...)``), each map index has its own task state store namespace. ``clear()`` clears only the current index's store.

To wipe state across all map indices of a task, use the :doc:`Core API </administration-and-deployment/task-and-asset-state-store>` (e.g. via the UI or CLI) after the task group has finished.

.. code-block:: python

    # Inside a mapped task — clears only this index
    task_state_store.clear()


Automatic cleanup (``clear_on_success``)
----------------------------------------

When ``[state_store] clear_on_success = True``, all task state store keys for a task instance are automatically deleted when the task moves to the ``success`` state. This is useful for reducing storage when post-success observability is not needed.

.. note::

   ``clear_on_success`` clears **task state store only**. Asset store is scoped to the asset, not the task instance, and is never affected by this setting. Asset store persists across runs and must be cleared explicitly.

See :doc:`/administration-and-deployment/task-and-asset-state-store` for full configuration details.
