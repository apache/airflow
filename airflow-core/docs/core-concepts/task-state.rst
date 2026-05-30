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

.. _concepts:task-state:

Task State
==========

.. versionadded:: 3.3

Task state is a persistent key/value store scoped to a single task instance (``dag_id`` + ``run_id`` + ``task_id`` + ``map_index``). It survives worker crashes and task retries within the same Dag run, making it suitable for storing external job IDs, intra-task checkpoints, and progress metadata.

Task state is accessed through the task context via ``context["task_state"]`` and exposes four methods: ``get``, ``set``, ``delete``, and ``clear``.


Accessing task state
--------------------

Inside any ``@task``-decorated function or ``BaseOperator.execute()`` method, task state is available through the ``context`` dictionary via the ``task_state`` key. From there, it can be used to retrieve, set, delete, or clear task state for a specific key-value pair. In this example, the ``job_id`` is retrieved from task state, then upated.

.. code-block:: python
    from airflow.sdk import task
    import random

    @task
    def my_task(**context):
        # Retrieve task_state from context
        task_state = context["task_state"]
        my_value = task_state.get("my_key")

        # Set the new value
        new_value = f"It is {random.randint(1, 12 + 1)} o'clock"
        task_state.set("my_key", new_value)

Reference
-------------

``get(key)``
~~~~~~~~~~~~

Returns the stored string value, or ``None`` if the key does not exist.

.. code-block:: python

    value = task_state.get("job_id")  # returns str or None

``set(key, value, *, retention=None)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Writes or overwrites a value for the specified key. Note, ``value`` can be any JSON-compatible type, including:

* ``str``
* ``int``
* ``float``
* ``bool``
* ``list``
* ``dict``

The optional ``retention`` argument controls when the key expires:

* ``timedelta(...)``: expire after the given duration from the time of the write (e.g. ``timedelta(hours=6)``).  The expiry timestamp is computed on the worker in UTC before the value is sent to the API server.
* ``NEVER_EXPIRE``: the key never expires and is skipped by garbage collection, regardless of the global ``[state_store] default_retention_days`` setting.
* ``None`` (default): fall back to the global ``[state_store] default_retention_days`` config.

.. important::

   ``retention`` accepts only a :class:`~datetime.timedelta`, not a plain integer number of days.  Passing an integer raises a ``TypeError``.

   .. code-block:: python

       # correct
       task_state.set("key", "val", retention=timedelta(days=7))

       # wrong — raises TypeError
       task_state.set("key", "val", retention=7)

``NEVER_EXPIRE`` sentinel
^^^^^^^^^^^^^^^^^^^^^^^^^

Import ``NEVER_EXPIRE`` from ``airflow.sdk.execution_time.context``:

.. code-block:: python

    from airflow.sdk.execution_time.context import NEVER_EXPIRE

    task_state.set("job_id", job_id, retention=NEVER_EXPIRE)

``delete(key)``
~~~~~~~~~~~~~~~

Deletes a single key.  No-op if the key does not exist.

.. code-block:: python

    task_state.delete("job_id")

``clear(all_map_indices=False)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deletes *all* state keys for this task instance.

For :doc:`mapped tasks <dynamic-task-mapping>`, the default clears only the current map index.  Pass ``all_map_indices=True`` to wipe state across **every** mapped instance of the task (fleet-wide reset).

.. code-block:: python

    # clear only this map index
    task_state.clear()

    # clear all map indices (fleet-wide)
    task_state.clear(all_map_indices=True)


Use Cases
---------

External job resumption
~~~~~~~~~~~~~~~~~~~~~~~

A common pattern for long-running external jobs: check whether a job ID is already stored before submitting, and use ``NEVER_EXPIRE`` so the key outlives
the default retention window.

.. code-block:: python

    from datetime import timedelta

    from airflow.sdk import DAG, task
    from airflow.sdk.execution_time.context import NEVER_EXPIRE


    with DAG("spark_job_dag", schedule=None):

        @task
        def run_spark_job(**context):
            task_state = context["task_state"]

            # Check for an already-submitted job from a previous attempt.
            job_id = task_state.get("job_id")
            if job_id is None:
                job_id = spark_client.submit_job(...)
                # Store with NEVER_EXPIRE so the key is not garbage-collected before the job finishes
                task_state.set("job_id", str(job_id), retention=NEVER_EXPIRE)

            # Reattach to the job and wait for completion.
            result = spark_client.wait_for_completion(job_id)
            return result

On a retry, the task finds the stored ``job_id`` and reattaches instead of submitting a duplicate job. Another example of this sort of logic can be found in `example_task_state.py <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/example_dags/example_task_state.py>`_.

Intra-task checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~

For tasks that process paginated or batched data, store the last-completed offset so a retry can resume mid-stream rather than restarting from the beginning.

.. code-block:: python

    from airflow.sdk import DAG, task


    with DAG("paginated_ingest", schedule="@daily"):

        @task
        def ingest_pages(**context):
            # Retrieve the task_state
            task_state = context["task_state"]
            raw = task_state.get("last_page")

            start_page = raw + 1 if raw is not None else 1

            for page in range(start_page, total_pages + 1):
                fetch_and_load(page)
                task_state.set("last_page", page)  # Update the task_state for reuse later


On a retry, the task reads ``last_page`` and skips pages that were already processed.

Progress metadata
~~~~~~~~~~~~~~~~~

Task state can expose in-progress metrics for observability — row counts, status strings, or lightweight JSON payloads — without requiring XCom or an external system.

.. code-block:: python

    import json

    from airflow.sdk import DAG, task


    with DAG("row_ingest", schedule="@hourly"):

        @task
        def ingest_rows(**context):
            task_state = context["task_state"]
            total = 0

            for batch in get_batches():
                load(batch)
                total += len(batch)
                task_state.set(
                    "progress",
                    {
                        "rows_loaded": total,
                        "status": "running"
                    },
                )

            task_state.set(
                "progress",
                json.dumps({
                    "rows_loaded": total,
                    "status": "done"
                }),
            )

The ``progress`` key is visible through the REST API and the Airflow UI while the task is running.


Sync vs. deferrable tasks
-------------------------

Task state behaves slightly differently depending on whether a task runs synchronously or uses the deferral mechanism.

Synchronous tasks
~~~~~~~~~~~~~~~~~

If the worker process crashes, the task instance is retried.  Task state written before the crash is preserved, so the retry can pick up where the previous attempt left off (see the `External job resumption`_pattern above).

Deferrable tasks
~~~~~~~~~~~~~~~~

Once a task defers, the Triggerer handles continuity across poke cycles. Use task state in deferrable tasks only when you need to survive an operator-initiated clear, not for normal poke continuity.


Mapped tasks
------------

When a task is dynamically mapped (``task.expand(...)``), each map index has its own task state namespace.  ``clear()`` without arguments clears state only for the current index.  ``clear(all_map_indices=True)`` wipes state across every index of the task.

.. code-block:: python

    # Inside a mapped task — clear only this index
    task_state.clear()

    # Wipe state for all indices of this task
    task_state.clear(all_map_indices=True)


Automatic cleanup (``clear_on_success``)
-----------------------------------------

When ``[state_store] clear_on_success = True``, all task state keys for a task instance are automatically deleted when the task moves to the ``success`` state.  This is useful for reducing storage when post-success observability is not needed.

.. note::

   ``clear_on_success`` clears **task state only**.  Asset state is scoped to the asset, not the task instance, and is never affected by this setting. Asset state persists across runs and must be cleared explicitly.

See :doc:`/administration-and-deployment/state-store` for full configuration details.
