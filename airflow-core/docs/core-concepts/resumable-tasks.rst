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

.. _concepts-resumable-tasks:

Resumable Tasks
===============

.. versionadded:: 3.3.0

Many data engineering workflows involve submitting work to an external system and waiting for it
to complete. A Spark job, a BigQuery query, a Kubernetes batch pod, an EMR step: these are all
tasks where the real work happens outside Airflow, and the operator's job is mostly to submit,
poll, and collect the result.

These tasks share a common failure mode. In classic operator cases, the worker slot is held for the
entire polling duration, and if the worker process is restarted or the host is preempted, the task
retries from scratch, losing all the progress made. Depending on the operator, that means the external
job is submitted again, creating a duplicate run in context of the external system.

.. _concepts-durable-execution:

Durable execution
-----------------

Surviving that failure mode is what **durable execution** means: a task outlives the loss of the
process running it and can continue / re-attach from where it stopped, rather than repeating work
already done or submitting a duplicate job to an external system.

The :doc:`task state store <task-state-store>` is the mechanism Airflow provides to achieve it. It
is the only per-task-instance storage that outlives a worker crash and is still readable by the
next attempt, which is what lets a retry recover a checkpoint or an external job identifier written
by the attempt before it. Durable execution is the outcome; the task state store is how you get it.

Operator documentation across the provider ecosystem uses the same term for the operator-level
feature built on this mechanism, normally exposed as a ``durable`` parameter. Spark, Kubernetes,
Databricks, Snowflake, BigQuery, Redshift and Glue operators each have a "Durable execution"
section describing what they persist and how they reconnect on retry.

Airflow recommends three approaches for achieving this with long-running external work.
Understanding the trade-offs between them helps you choose the right one for your situation.

.. _concepts-resumable-tasks-deferrable:

Deferrable Operators
--------------------

A deferrable operator pauses itself at the point where it would otherwise start polling, hands
the polling work to the Triggerer component, and releases its worker slot. When the external
condition is met, the Triggerer wakes the task and the worker resumes from where the operator
left off.

This is the most resource-efficient option. A single Triggerer process can concurrently watch
thousands of conditions, so the rest of the worker pool stays free for other tasks.

The trade-offs are:

* A Triggerer component must be running. Deployments that do not include a Triggerer cannot use this pattern.
* Writing a custom deferrable operator requires implementing a dedicated ``Trigger`` class in
  addition to the operator itself.
* The polling logic runs inside the Triggerer's async event loop. Blocking calls inside a
  Trigger stall the entire Triggerer process.

If a deferrable operator already exists for your use case, or your team is comfortable
implementing one, this is the recommended path considering its resource efficiency.

For more details, see :doc:`../authoring-and-scheduling/deferring`.

.. _concepts-resumable-tasks-resumable:

Resumable Tasks
---------------

A resumable task uses the task state store to persist a checkpoint before
it would otherwise lose progress. On retry, the task reads that checkpoint and continues from
where it left off rather than starting over.

The worker slot is held for the full duration of the task, the same as a standard synchronous
operator. The benefit is crash safety and continuity, not resource efficiency.

Resumable tasks are useful when:

* No deferrable operator exists for your external system and writing one is not practical.
* You want crash recovery without operating a Triggerer.
* The task processes work incrementally (for example, reading files from a list or paginating
  through API results) and should be able to resume from its last completed batch.

**General pattern**

The task reads a checkpoint from ``task_state_store`` at the start, does some work, writes an updated
checkpoint, and either continues or finishes. On the next run (whether due to a retry after a
crash or a deliberate reschedule), it reads the checkpoint again and picks up from there.

.. code-block:: python

    from airflow.sdk import dag, task


    @dag(schedule=None)
    def process_files_dag():

        @task(retries=5)
        def process_files(context=None):
            task_state_store = context["task_state_store"]
            files = ["a.csv", "b.csv", "c.csv", "d.csv"]

            last_processed = task_state_store.get("last_processed")
            start_index = 0
            if last_processed is not None:
                start_index = files.index(last_processed) + 1

            for file in files[start_index:]:
                # ... process the file ...
                task_state_store.set("last_processed", file)

        process_files()


    process_files_dag()

This pattern works without any additional work, relying only on ``context``. The state store is just
a key-value store scoped to the task instance, and what you checkpoint is up to you.

**Resumable operators for external jobs**

When the task submits a job to an external system and then polls for completion, there is an
additional problem: on retry, the task would submit a second job even though the first one may
still be running. Instead of having to handle this manually, the :class:`~airflow.sdk.ResumableJobMixin`
solves this by persisting the external job identifier before polling starts, and reconnecting to the
existing job on retry instead of submitting a new one.

For more details and a working example, see :class:`~airflow.sdk.ResumableJobMixin`.

**Clearing a task is treated the same as a retry**

Clearing a task instance does not delete its ``task_state_store`` rows -- they are only removed
when the ``dag_run`` itself is deleted, or by :ref:`airflow state-store clean
<task-and-asset-state-store-cleanup>`. For a checkpointed task this is usually what you want:
clearing resumes from the last checkpoint rather than starting over.

For an operator with durable execution, it means clearing a task whose external job already
succeeded reads that stored result back and returns immediately, without resubmitting the job. If
you want clearing to always resubmit regardless of a prior success, set
``[state_store] clear_on_success = True``, which deletes a task's state store rows automatically
when it moves to ``SUCCESS`` (see :doc:`/administration-and-deployment/task-and-asset-state-store`).

This does not guarantee the external job is still there to reconnect to, though. Clearing a task
that is actively running (``deferrable=False``) stops the worker process, which runs the
operator's ``on_kill``. Most operators with durable execution cancel the external job there by
default, so the next attempt finds it already stopped instead of still running -- an operator that
leaves the job running by default on kill is the exception, check its own docs. Deferred tasks
(``deferrable=True``) don't have this problem: there is no actively polling worker process for the
clear to interrupt.

.. _concepts-resumable-tasks-async:

Asynchronous Tasks
------------------

.. note::

   Async task support applies to Python tasks only: ``@task`` decorated ``async def`` functions
   and class-based operators that subclass ``BaseAsyncOperator``. It is not available for
   other operator types.

Python tasks support ``async``/``await`` syntax. When the decorated callable is an async
function, Airflow runs it inside an event loop, which lets you fan out many concurrent I/O
operations (HTTP requests, database queries, file reads) within a single task execution without
blocking the event loop while waiting for each one.

The worker slot is held for the full duration of the task. Async tasks are not designed for
long external waits; they are designed for high-throughput I/O work that completes within a
single execution. To survive a worker crash, an async task can checkpoint its progress to
:doc:`task state store <task-state-store>` with ``aget``/``aset`` and resume from the checkpoint on retry.

For guidance on when to use async tasks versus deferrable operators, see
:doc:`task-sdk:deferred-vs-async-operators`.

.. _concepts-resumable-tasks-comparison:

Comparison
----------

.. list-table::
   :header-rows: 1

   * - Characteristic
     - Deferrable operator
     - Resumable task
     - Async task
   * - Worker slot during external wait
     - Freed
     - Held
     - Held
   * - Requires Triggerer
     - Yes
     - No
     - No
   * - Handles crash recovery
     - Yes (via Triggerer)
     - Yes (via task state store checkpoint)
     - No
   * - Prevents duplicate job submission
     - Depends on operator implementation
     - Yes (with ``ResumableJobMixin``)
     - Not applicable
   * - Suitable for concurrent I/O fan-out
     - No
     - No
     - Yes
   * - Available from
     - Airflow 2.2
     - Airflow 3.3
     - Airflow 3.2
