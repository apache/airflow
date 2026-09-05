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

.. _troubleshooting:

Troubleshooting
===============

Obscure task failures
^^^^^^^^^^^^^^^^^^^^^

Task state changed externally
-----------------------------

This message indicates that the task instance's state does not match the state reported by another component. The message itself does not identify the root cause.

What this message means
"""""""""""""""""""""""

Task state can be updated by different Airflow components or by
external actions. If the state reported by the executor does not match
the state stored for the task instance, Airflow may log a state mismatch.

One common message looks like this::

    Executor <executor> reported that the task instance <ti> finished with state <executor_state>, but the task instance's state attribute is <ti_state>.

Check the logs around the time of the state change to determine what caused it.

How to investigate
""""""""""""""""""

Start with the task and scheduler logs, then check the worker or
infrastructure logs if the cause is not clear.

1. Check task logs. If there are no logs in the UI, the task likely never started on a worker, or the worker died before it could write logs.
2. Check scheduler logs around the same timestamp.
3. Check worker or executor logs, to see whether the task was received, started, or never dispatched.
4. Check infrastructure logs (container, pod, or host) for OOM, eviction, or restarts. See :ref:`troubleshooting-process-terminated-by-signal` for SIGTERM and SIGKILL.
5. Check whether a user or an external process changed the task state in the Airflow UI or through the :doc:`Airflow REST API <stable-rest-api-ref>`.

Common causes
"""""""""""""

Below are some example scenarios where a task's state may be changed by a component other than the executor:

- If a task's Dag failed to parse on the worker, the scheduler may mark the task as failed. If confirmed, consider increasing :ref:`core.dagbag_import_timeout <config:core__dagbag_import_timeout>` and :ref:`dag_processor.dag_file_processor_timeout <config:dag_processor__dag_file_processor_timeout>`.
- A task can be retried or marked as failed if it remains queued longer than :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>`. See :ref:`troubleshooting-task-stuck-queued`.
- If a :ref:`task instance's heartbeat times out <concepts:task-instance-heartbeat-timeout>`, it will be marked failed by the scheduler. See :ref:`troubleshooting-task-stuck-running`.
- The task process was killed by the operating system or orchestrator. See :ref:`troubleshooting-process-terminated-by-signal`.
- A user or an external process marked the task as successful or failed in the Airflow UI or through the :doc:`Airflow REST API <stable-rest-api-ref>`.

.. _troubleshooting-task-stuck-queued:

Task stuck in queued state
--------------------------

A task remains ``queued`` while it is waiting to be executed. If it stays queued longer than :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>` (default 600 seconds), it may be retried or marked as failed. There will often be no task logs in the UI, because a worker never ran the task.

Here are some of the common causes:

- Not enough worker capacity, pool slots, or :ref:`core.parallelism <config:core__parallelism>`.
- The executor is unable to dispatch tasks to workers.
- The queued timeout is shorter than how long tasks wait under normal load.

How to troubleshoot:

- Check scheduler logs for tasks stuck in ``queued``.
- Confirm workers are running and accepting work.
- Check pool and concurrency limits.
- If tasks wait in queue longer than the timeout under normal load, increase :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>` or add worker capacity.

.. _troubleshooting-task-stuck-running:

Task stuck in running state
---------------------------

A task may remain ``running`` in the UI even though it appears to make no progress. If the task instance stops sending heartbeats, the scheduler detects a :ref:`task instance heartbeat timeout <concepts:task-instance-heartbeat-timeout>` (formerly called a zombie task) and may mark the task as failed or reschedule it.

Here are some of the common causes:

- The worker ran out of memory and was killed. See :ref:`troubleshooting-oom`.
- The worker stopped running or stopped sending heartbeats, for example after a restart, eviction, scale-down, or liveness probe failure.

How to troubleshoot:

- Check the task logs. If they stop abruptly, inspect worker and infrastructure logs for the same timestamp.
- Check whether the worker is still running and sending heartbeats.
- Check infrastructure logs for OOM kills, restarts, evictions, liveness probe failures, or scale-down events.
- If the worker is healthy but heartbeat timeouts continue to occur, review :ref:`scheduler.task_instance_heartbeat_timeout <config:scheduler__task_instance_heartbeat_timeout>`.

.. _troubleshooting-process-terminated-by-signal:

Process terminated by signal
----------------------------

Sometimes, Airflow or some adjacent system will kill a task instance's ``TaskRunner``, causing the task instance to fail.

Below we discuss a few common cases.

Dag run timeout
"""""""""""""""

A dag run timeout can be specified by ``dagrun_timeout`` in the dag's definition.
The task process would likely be killed with SIGTERM (exit code -15).

.. _troubleshooting-oom:

Out of memory error (OOM)
"""""""""""""""""""""""""

When a task process consumes too much memory for a worker, the best case scenario is it is killed
with SIGKILL (exit code -9). Depending on configuration and infrastructure, it is also
possible that the whole worker will be killed due to OOM and then the tasks would be marked as
failed after failing to heartbeat.

Lingering task supervisor processes
-----------------------------------

Under very high concurrency the socket handlers inside the task supervisor may
miss the final EOF events from the task process. When this occurs the supervisor
believes sockets are still open and will not exit. The
:ref:`workers.socket_cleanup_timeout <config:workers__socket_cleanup_timeout>` option controls how long the supervisor
waits after the task finishes before force-closing any remaining sockets. If you
observe leftover ``supervisor`` processes, consider increasing this delay.
