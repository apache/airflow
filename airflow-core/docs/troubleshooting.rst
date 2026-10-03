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

How to debug your Airflow deployment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The sections below walk through Airflow deployment issues using David A. Wheeler's writeup
of Agans' nine rules of debugging [1]_, with an Airflow-specific action for each rule. They are
written as a general starting point; for the specific known failure modes already documented,
see `Obscure task failures`_ below.

Understand the system
----------------------

A minimal Airflow deployment is made up of a *scheduler*, a *Dag processor*, a *Dag bundle*,
an *API server*, and a *metadata database*; larger deployments add *workers* and a *triggerer*.
Each component can fail independently, and Airflow 3 removed the standalone webserver process
in favor of the API server, and moved Dag parsing out of the scheduler and into its own
*Dag processor* process. Before debugging a specific failure, know which of these components
is involved. See :doc:`/core-concepts/overview` for the full component breakdown.

Make it fail
------------

Reproduce the failure outside of the full scheduling loop before you start changing things:

- ``airflow tasks test <dag_id> <task_id> [logical_date_or_run_id]`` runs a single task instance
  without checking dependencies or recording state in the database.
- ``airflow dags test <dag_id> [logical_date]`` runs one full DagRun locally, without the
  scheduler.

Both commands run in your current process, so you can also attach a debugger to them; see
:doc:`/core-concepts/debug` for running a Dag under ``pdb`` or an IDE debugger.

Quit thinking and look
-----------------------

Read the actual task log before guessing at a cause. By default, task logs are written under
``$AIRFLOW_HOME/logs/`` using the path
``dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=<n>.log`` (add a
``map_index=<n>/`` segment for mapped tasks). This is controlled by the
:ref:`logging.log_filename_template <config:logging__log_filename_template>` setting, so check
that setting if logs aren't where you expect them. ``airflow tasks state <dag_id> <task_id>
<logical_date_or_run_id>`` will confirm the recorded state of a task instance before you go
looking at logs at all.

Divide and conquer
-------------------

Narrow the failure down to a single component before digging further:

- ``airflow dags list-import-errors`` shows Dags the *Dag processor* failed to parse. A Dag
  that fails to parse is a Dag-processor problem, not a scheduler problem, in Airflow 3.
- ``airflow db check`` confirms the metadata database is reachable.
- ``airflow tasks failed-deps <dag_id> <task_id> <logical_date_or_run_id>`` shows the unmet
  dependencies that are keeping the scheduler from queuing a task instance.

Change one thing at a time
---------------------------

When testing a fix, change a single variable and re-run ``airflow tasks test`` (or
``airflow dags test``) before layering on the next change. ``airflow config get-value <section>
<option>`` prints the effective value of a single configuration option, so you can confirm
exactly what changed between runs instead of assuming.

Keep an audit trail
--------------------

Record what you tried and what happened. ``airflow version`` records the exact version you were
running; ``airflow dags show <dag_id> --save graph.png`` saves the task dependency graph for a
Dag; ``airflow tasks states-for-dag-run <dag_id> <logical_date_or_run_id>`` records the state of
every task instance in a run. Keeping these alongside your notes makes it possible to tell later
whether a change actually affected behavior.

Check the plug
---------------

Before debugging further, check the things that are easy to overlook:

- Is the Dag paused? ``airflow dags list`` includes an ``is_paused`` column.
- Can the component you're debugging actually reach the metadata database? ``airflow db check``.
- Are you confusing the Dag's *logical date* with wall-clock time? A DagRun's ``logical_date``
  is not the time the run actually started.

Get a fresh view
-----------------

When asking someone else to look at a failure, give them something reproducible rather than a
description: the exact command you ran, the ``airflow version`` output, the relevant log excerpt,
and, if it's a Dag-structure question, a saved graph from ``airflow dags show``. A vague
description is much harder for a second set of eyes to act on than a small, self-contained
reproduction.

If you didn't fix it, it ain't fixed
-------------------------------------

Confirm the fix by reproducing the original failure the same way you made it fail in the first
place -- rerun the same ``airflow tasks test`` or ``airflow dags test`` invocation -- rather than
assuming a code change or a passing unit test alone means the deployment issue is resolved. If
the original failure was intermittent, re-run it more than once before calling it fixed.

.. [1] David A. Wheeler, `Debugging: nine indispensable rules for finding even the most
   elusive software and hardware problems <https://dwheeler.com/essays/debugging-agans.html>`__
   (2004), summarizing David J. Agans' nine rules of debugging.

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

- Not enough worker capacity.
- The executor is unable to dispatch tasks to workers.
- The queued timeout is shorter than how long tasks wait under normal load.

How to troubleshoot:

- Check scheduler logs for tasks stuck in ``queued``.
- Confirm workers are running and accepting work.
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
