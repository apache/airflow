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

How to debug your Airflow deployment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The sections below walk through Airflow deployment issues using David A. Wheeler's writeup
of Agans' nine rules of debugging [1]_, with an Airflow-specific action for each rule.
They are written as a general starting point; for the specific known failure modes, already
documented, see `Obscure task failures`_ below.


Understand the system
----------------------
A minimal Airflow deployment consists of a *scheduler*, a *Dag processor*, a *Dag bundle*,
an *API server*, and a *Metadata database*; larger deployments add *workers* and a *triggerer*.
Each of these components can fail independently, and Airflow 3 removed the standalone webserver
process in favour of the API server, and moved Dag parsing out of scheduler and into its own
*Dag processor* process. Before debugging a specific failure, it is important to know which of
these components are involved. See :doc:`/core-concepts/overview` for the full component breakdown.

Make it fail
----------------

Reproduce the failure outside of the full scheduling loop before you start changing things:

- ``airflow tasks test <dag_id> <task_id> [logical_date_or_run_id]``` runs a single instance
without checking dependencies or updating the database.
- ``airflow dags test <dag_id> [logical_date]``` runs one full DagRun locally without the
scheduler.

Both commands run in your current process, so you can also attach a debugger to them; see
:doc:`core-concepts/debugging` for running a Dag under ``pdb`` or and IDE debugger.

Quit thinking and look
----------------------

Read the actual task log before guessing at a cause. By default, task logs are written under
``$AIRFLOW_HOME/logs`` using the path
``dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=<attempt_number>.log``. (add a
``map_index=<n>/`` segment for mapped tasks). This is controlled by the
:ref:`logging.log_filename_template <config:logging__log_filename_template>` setting, so check
that setting if logs are not where you expect them. ``airflow tasks state <dag_id> <task_id>
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
   (2004), summarising David J. Agans' nine rules of debugging.

Obscure task failures
^^^^^^^^^^^^^^^^^^^^^

Task state changed externally
-----------------------------

There are many potential causes for a task's state to be changed by a component other than the executor, which might cause some confusion when reviewing task instance or scheduler logs.

Below are some example scenarios that could cause a task's state to change by a component other than the executor:

- If a task's Dag failed to parse on the worker, the scheduler may mark the task as failed. If confirmed, consider increasing :ref:`core.dagbag_import_timeout <config:core__dagbag_import_timeout>` and :ref:`dag_processor.dag_file_processor_timeout <config:dag_processor__dag_file_processor_timeout>`.
- The scheduler will mark a task as failed if the task has been queued for longer than :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>`.
- If a :ref:`task instance's heartbeat times out <concepts:task-instance-heartbeat-timeout>`, it will be marked failed by the scheduler.
- A user marked the task as successful or failed in the Airflow UI.
- An external script or process used the :doc:`Airflow REST API <stable-rest-api-ref>` to change the state of a task.

Process terminated by signal
----------------------------

Sometimes, Airflow or some adjacent system will kill a task instance's ``TaskRunner``, causing the task instance to fail.

Below we discuss a few common cases.

Dag run timeout
"""""""""""""""

A dag run timeout can be specified by ``dagrun_timeout`` in the dag's definition.
The task process would likely be killed with SIGTERM (exit code -15).

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
