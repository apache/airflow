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
