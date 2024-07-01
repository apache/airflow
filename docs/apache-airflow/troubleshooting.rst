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

- If a task's DAG failed to parse on the worker, the scheduler may mark the task as failed. If confirmed, consider increasing :ref:`core.dagbag_import_timeout <config:core__dagbag_import_timeout>` and :ref:`core.dag_file_processor_timeout <config:core__dag_file_processor_timeout>`.
- The scheduler will mark a task as failed if the task has been queued for longer than :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>`.
- If a task becomes a :ref:`zombie <concepts:zombies>`, it will be marked failed by the scheduler.
- A user marked the task as successful or failed in the Airflow UI.
- An external script or process used the :doc:`Airflow REST API <stable-rest-api-ref>` to change the state of a task.

LocalTaskJob killed
-------------------

Sometimes, Airflow or some adjacent system will kill a task instance's ``LocalTaskJob``, causing the task instance to fail.

Here are some examples that could cause such an event:

- A DAG run timeout, specified by ``dagrun_timeout`` in the DAG's definition.
- An Airflow worker running out of memory
  - Usually, Airflow workers that run out of memory receive a SIGKILL and are marked as a zombie and failed by the scheduler. However, in some scenarios, Airflow kills the task before that happens.
