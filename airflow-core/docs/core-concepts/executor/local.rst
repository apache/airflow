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


.. _executor:LocalExecutor:

Local Executor
==============

:class:`~airflow.executors.local_executor.LocalExecutor` runs tasks by spawning processes in a controlled fashion in different modes.

Given that BaseExecutor has the option to receive a ``parallelism`` parameter to limit the number of process spawned,
when this parameter is ``0`` the number of processes that LocalExecutor can spawn is unlimited.

The following strategies are implemented:

- | **Unlimited Parallelism** (``self.parallelism == 0``): In this strategy, LocalExecutor will
  | spawn a process every time ``execute_async`` is called, that is, every task submitted to the
  | :class:`~airflow.executors.local_executor.LocalExecutor` will be executed in its own process. Once the task is executed and the
  | result stored in the ``result_queue``, the process terminates. There is no need for a
  | ``task_queue`` in this approach, since as soon as a task is received a new process will be
  | allocated to the task. Processes used in this strategy are of class :class:`~airflow.executors.local_executor.LocalWorker`.

- | **Limited Parallelism** (``self.parallelism > 0``): In this strategy, the :class:`~airflow.executors.local_executor.LocalExecutor` spawns
  | the number of processes equal to the value of ``self.parallelism`` at ``start`` time,
  | using a ``task_queue`` to coordinate the ingestion of tasks and the work distribution among
  | the workers, which will take a task as soon as they are ready. During the lifecycle of
  | the LocalExecutor, the worker processes are running waiting for tasks, once the
  | LocalExecutor receives the call to shutdown the executor a poison token is sent to the
  | workers to terminate them. Processes used in this strategy are of class :class:`~airflow.executors.local_executor.QueuedLocalWorker`.

.. note::

   When multiple Schedulers are configured with ``executor = LocalExecutor`` in the ``[core]`` section of your ``airflow.cfg``, each Scheduler will run a LocalExecutor. This means tasks would be processed in a distributed fashion across the machines running the Schedulers.

   One consideration should be taken into account:

   - Restarting a Scheduler: If a Scheduler is restarted, it may take some time for other Schedulers to recognize the orphaned tasks and restart or fail them.
