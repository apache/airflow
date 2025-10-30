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

:class:`~airflow.executors.local_executor.LocalExecutor` runs tasks by spawning processes in a controlled fashion on the scheduler node.

The parameter ``parallelism`` limits the number of process spawned not to overwhelm the node.
This parameter must be greater than ``0``.

The :class:`~airflow.executors.local_executor.LocalExecutor` spawns the number of processes equal to the value of ``self.parallelism`` at
``start`` time, using a ``task_queue`` to coordinate the ingestion of tasks and the work distribution among the workers, which will take
a task as soon as they are ready. During the lifecycle of the LocalExecutor, the worker processes are running waiting for tasks, once the
LocalExecutor receives the call to shutdown the executor a poison token is sent to the workers to terminate them. Processes used in this
strategy are of class :class:`~airflow.executors.local_executor.QueuedLocalWorker`.

.. note::

   When multiple Schedulers are configured with ``executor=LocalExecutor`` in the ``[core]`` section of your ``airflow.cfg``, each
   Scheduler will run a LocalExecutor. This means tasks would be processed in a distributed fashion across the machines running the
   Schedulers.

   One consideration should be taken into account:

   - Restarting a Scheduler: If a Scheduler is restarted, it may take some time for other Schedulers to recognize the orphaned tasks
     and restart or fail them.

.. note::

   Previous versions of Airflow had the option to configure the LocalExecutor with unlimited parallelism
   (``self.parallelism = 0``). This option has been removed in Airflow 3.0.0 to avoid overwhelming the scheduler node.
