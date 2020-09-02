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

Scheduler
==========

The scheduler is the core component in Airflow that is responsible for monitoring all tasks and DAGs and triggers the task instances once their dependencies are complete. And for this reason, it is imperative to learn about the working of the scheduler.

Scheduling in airflow involves the 3 following components.

1. ``SchedulerJob``
    Responsibilities include starting the executor, creating new process for managing and updating DAG definitions from the file, transitioning the task instances from one state to another and executing them via a configured executor

2. ``DAGFileProcessorManager``
    Responsibilities include listing the files in the DagBag and creating new DAGFileProcessors for parsing DAG definitions on demand

3. ``DAGFileProcessor``
    Responsibilities include parsing the DAG definition in a file, creating the necessary DAG runs and TaskInstances

Logic behind scheduling is as follows:

1. Airflow kicks off the SchedulerJob which in turn creates a DAGFileProcessorManager. The DAGFileProcessorManager enumerates all the files in the DAG directory.

2. The DAGFileProcessorManager creates child processes known as DAGFileProcessor which is responsible for creating the necessary DAG Runs and TaskInstances. When it determines that task instances should run, it updates their state to ``SCHEDULED``. The number of DAGFileProcessor to create is configurable via ``max_threads`` in ``airflow.cfg``.

3. If any of the DAGFileProcessor have finished, another process is created to work on the next file in the series.

4. The cycle is repeated for all the files in the DAG Bag. If the process to parse DAG file is still running when the file's turn comes up in the next cycle, the file is skipped and the next file in the series will be assigned for the new processor. This isolation provides a non-blocking DAG parsing functionality.


TODO: Add sequence diagram. Requires Rebase from upstream

The reason that the task instances are created in the ``SCHEDULED`` state, but then are set to the ``QUEUED`` state once it is sent to the executor, is to ensure that a task instance isn't repeatedly send to the executor if the executor is slow and a DAG definition file is processed multiple times before the executor has a chance to run the task. When the DAGFileProcessor examines a DAG for potential tasks to put into the ``SCHEDULED`` state, it skips those task instances in the ``QUEUED`` state.

Since the scheduler can run indefinitely, it's necessary to periodically refresh the list of files in the DAG definition directory. The refresh interval is controlled with the ``dag_dir_list_interval`` option in ``[scheduler]`` section. In cases where there are only a small number of DAG definition files, the loop could potentially process the DAG definition files many times a minute. To control the rate of DAG file processing, the ``min_file_process_interval`` option in ``[scheduler]`` section can be set to a higher value. This parameter ensures that a DAG definition file is not processed more often than once every ``min_file_process_interval`` seconds.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute the ``airflow scheduler`` command. It uses the configuration specified in
``airflow.cfg``.

The scheduler uses the configured :doc:`Executor </executor/index>` to run tasks that are ready.

To start a scheduler, simply run the command:

.. code-block:: bash

    airflow scheduler

Your DAGs will start executing once the scheduler is running successfully.

.. note::

    The first DAG Run is created based on the minimum ``start_date`` for the tasks in your DAG.
    Subsequent DAG Runs are created by the scheduler process, based on your DAG’s ``schedule_interval``,
    sequentially.


The scheduler won't trigger your tasks until the period it covers has ended e.g., A job with ``schedule_interval`` set as ``@daily`` runs after the day
has ended. This technique makes sure that whatever data is required for that period is fully available before the dag is executed.
In the UI, it appears as if Airflow is running your tasks a day **late**

.. note::

    If you run a DAG on a ``schedule_interval`` of one day, the run with ``execution_date`` ``2019-11-21`` triggers soon after ``2019-11-21T23:59``.

    **Let’s Repeat That**, the scheduler runs your job one ``schedule_interval`` AFTER the start date, at the END of the period.

    You should refer to :doc:`dag-run` for details on scheduling a DAG.

Using pools in tasks
--------------------
If the task instances have pools assigned to them in the DAG definition, the scheduler prioritizes the queued tasks in the pool and tries to run them if there are slots available. This is also applicable to the default pool.

Triggering DAG with Future Date
-------------------------------

If you want to use 'external trigger' to run future-dated execution dates, set ``allow_trigger_in_future = True`` in ``scheduler`` section in ``airflow.cfg``.
This only has effect if your DAG has no ``schedule_interval``.
If you keep default ``allow_trigger_in_future = False`` and try ``external trigger`` to run future-dated execution dates,
the scheduler won't execute it now but the scheduler will execute it in the future once the current date rolls over to the execution date.
