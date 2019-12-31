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

The Airflow scheduler monitors all tasks and all DAGs and triggers the
Task instances whose dependencies have been met. Behind the scenes,
it spins up a subprocess, which monitors and stays in sync with a folder
for all DAG objects it may contain, and periodically (every minute or so)
collects DAG parsing results and inspects active tasks to see whether
they can be triggered.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute the ``airflow scheduler`` command. It uses the configuration specified in
``airflow.cfg``.

The scheduler uses the configured :doc:`Executor </executor/index>` to run tasks that are ready.

To start a scheduler, simply run the command:

.. code:: bash

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

    You should refer :doc:`dag-run` for details on scheduling a DAG.
