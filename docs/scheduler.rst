..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Scheduling & Triggers
=====================

The Airflow scheduler monitors all tasks and all DAGs, and triggers the
task instances whose dependencies have been met. Behind the scenes,
it spins up a subprocess, which monitors and stays in sync with a folder
for all DAG objects it may contain, and periodically (every minute or so)
collects DAG parsing results and inspects active tasks to see whether
they can be triggered.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute ``airflow scheduler``. It will use the configuration specified in
``airflow.cfg``.

Note that if you run a DAG on a ``schedule_interval`` of one day,
the run stamped ``2016-01-01`` will be triggered soon after ``2016-01-01T23:59``.
In other words, the job instance is started once the period it covers
has ended.

**Let's Repeat That** The scheduler runs your job one ``schedule_interval`` AFTER the
start date, at the END of the period.

The scheduler starts an instance of the executor specified in the your
``airflow.cfg``. If it happens to be the ``LocalExecutor``, tasks will be
executed as subprocesses; in the case of ``CeleryExecutor``, ``DaskExecutor``, and
``MesosExecutor``, tasks are executed remotely.

To start a scheduler, simply run the command:

.. code:: bash

    airflow scheduler


DAG Runs
''''''''

A DAG Run is an object representing an instantiation of the DAG in time.

Each DAG may or may not have a schedule, which informs how ``DAG Runs`` are
created. ``schedule_interval`` is defined as a DAG arguments, and receives
preferably a
`cron expression <https://en.wikipedia.org/wiki/Cron#CRON_expression>`_ as
a ``str``, or a ``datetime.timedelta`` object. Alternatively, you can also
use one of these cron "preset":

+--------------+----------------------------------------------------------------+---------------+
| preset       | meaning                                                        | cron          |
+==============+================================================================+===============+
| ``None``     | Don't schedule, use for exclusively "externally triggered"     |               |
|              | DAGs                                                           |               |
+--------------+----------------------------------------------------------------+---------------+
| ``@once``    | Schedule once and only once                                    |               |
+--------------+----------------------------------------------------------------+---------------+
| ``@hourly``  | Run once an hour at the beginning of the hour                  | ``0 * * * *`` |
+--------------+----------------------------------------------------------------+---------------+
| ``@daily``   | Run once a day at midnight                                     | ``0 0 * * *`` |
+--------------+----------------------------------------------------------------+---------------+
| ``@weekly``  | Run once a week at midnight on Sunday morning                  | ``0 0 * * 0`` |
+--------------+----------------------------------------------------------------+---------------+
| ``@monthly`` | Run once a month at midnight of the first day of the month     | ``0 0 1 * *`` |
+--------------+----------------------------------------------------------------+---------------+
| ``@yearly``  | Run once a year at midnight of January 1                       | ``0 0 1 1 *`` |
+--------------+----------------------------------------------------------------+---------------+

**Note**: Use ``schedule_interval=None`` and not ``schedule_interval='None'`` when
you don't want to schedule your DAG.

Your DAG will be instantiated
for each schedule, while creating a ``DAG Run`` entry for each schedule.

DAG runs have a state associated to them (running, failed, success) and
informs the scheduler on which set of schedules should be evaluated for
task submissions. Without the metadata at the DAG run level, the Airflow
scheduler would have much more work to do in order to figure out what tasks
should be triggered and come to a crawl. It might also create undesired
processing when changing the shape of your DAG, by say adding in new
tasks.

Backfill and Catchup
''''''''''''''''''''

An Airflow DAG with a ``start_date``, possibly an ``end_date``, and a ``schedule_interval`` defines a
series of intervals which the scheduler turn into individual Dag Runs and execute. A key capability of
Airflow is that these DAG Runs are atomic, idempotent items, and the scheduler, by default, will examine
the lifetime of the DAG (from start to end/now, one interval at a time) and kick off a DAG Run for any
interval that has not been run (or has been cleared). This concept is called Catchup.

If your DAG is written to handle its own catchup (IE not limited to the interval, but instead to "Now"
for instance.), then you will want to turn catchup off (Either on the DAG itself with ``dag.catchup =
False``) or by default at the configuration file level with ``catchup_by_default = False``. What this
will do, is to instruct the scheduler to only create a DAG Run for the most current instance of the DAG
interval series.

.. code:: python

    """
    Code that goes along with the Airflow tutorial located at:
    https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
    """
    from airflow import DAG
    from airflow.operators.bash_operator import BashOperator
    from datetime import datetime, timedelta


    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2015, 12, 1),
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }

    dag = DAG(
        'tutorial',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval='@daily',
        catchup=False)

In the example above, if the DAG is picked up by the scheduler daemon on 2016-01-02 at 6 AM, (or from the
command line), a single DAG Run will be created, with an ``execution_date`` of 2016-01-01, and the next
one will be created just after midnight on the morning of 2016-01-03 with an execution date of 2016-01-02.

If the ``dag.catchup`` value had been True instead, the scheduler would have created a DAG Run for each
completed interval between 2015-12-01 and 2016-01-02 (but not yet one for 2016-01-02, as that interval
hasn't completed) and the scheduler will execute them sequentially. This behavior is great for atomic
datasets that can easily be split into periods. Turning catchup off is great if your DAG Runs perform
backfill internally.

External Triggers
'''''''''''''''''

Note that ``DAG Runs`` can also be created manually through the CLI while
running an ``airflow trigger_dag`` command, where you can define a
specific ``run_id``. The ``DAG Runs`` created externally to the
scheduler get associated to the trigger's timestamp, and will be displayed
in the UI alongside scheduled ``DAG runs``.

In addition, you can also manually trigger a ``DAG Run`` using the web UI (tab "DAGs" -> column "Links" -> button "Trigger Dag").


To Keep in Mind
'''''''''''''''
* The first ``DAG Run`` is created based on the minimum ``start_date`` for the
  tasks in your DAG.
* Subsequent ``DAG Runs`` are created by the scheduler process, based on
  your DAG's ``schedule_interval``, sequentially.
* When clearing a set of tasks' state in hope of getting them to re-run,
  it is important to keep in mind the ``DAG Run``'s state too as it defines
  whether the scheduler should look into triggering tasks for that run.

Here are some of the ways you can **unblock tasks**:

* From the UI, you can **clear** (as in delete the status of) individual task instances
  from the task instances dialog, while defining whether you want to includes the past/future
  and the upstream/downstream dependencies. Note that a confirmation window comes next and
  allows you to see the set you are about to clear. You can also clear all task instances
  associated with the dag.
* The CLI command ``airflow clear -h`` has lots of options when it comes to clearing task instance
  states, including specifying date ranges, targeting task_ids by specifying a regular expression,
  flags for including upstream and downstream relatives, and targeting task instances in specific
  states (``failed``, or ``success``)
* Clearing a task instance will no longer delete the task instance record. Instead it updates
  max_tries and set the current task instance state to be None.
* Marking task instances as failed can be done through the UI. This can be used to stop running task instances.
* Marking task instances as successful can be done through the UI. This is mostly to fix false negatives,
  or for instance when the fix has been applied outside of Airflow.
* The ``airflow backfill`` CLI subcommand has a flag to ``--mark_success`` and allows selecting
  subsections of the DAG as well as specifying date ranges.

