Scheduling & Triggers
=====================

The Airflow scheduler monitors all tasks and all DAGs, and triggers the
task instances whose dependencies have been met. Behind the scenes,
it monitors and stays in sync with a folder for all DAG objects it may contain,
and periodically (every minute or so) inspects active tasks to see whether
they can be triggered.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute ``airflow scheduler``. It will use the configuration specified in
``airflow.cfg``.

Note that if you run a DAG on a ``schedule_interval`` of one day,
the run stamped ``2016-01-01`` will be trigger soon after ``2016-01-01T23:59``.
In other words, the job instance is started once the period it covers
has ended.

The scheduler starts an instance of the executor specified in the your
``airflow.cfg``. If it happens to be the ``LocalExecutor``, tasks will be
executed as subprocesses; in the case of ``CeleryExecutor`` and
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
[cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression) as
a ``str``, or a ``datetime.timedelta`` object. Alternatively, you can also
use one of these cron "preset":

+--------------+----------------------------------------------------------------+---------------+
| preset       | Run once a year at midnight of January 1                       | cron          |
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


Your DAG will be instantiated
for each schedule, while creating a ``DAG Run`` entry for each schedule.

DAG runs have a state associated to them (running, failed, success) and
informs the scheduler on which set of schedules should be evaluated for
task submissions. Without the metadata at the DAG run level, the Airflow
scheduler would have much more work to do in order to figure out what tasks
should be triggered and come to a crawl. It might also create undesired
processing when changing the shape of your DAG, by say adding in new
tasks.

External Triggers
'''''''''''''''''

Note that ``DAG Runs`` can also be created manually through the CLI while
running an ``airflow trigger_dag`` command, where you can define a
specific ``run_id``. The ``DAG Runs`` created externally to the
scheduler get associated to the trigger's timestamp, and will be displayed
in the UI alongside scheduled ``DAG runs``.


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

* From the UI, you can **clear** (as in delete the status of) individual task instances from the task instances dialog, while defining whether you want to includes the past/future and the upstream/downstream dependencies. Note that a confirmation window comes next and allows you to see the set you are about to clear.
* The CLI command ``airflow clear -h`` has lots of options when it comes to clearing task instance states, including specifying date ranges, targeting task_ids by specifying a regular expression, flags for including upstream and downstream relatives, and targeting task instances in specific states (``failed``, or ``success``)
* Marking task instances as successful can be done through the UI. This is mostly to fix false negatives, or for instance when the fix has been applied outside of Airflow.
* The ``airflow backfill`` CLI subcommand has a flag to ``--mark_success`` and allows selecting subsections of the DAG as well as specifying date ranges.

