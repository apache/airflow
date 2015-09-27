The Scheduler
=============

The Airflow scheduler monitors all tasks and all DAGs and schedules the
task instances whose dependencies have been met. Behind the scenes,
it monitors a folder for all DAG objects it may contain,
and periodically inspects all tasks to see whether it can schedule the
next run.

Note that if you run a DAG on a ``schedule_interval`` of one day,
the run stamped ``2016-01-01`` will be trigger soon after ``2016-01-01T23:59``.
In other words, the job instance is started once the period it covers
has ended.

The scheduler starts an instance of the executor specified in the your
``airflow.cfg``. If it happens to be the LocalExecutor, tasks will be
executed as subprocesses; in the case of CeleryExecutor and MesosExecutor, tasks are
executed remotely.

To start a scheduler, simply run the command:

.. code:: bash

    airflow scheduler

Note that:

* It **won't parallelize** multiple instances of the same tasks; it always wait for the previous schedule to be done before moving forward
* It will **not fill in gaps**; it only moves forward in time from the latest task instance on that task
* If a task instance failed and the task is set to ``depends_on_past=True``, it won't move forward from that point until the error state is cleared and the task runs successfully, or is marked as successful
* If no task history exists for a task, it will attempt to run it on the task's ``start_date``

Understanding this, you should be able to comprehend what is keeping your
tasks from running or moving forward. To allow the scheduler to move forward, you may want to clear the state of some task instances, or mark them as successful.

Here are some of the ways you can **unblock tasks**:

* From the UI, you can **clear** (as in delete the status of) individual task instances from the task instances dialog, while defining whether you want to includes the past/future and the upstream/downstream dependencies. Note that a confirmation window comes next and allows you to see the set you are about to clear.
* The CLI command ``airflow clear -h`` has lots of options when it comes to clearing task instance states, including specifying date ranges, targeting task_ids by specifying a regular expression, flags for including upstream and downstream relatives, and targeting task instances in specific states (``failed``, or ``success``)
* Marking task instances as successful can be done through the UI. This is mostly to fix false negatives, or for instance when the fix has been applied outside of Airflow.
* The ``airflow backfill`` CLI subcommand has a flag to ``--mark_success`` and allows selecting subsections of the DAG as well as specifying date ranges.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute ``airflow scheduler``. It will use the configuration specified in
``airflow.cfg``.
