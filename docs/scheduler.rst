The Scheduler
=============

The Airflow scheduler monitors all tasks and all dags and schedules the
task instances whose dependencies have been met. Behinds the scene, 
it monitors a folder for all dag objects it may contain,
and periodically inspects all tasks to see whether is can schedule the
next run.

Note that: 

* It **won't parallelize** multiple instances of the same tasks, it always wait for the previous schedule to be done to move forward
* It will **not fill in gaps**, it only moves forward in time from the latest task instance on that task
* If a task instance failed and the task is set to ``depends_on_past=True``, it won't move forward from that point until the error state is cleared and runs successfully, or is marked as successful
* If no task history exist for a task, it will attempt to run it on the task's ``start_date``

Understanding this, you should be able to comprehend what is keeping your 
tasks from running or moving forward. To allow the scheduler to move forward, you may want to clear the state
of some task instances, or mark them as successful.

Here are some of the ways you can **unblock tasks**:

* From the UI, you can **clear** (as in delete the status of) individual task instances from the tasks instance dialog, while defining whether you want to includes the past/future and the upstream/downstream dependencies. Note that a confirmation window comes next and allows you to see the set you are about to clear.
* The CLI ``airflow clear -h`` has lots of options when it comes to clearing task instances states, including specifying date ranges, targeting task_ids by specifying a regular expression, flags for including upstream and downstream relatives, and targeting task instances in specific states (``failed``, or ``success``)
* Marking task instances as successful can be done through the UI. This is mostly to fix false negatives, or when the fix has been applied outside of Airflow for instance.
* The ``airflow backfill`` CLI subcommand has a flag to ``--mark_success`` and allows to select subsections of the dag as well as specifying date ranges.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is 
execute ``airflow scheduler``. It will use the configuration specified in the
``airflow.cfg``.
