:mod:`airflow.jobs.backfill_job`
================================

.. py:module:: airflow.jobs.backfill_job


Module Contents
---------------

.. py:class:: BackfillJob(dag, start_date=None, end_date=None, mark_success=False, donot_pickle=False, ignore_first_depends_on_past=False, ignore_task_deps=False, pool=None, delay_on_limit_secs=1.0, verbose=False, conf=None, rerun_failed_tasks=False, run_backwards=False, *args, **kwargs)

   Bases: :class:`airflow.jobs.base_job.BaseJob`

   A backfill job consists of a dag or subdag for a specific time range. It
   triggers a set of task instance runs, in the right order and lasts for
   as long as it takes for the set of task instance to be completed.

   .. py:class:: _DagRunTaskStatus(to_run=None, running=None, skipped=None, succeeded=None, failed=None, not_ready=None, deadlocked=None, active_runs=None, executed_dag_run_dates=None, finished_runs=0, total_runs=0)

      Internal status of the backfill job. This class is intended to be instantiated
      only within a BackfillJob instance and will track the execution of tasks,
      e.g. running, skipped, succeeded, failed, etc. Information about the dag runs
      related to the backfill job are also being tracked in this structure,
      .e.g finished runs, etc. Any other status related information related to the
      execution of dag runs / tasks can be included in this structure since it makes
      it easier to pass it around.

      :param to_run: Tasks to run in the backfill
      :type to_run: dict[tuple[TaskInstanceKey], airflow.models.TaskInstance]
      :param running: Maps running task instance key to task instance object
      :type running: dict[tuple[TaskInstanceKey], airflow.models.TaskInstance]
      :param skipped: Tasks that have been skipped
      :type skipped: set[tuple[TaskInstanceKey]]
      :param succeeded: Tasks that have succeeded so far
      :type succeeded: set[tuple[TaskInstanceKey]]
      :param failed: Tasks that have failed
      :type failed: set[tuple[TaskInstanceKey]]
      :param not_ready: Tasks not ready for execution
      :type not_ready: set[tuple[TaskInstanceKey]]
      :param deadlocked: Deadlocked tasks
      :type deadlocked: set[airflow.models.TaskInstance]
      :param active_runs: Active dag runs at a certain point in time
      :type active_runs: list[DagRun]
      :param executed_dag_run_dates: Datetime objects for the executed dag runs
      :type executed_dag_run_dates: set[datetime.datetime]
      :param finished_runs: Number of finished runs so far
      :type finished_runs: int
      :param total_runs: Number of total dag runs able to run
      :type total_runs: int


   .. attribute:: STATES_COUNT_AS_RUNNING
      

      

   .. attribute:: __mapper_args__
      

      

   
   .. method:: _update_counters(self, ti_status, session=None)

      Updates the counters per state of the tasks that were running. Can re-add
      to tasks to run in case required.

      :param ti_status: the internal status of the backfill job tasks
      :type ti_status: BackfillJob._DagRunTaskStatus



   
   .. method:: _manage_executor_state(self, running)

      Checks if the executor agrees with the state of task instances
      that are running

      :param running: dict of key, task to verify



   
   .. method:: _get_dag_run(self, run_date: datetime, dag: DAG, session: Session = None)

      Returns a dag run for the given run date, which will be matched to an existing
      dag run if available or create a new dag run otherwise. If the max_active_runs
      limit is reached, this function will return None.

      :param run_date: the execution date for the dag run
      :param dag: DAG
      :param session: the database session object
      :return: a DagRun in state RUNNING or None



   
   .. method:: _task_instances_for_dag_run(self, dag_run, session=None)

      Returns a map of task instance key to task instance object for the tasks to
      run in the given dag run.

      :param dag_run: the dag run to get the tasks from
      :type dag_run: airflow.models.DagRun
      :param session: the database session object
      :type session: sqlalchemy.orm.session.Session



   
   .. method:: _log_progress(self, ti_status)



   
   .. method:: _process_backfill_task_instances(self, ti_status, executor, pickle_id, start_date=None, session=None)

      Process a set of task instances from a set of dag runs. Special handling is done
      to account for different task instance states that could be present when running
      them in a backfill process.

      :param ti_status: the internal status of the job
      :type ti_status: BackfillJob._DagRunTaskStatus
      :param executor: the executor to run the task instances
      :type executor: BaseExecutor
      :param pickle_id: the pickle_id if dag is pickled, None otherwise
      :type pickle_id: int
      :param start_date: the start date of the backfill job
      :type start_date: datetime.datetime
      :param session: the current session object
      :type session: sqlalchemy.orm.session.Session
      :return: the list of execution_dates for the finished dag runs
      :rtype: list



   
   .. method:: _collect_errors(self, ti_status, session=None)



   
   .. method:: _execute_for_run_dates(self, run_dates, ti_status, executor, pickle_id, start_date, session=None)

      Computes the dag runs and their respective task instances for
      the given run dates and executes the task instances.
      Returns a list of execution dates of the dag runs that were executed.

      :param run_dates: Execution dates for dag runs
      :type run_dates: list
      :param ti_status: internal BackfillJob status structure to tis track progress
      :type ti_status: BackfillJob._DagRunTaskStatus
      :param executor: the executor to use, it must be previously started
      :type executor: BaseExecutor
      :param pickle_id: numeric id of the pickled dag, None if not pickled
      :type pickle_id: int
      :param start_date: backfill start date
      :type start_date: datetime.datetime
      :param session: the current session object
      :type session: sqlalchemy.orm.session.Session



   
   .. method:: _set_unfinished_dag_runs_to_failed(self, dag_runs, session=None)

      Go through the dag_runs and update the state based on the task_instance state.
      Then set DAG runs that are not finished to failed.

      :param dag_runs: DAG runs
      :param session: session
      :return: None



   
   .. method:: _execute(self, session=None)

      Initializes all components required to run a dag for a specified date range and
      calls helper method to execute the tasks.



   
   .. method:: reset_state_for_orphaned_tasks(self, filter_by_dag_run=None, session=None)

      This function checks if there are any tasks in the dagrun (or all) that
      have a schedule or queued states but are not known by the executor. If
      it finds those it will reset the state to None so they will get picked
      up again.  The batch option is for performance reasons as the queries
      are made in sequence.

      :param filter_by_dag_run: the dag_run we want to process, None if all
      :type filter_by_dag_run: airflow.models.DagRun
      :return: the number of TIs reset
      :rtype: int




