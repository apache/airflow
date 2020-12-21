:mod:`airflow.api.common.experimental.mark_tasks`
=================================================

.. py:module:: airflow.api.common.experimental.mark_tasks

.. autoapi-nested-parse::

   Marks tasks APIs.



Module Contents
---------------

.. function:: _create_dagruns(dag, execution_dates, state, run_type)
   Infers from the dates which dag runs need to be created and does so.

   :param dag: the dag to create dag runs for
   :param execution_dates: list of execution dates to evaluate
   :param state: the state to set the dag run to
   :param run_type: The prefix will be used to construct dag run id: {run_id_prefix}__{execution_date}
   :return: newly created and existing dag runs for the execution dates supplied


.. function:: set_state(tasks: Iterable[BaseOperator], execution_date: datetime.datetime, upstream: bool = False, downstream: bool = False, future: bool = False, past: bool = False, state: str = State.SUCCESS, commit: bool = False, session=None)
   Set the state of a task instance and if needed its relatives. Can set state
   for future tasks (calculated from execution_date) and retroactively
   for past tasks. Will verify integrity of past dag runs in order to create
   tasks that did not exist. It will not create dag runs that are missing
   on the schedule (but it will as for subdag dag runs if needed).

   :param tasks: the iterable of tasks from which to work. task.task.dag needs to be set
   :param execution_date: the execution date from which to start looking
   :param upstream: Mark all parents (upstream tasks)
   :param downstream: Mark all siblings (downstream tasks) of task_id, including SubDags
   :param future: Mark all future tasks on the interval of the dag up until
       last execution date.
   :param past: Retroactively mark all tasks starting from start_date of the DAG
   :param state: State to which the tasks need to be set
   :param commit: Commit tasks to be altered to the database
   :param session: database session
   :return: list of tasks that have been created and updated


.. function:: all_subdag_tasks_query(sub_dag_run_ids, session, state, confirmed_dates)
   Get *all* tasks of the sub dags


.. function:: get_all_dag_task_query(dag, session, state, task_ids, confirmed_dates)
   Get all tasks of the main dag that will be affected by a state change


.. function:: get_subdag_runs(dag, session, state, task_ids, commit, confirmed_dates)
   Go through subdag operators and create dag runs. We will only work
   within the scope of the subdag. We wont propagate to the parent dag,
   but we will propagate from parent to subdag.


.. function:: verify_dagruns(dag_runs, commit, state, session, current_task)
   Verifies integrity of dag_runs.

   :param dag_runs: dag runs to verify
   :param commit: whether dag runs state should be updated
   :param state: state of the dag_run to set if commit is True
   :param session: session to use
   :param current_task: current task
   :return:


.. function:: verify_dag_run_integrity(dag, dates)
   Verify the integrity of the dag runs in case a task was added or removed
   set the confirmed execution dates as they might be different
   from what was provided


.. function:: find_task_relatives(tasks, downstream, upstream)
   Yield task ids and optionally ancestor and descendant ids.


.. function:: get_execution_dates(dag, execution_date, future, past)
   Returns dates of DAG execution


.. function:: _set_dag_run_state(dag_id, execution_date, state, session=None)
   Helper method that set dag run state in the DB.

   :param dag_id: dag_id of target dag run
   :param execution_date: the execution date from which to start looking
   :param state: target state
   :param session: database session


.. function:: set_dag_run_state_to_success(dag, execution_date, commit=False, session=None)
   Set the dag run for a specific execution date and its task instances
   to success.

   :param dag: the DAG of which to alter state
   :param execution_date: the execution date from which to start looking
   :param commit: commit DAG and tasks to be altered to the database
   :param session: database session
   :return: If commit is true, list of tasks that have been updated,
            otherwise list of tasks that will be updated
   :raises: ValueError if dag or execution_date is invalid


.. function:: set_dag_run_state_to_failed(dag, execution_date, commit=False, session=None)
   Set the dag run for a specific execution date and its running task instances
   to failed.

   :param dag: the DAG of which to alter state
   :param execution_date: the execution date from which to start looking
   :param commit: commit DAG and tasks to be altered to the database
   :param session: database session
   :return: If commit is true, list of tasks that have been updated,
            otherwise list of tasks that will be updated
   :raises: AssertionError if dag or execution_date is invalid


.. function:: set_dag_run_state_to_running(dag, execution_date, commit=False, session=None)
   Set the dag run for a specific execution date to running.

   :param dag: the DAG of which to alter state
   :param execution_date: the execution date from which to start looking
   :param commit: commit DAG and tasks to be altered to the database
   :param session: database session
   :return: If commit is true, list of tasks that have been updated,
            otherwise list of tasks that will be updated


