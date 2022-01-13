#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Marks tasks APIs."""

from datetime import datetime
from typing import TYPE_CHECKING, Iterable, List, Optional

from sqlalchemy import or_
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm.session import Session as SASession

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.subdag import SubDagOperator
from airflow.utils import timezone
from airflow.utils.helpers import exactly_one
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType


def _create_dagruns(
    dag: DAG, execution_dates: List[datetime], state: DagRunState, run_type: DagRunType
) -> List[DagRun]:
    """
    Infers from the dates which dag runs need to be created and does so.

    :param dag: the dag to create dag runs for
    :param execution_dates: list of execution dates to evaluate
    :param state: the state to set the dag run to
    :param run_type: The prefix will be used to construct dag run id: {run_id_prefix}__{execution_date}
    :return: newly created and existing dag runs for the execution dates supplied
    """
    # find out if we need to create any dag runs
    dag_runs = DagRun.find(dag_id=dag.dag_id, execution_date=execution_dates)
    dates_to_create = list(set(execution_dates) - {dag_run.execution_date for dag_run in dag_runs})

    for date in dates_to_create:
        dag_run = dag.create_dagrun(
            execution_date=date,
            start_date=timezone.utcnow(),
            external_trigger=False,
            state=state,
            run_type=run_type,
        )
        dag_runs.append(dag_run)

    return dag_runs


@provide_session
def set_state(
    *,
    tasks: Iterable[BaseOperator],
    dag_run_id: Optional[str] = None,
    execution_date: Optional[datetime] = None,
    upstream: bool = False,
    downstream: bool = False,
    future: bool = False,
    past: bool = False,
    state: TaskInstanceState = TaskInstanceState.SUCCESS,
    commit: bool = False,
    session: SASession = NEW_SESSION,
) -> List[TaskInstance]:
    """
    Set the state of a task instance and if needed its relatives. Can set state
    for future tasks (calculated from run_id) and retroactively
    for past tasks. Will verify integrity of past dag runs in order to create
    tasks that did not exist. It will not create dag runs that are missing
    on the schedule (but it will as for subdag dag runs if needed).

    :param tasks: the iterable of tasks from which to work. task.task.dag needs to be set
    :type tasks: list[airflow.models.baseoperator.BaseOperator]
    :param dag_run_id: the run_id of the dagrun to start looking from
    :type dag_run_id: str
    :param execution_date: the execution date from which to start looking(deprecated)
    :type execution_date: datetime.datetime
    :param upstream: Mark all parents (upstream tasks)
    :type upstream: bool
    :param downstream: Mark all siblings (downstream tasks) of task_id, including SubDags
    :type downstream: bool
    :param future: Mark all future tasks on the interval of the dag up until
        last execution date.
    :type future: bool
    :param past: Retroactively mark all tasks starting from start_date of the DAG
    :type past: bool
    :param state: State to which the tasks need to be set
    :type state: str
    :param commit: Commit tasks to be altered to the database
    :type commit: bool
    :param session: database session
    :type session: sqlalchemy.orm.session.Session
    :return: list of tasks that have been created and updated
    """
    if not tasks:
        return []

    if not exactly_one(execution_date, dag_run_id):
        raise ValueError("Exactly one of dag_run_id and execution_date must be set")

    if execution_date and not timezone.is_localized(execution_date):
        raise ValueError(f"Received non-localized date {execution_date}")

    task_dags = {task.dag for task in tasks}
    if len(task_dags) > 1:
        raise ValueError(f"Received tasks from multiple DAGs: {task_dags}")
    dag = next(iter(task_dags))
    if dag is None:
        raise ValueError("Received tasks with no DAG")

    if execution_date:
        dag_run_id = dag.get_dagrun(execution_date=execution_date).run_id
    if not dag_run_id:
        raise ValueError("Received tasks with no dag_run_id")

    dag_run_ids = get_run_ids(dag, dag_run_id, future, past)

    task_ids = list(find_task_relatives(tasks, downstream, upstream))

    confirmed_dates = verify_dag_run_integrity(dag, dag_run_ids)

    sub_dag_run_ids = get_subdag_runs(dag, session, DagRunState(state), task_ids, commit, confirmed_dates)

    # now look for the task instances that are affected

    qry_dag = get_all_dag_task_query(dag, session, state, task_ids, confirmed_dates)

    if commit:
        tis_altered = qry_dag.with_for_update().all()
        if sub_dag_run_ids:
            qry_sub_dag = all_subdag_tasks_query(sub_dag_run_ids, session, state, confirmed_dates)
            tis_altered += qry_sub_dag.with_for_update().all()
        for task_instance in tis_altered:
            task_instance.set_state(state)
    else:
        tis_altered = qry_dag.all()
        if sub_dag_run_ids:
            qry_sub_dag = all_subdag_tasks_query(sub_dag_run_ids, session, state, confirmed_dates)
            tis_altered += qry_sub_dag.all()
    return tis_altered


def all_subdag_tasks_query(
    sub_dag_run_ids: List[str], session: SASession, state: TaskInstanceState, confirmed_dates: List[datetime]
):
    """Get *all* tasks of the sub dags"""
    qry_sub_dag = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id.in_(sub_dag_run_ids), TaskInstance.execution_date.in_(confirmed_dates))
        .filter(or_(TaskInstance.state.is_(None), TaskInstance.state != state))
    )
    return qry_sub_dag


def get_all_dag_task_query(
    dag: DAG,
    session: SASession,
    state: TaskInstanceState,
    task_ids: List[str],
    confirmed_dates: List[datetime],
):
    """Get all tasks of the main dag that will be affected by a state change"""
    qry_dag = (
        session.query(TaskInstance)
        .join(TaskInstance.dag_run)
        .filter(
            TaskInstance.dag_id == dag.dag_id,
            DagRun.execution_date.in_(confirmed_dates),
            TaskInstance.task_id.in_(task_ids),
        )
        .filter(or_(TaskInstance.state.is_(None), TaskInstance.state != state))
        .options(contains_eager(TaskInstance.dag_run))
    )
    return qry_dag


def get_subdag_runs(
    dag: DAG,
    session: SASession,
    state: DagRunState,
    task_ids: List[str],
    commit: bool,
    confirmed_dates: List[datetime],
) -> List[str]:
    """Go through subdag operators and create dag runs. We will only work
    within the scope of the subdag. We won't propagate to the parent dag,
    but we will propagate from parent to subdag.
    """
    dags = [dag]
    sub_dag_ids = []
    while dags:
        current_dag = dags.pop()
        for task_id in task_ids:
            if not current_dag.has_task(task_id):
                continue

            current_task = current_dag.get_task(task_id)
            if isinstance(current_task, SubDagOperator) or current_task.task_type == "SubDagOperator":
                # this works as a kind of integrity check
                # it creates missing dag runs for subdag operators,
                # maybe this should be moved to dagrun.verify_integrity
                if TYPE_CHECKING:
                    assert current_task.subdag
                dag_runs = _create_dagruns(
                    current_task.subdag,
                    execution_dates=confirmed_dates,
                    state=DagRunState.RUNNING,
                    run_type=DagRunType.BACKFILL_JOB,
                )

                verify_dagruns(dag_runs, commit, state, session, current_task)

                dags.append(current_task.subdag)
                sub_dag_ids.append(current_task.subdag.dag_id)
    return sub_dag_ids


def verify_dagruns(
    dag_runs: List[DagRun],
    commit: bool,
    state: DagRunState,
    session: SASession,
    current_task: BaseOperator,
):
    """Verifies integrity of dag_runs.

    :param dag_runs: dag runs to verify
    :param commit: whether dag runs state should be updated
    :param state: state of the dag_run to set if commit is True
    :param session: session to use
    :param current_task: current task
    :return:
    """
    for dag_run in dag_runs:
        dag_run.dag = current_task.subdag
        dag_run.verify_integrity()
        if commit:
            dag_run.state = state
            session.merge(dag_run)


def verify_dag_run_integrity(dag: DAG, run_ids: List[str]) -> List[datetime]:
    """
    Verify the integrity of the dag runs in case a task was added or removed
    set the confirmed execution dates as they might be different
    from what was provided
    """
    confirmed_dates = []
    dag_runs = DagRun.find(dag_id=dag.dag_id, run_id=run_ids)
    for dag_run in dag_runs:
        dag_run.dag = dag
        dag_run.verify_integrity()
        confirmed_dates.append(dag_run.execution_date)
    return confirmed_dates


def find_task_relatives(tasks, downstream, upstream):
    """Yield task ids and optionally ancestor and descendant ids."""
    for task in tasks:
        yield task.task_id
        if downstream:
            for relative in task.get_flat_relatives(upstream=False):
                yield relative.task_id
        if upstream:
            for relative in task.get_flat_relatives(upstream=True):
                yield relative.task_id


@provide_session
def get_execution_dates(
    dag: DAG, execution_date: datetime, future: bool, past: bool, *, session: SASession = NEW_SESSION
) -> List[datetime]:
    """Returns dates of DAG execution"""
    latest_execution_date = dag.get_latest_execution_date(session=session)
    if latest_execution_date is None:
        raise ValueError(f"Received non-localized date {execution_date}")
    execution_date = timezone.coerce_datetime(execution_date)
    # determine date range of dag runs and tasks to consider
    end_date = latest_execution_date if future else execution_date
    if dag.start_date:
        start_date = dag.start_date
    else:
        start_date = execution_date
    start_date = execution_date if not past else start_date
    if not dag.timetable.can_run:
        # If the DAG never schedules, need to look at existing DagRun if the user wants future or
        # past runs.
        dag_runs = dag.get_dagruns_between(start_date=start_date, end_date=end_date)
        dates = sorted({d.execution_date for d in dag_runs})
    elif not dag.timetable.periodic:
        dates = [start_date]
    else:
        dates = [
            info.logical_date for info in dag.iter_dagrun_infos_between(start_date, end_date, align=False)
        ]
    return dates


@provide_session
def get_run_ids(dag: DAG, run_id: str, future: bool, past: bool, session: SASession = NEW_SESSION):
    """Returns run_ids of DAG execution"""
    last_dagrun = dag.get_last_dagrun(include_externally_triggered=True)
    current_dagrun = dag.get_dagrun(run_id=run_id)
    first_dagrun = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag.dag_id)
        .order_by(DagRun.execution_date.asc())
        .first()
    )

    if last_dagrun is None:
        raise ValueError(f'DagRun for {dag.dag_id} not found')

    # determine run_id range of dag runs and tasks to consider
    end_date = last_dagrun.logical_date if future else current_dagrun.logical_date
    start_date = current_dagrun.logical_date if not past else first_dagrun.logical_date
    if not dag.timetable.can_run:
        # If the DAG never schedules, need to look at existing DagRun if the user wants future or
        # past runs.
        dag_runs = dag.get_dagruns_between(start_date=start_date, end_date=end_date)
        run_ids = sorted({d.run_id for d in dag_runs})
    elif not dag.timetable.periodic:
        run_ids = [run_id]
    else:
        dates = [
            info.logical_date for info in dag.iter_dagrun_infos_between(start_date, end_date, align=False)
        ]
        run_ids = [dr.run_id for dr in DagRun.find(dag_id=dag.dag_id, execution_date=dates)]
    return run_ids


def _set_dag_run_state(dag_id: str, dag_run_id: str, state: DagRunState, session: SASession = NEW_SESSION):
    """
    Helper method that set dag run state in the DB.

    :param dag_id: dag_id of target dag run
    :param dag_run_id: dag run id of target dag run
    :param state: target state
    :param session: database session
    """
    dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id).one()
    dag_run.state = state
    if state == State.RUNNING:
        dag_run.start_date = timezone.utcnow()
        dag_run.end_date = None
    else:
        dag_run.end_date = timezone.utcnow()
    session.merge(dag_run)


@provide_session
def set_dag_run_state_to_success(
    *,
    dag: DAG,
    execution_date: Optional[datetime] = None,
    run_id: Optional[str] = None,
    commit: bool = False,
    session: SASession = NEW_SESSION,
):
    """
    Set the dag run for a specific execution date and its task instances
    to success.

    :param dag: the DAG of which to alter state
    :param execution_date: the execution date from which to start looking(deprecated)
    :param run_id: the run_id to start looking from
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    :raises: ValueError if dag or execution_date is invalid
    """
    if not exactly_one(execution_date, run_id):
        return []

    if not dag:
        return []

    if execution_date:
        if not timezone.is_localized(execution_date):
            raise ValueError(f"Received non-localized date {execution_date}")
        dag_run = dag.get_dagrun(execution_date=execution_date)
        if not dag_run:
            raise ValueError(f'DagRun with execution_date: {execution_date} not found')
        run_id = dag_run.run_id
    if not run_id:
        raise ValueError(f'Invalid dag_run_id: {run_id}')
    # Mark the dag run to success.
    if commit:
        _set_dag_run_state(dag.dag_id, run_id, DagRunState.SUCCESS, session)

    # Mark all task instances of the dag run to success.
    for task in dag.tasks:
        task.dag = dag
    return set_state(tasks=dag.tasks, dag_run_id=run_id, state=State.SUCCESS, commit=commit, session=session)


@provide_session
def set_dag_run_state_to_failed(
    *,
    dag: DAG,
    execution_date: Optional[datetime] = None,
    run_id: Optional[str] = None,
    commit: bool = False,
    session: SASession = NEW_SESSION,
):
    """
    Set the dag run for a specific execution date or run_id and its running task instances
    to failed.

    :param dag: the DAG of which to alter state
    :param execution_date: the execution date from which to start looking(deprecated)
    :param run_id: the DAG run_id to start looking from
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    :raises: AssertionError if dag or execution_date is invalid
    """
    if not exactly_one(execution_date, run_id):
        return []
    if not dag:
        return []

    if execution_date:
        if not timezone.is_localized(execution_date):
            raise ValueError(f"Received non-localized date {execution_date}")
        dag_run = dag.get_dagrun(execution_date=execution_date)
        if not dag_run:
            raise ValueError(f'DagRun with execution_date: {execution_date} not found')
        run_id = dag_run.run_id

    if not run_id:
        raise ValueError(f'Invalid dag_run_id: {run_id}')

    # Mark the dag run to failed.
    if commit:
        _set_dag_run_state(dag.dag_id, run_id, DagRunState.FAILED, session)

    # Mark only RUNNING task instances.
    task_ids = [task.task_id for task in dag.tasks]
    tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag.dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.task_id.in_(task_ids),
        TaskInstance.state.in_(State.running),
    )
    task_ids_of_running_tis = [task_instance.task_id for task_instance in tis]

    tasks = []
    for task in dag.tasks:
        if task.task_id not in task_ids_of_running_tis:
            continue
        task.dag = dag
        tasks.append(task)

    return set_state(tasks=tasks, dag_run_id=run_id, state=State.FAILED, commit=commit, session=session)


@provide_session
def set_dag_run_state_to_running(
    *,
    dag: DAG,
    execution_date: Optional[datetime] = None,
    run_id: Optional[str] = None,
    commit: bool = False,
    session: SASession = NEW_SESSION,
):
    """
    Set the dag run for a specific execution date to running.

    :param dag: the DAG of which to alter state
    :param execution_date: the execution date from which to start looking
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    """
    res: List[BaseOperator] = []

    if not (execution_date is None) ^ (run_id is None):
        return res

    if not dag:
        return res

    if execution_date:

        if not timezone.is_localized(execution_date):
            raise ValueError(f"Received non-localized date {execution_date}")
        dag_run = dag.get_dagrun(execution_date=execution_date)
        if not dag_run:
            raise ValueError(f'DagRun with execution_date: {execution_date} not found')
        run_id = dag_run.run_id
    if not run_id:
        raise ValueError(f'DagRun with run_id: {run_id} not found')
    # Mark the dag run to running.
    if commit:
        _set_dag_run_state(dag.dag_id, run_id, DagRunState.RUNNING, session)

    # To keep the return type consistent with the other similar functions.
    return res
