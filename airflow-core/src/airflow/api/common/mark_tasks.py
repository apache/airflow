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

from __future__ import annotations

from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import lazyload

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session as SASession

    from airflow.models.dag import DAG
    from airflow.models.operator import Operator


@provide_session
def set_state(
    *,
    tasks: Collection[Operator | tuple[Operator, int]],
    run_id: str | None = None,
    upstream: bool = False,
    downstream: bool = False,
    future: bool = False,
    past: bool = False,
    state: TaskInstanceState = TaskInstanceState.SUCCESS,
    commit: bool = False,
    session: SASession = NEW_SESSION,
) -> list[TaskInstance]:
    """
    Set the state of a task instance and if needed its relatives.

    Can set state for future tasks (calculated from run_id) and retroactively
    for past tasks. Will verify integrity of past dag runs in order to create
    tasks that did not exist. It will not create dag runs that are missing
    on the schedule.

    :param tasks: the iterable of tasks or (task, map_index) tuples from which to work.
        ``task.dag`` needs to be set
    :param run_id: the run_id of the dagrun to start looking from
    :param upstream: Mark all parents (upstream tasks)
    :param downstream: Mark all siblings (downstream tasks) of task_id
    :param future: Mark all future tasks on the interval of the dag up until
        last logical date.
    :param past: Retroactively mark all tasks starting from start_date of the DAG
    :param state: State to which the tasks need to be set
    :param commit: Commit tasks to be altered to the database
    :param session: database session
    :return: list of tasks that have been created and updated
    """
    if not tasks:
        return []

    task_dags = {task[0].dag if isinstance(task, tuple) else task.dag for task in tasks}
    if len(task_dags) > 1:
        raise ValueError(f"Received tasks from multiple DAGs: {task_dags}")
    dag = next(iter(task_dags))
    if dag is None:
        raise ValueError("Received tasks with no DAG")
    if not run_id:
        raise ValueError("Received tasks with no run_id")

    if TYPE_CHECKING:
        assert isinstance(dag, DAG)

    dag_run_ids = get_run_ids(dag, run_id, future, past, session=session)
    task_id_map_index_list = list(find_task_relatives(tasks, downstream, upstream))
    # now look for the task instances that are affected

    qry_dag = get_all_dag_task_query(dag, state, task_id_map_index_list, dag_run_ids)

    if commit:
        tis_altered = session.scalars(qry_dag.with_for_update()).all()
        for task_instance in tis_altered:
            task_instance.set_state(state, session=session)
        session.flush()
    else:
        tis_altered = session.scalars(qry_dag).all()
    return tis_altered


def get_all_dag_task_query(
    dag: DAG,
    state: TaskInstanceState,
    task_ids: list[str | tuple[str, int]],
    run_ids: Iterable[str],
):
    """Get all tasks of the main dag that will be affected by a state change."""
    qry_dag = select(TaskInstance).where(
        TaskInstance.dag_id == dag.dag_id,
        TaskInstance.run_id.in_(run_ids),
        TaskInstance.ti_selector_condition(task_ids),
    )

    qry_dag = qry_dag.where(or_(TaskInstance.state.is_(None), TaskInstance.state != state)).options(
        lazyload(TaskInstance.dag_run)
    )
    return qry_dag


def find_task_relatives(tasks, downstream, upstream):
    """Yield task ids and optionally ancestor and descendant ids."""
    for item in tasks:
        if isinstance(item, tuple):
            task, map_index = item
            yield task.task_id, map_index
        else:
            task = item
            yield task.task_id
        if downstream:
            for relative in task.get_flat_relatives(upstream=False):
                yield relative.task_id
        if upstream:
            for relative in task.get_flat_relatives(upstream=True):
                yield relative.task_id


@provide_session
def get_run_ids(dag: DAG, run_id: str, future: bool, past: bool, session: SASession = NEW_SESSION):
    """Return DAG executions' run_ids."""
    current_dagrun = dag.get_dagrun(run_id=run_id, session=session)
    if current_dagrun.logical_date is None:
        return [run_id]

    last_dagrun = dag.get_last_dagrun(include_manually_triggered=True, session=session)
    first_dagrun = session.scalar(
        select(DagRun)
        .where(DagRun.dag_id == dag.dag_id, DagRun.logical_date.is_not(None))
        .order_by(DagRun.logical_date.asc())
        .limit(1)
    )
    if last_dagrun is None:
        raise ValueError(f"DagRun for {dag.dag_id} not found")

    # determine run_id range of dag runs and tasks to consider
    end_date = last_dagrun.logical_date if future else current_dagrun.logical_date
    start_date = current_dagrun.logical_date if not past else first_dagrun.logical_date
    if not dag.timetable.can_be_scheduled:
        # If the DAG never schedules, need to look at existing DagRun if the user wants future or
        # past runs.
        dag_runs = dag.get_dagruns_between(start_date=start_date, end_date=end_date, session=session)
        run_ids = sorted({d.run_id for d in dag_runs})
    elif not dag.timetable.periodic:
        run_ids = [run_id]
    else:
        dates = [
            info.logical_date for info in dag.iter_dagrun_infos_between(start_date, end_date, align=False)
        ]
        run_ids = [dr.run_id for dr in DagRun.find(dag_id=dag.dag_id, logical_date=dates, session=session)]
    return run_ids


def _set_dag_run_state(dag_id: str, run_id: str, state: DagRunState, session: SASession):
    """
    Set dag run state in the DB.

    :param dag_id: dag_id of target dag run
    :param run_id: run id of target dag run
    :param state: target state
    :param session: database session
    """
    dag_run = session.execute(
        select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
    ).scalar_one()
    dag_run.state = state
    session.merge(dag_run)


@provide_session
def set_dag_run_state_to_success(
    *,
    dag: DAG,
    run_id: str | None = None,
    commit: bool = False,
    session: SASession = NEW_SESSION,
) -> list[TaskInstance]:
    """
    Set the dag run's state to success.

    Set for a specific logical date and its task instances to success.

    :param dag: the DAG of which to alter state
    :param run_id: the run_id to start looking from
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    :raises: ValueError if dag or logical_date is invalid
    """
    if not dag:
        return []
    if not run_id:
        raise ValueError(f"Invalid dag_run_id: {run_id}")

    # Mark all task instances of the dag run to success - except for teardown as they need to complete work.
    normal_tasks = [task for task in dag.tasks if not task.is_teardown]

    # Mark the dag run to success.
    if commit and len(normal_tasks) == len(dag.tasks):
        _set_dag_run_state(dag.dag_id, run_id, DagRunState.SUCCESS, session)

    for task in normal_tasks:
        task.dag = dag
    return set_state(
        tasks=normal_tasks,
        run_id=run_id,
        state=TaskInstanceState.SUCCESS,
        commit=commit,
        session=session,
    )


@provide_session
def set_dag_run_state_to_failed(
    *,
    dag: DAG,
    run_id: str | None = None,
    commit: bool = False,
    session: SASession = NEW_SESSION,
) -> list[TaskInstance]:
    """
    Set the dag run's state to failed.

    Set for a specific logical date and its task instances to failed.

    :param dag: the DAG of which to alter state
    :param run_id: the DAG run_id to start looking from
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    """
    if not dag:
        return []
    if not run_id:
        raise ValueError(f"Invalid dag_run_id: {run_id}")

    running_states = (
        TaskInstanceState.RUNNING,
        TaskInstanceState.DEFERRED,
        TaskInstanceState.UP_FOR_RESCHEDULE,
    )

    # Mark only RUNNING task instances.
    task_ids = [task.task_id for task in dag.tasks]
    running_tis: list[TaskInstance] = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id.in_(task_ids),
            TaskInstance.state.in_(running_states),
        )
    ).all()

    # Do not kill teardown tasks
    task_ids_of_running_tis = [ti.task_id for ti in running_tis if not dag.task_dict[ti.task_id].is_teardown]

    running_tasks = []
    for task in dag.tasks:
        if task.task_id in task_ids_of_running_tis:
            task.dag = dag
            running_tasks.append(task)

    # Mark non-finished tasks as SKIPPED.
    pending_tis: list[TaskInstance] = session.scalars(
        select(TaskInstance).filter(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.run_id == run_id,
            or_(
                TaskInstance.state.is_(None),
                and_(
                    TaskInstance.state.not_in(State.finished),
                    TaskInstance.state.not_in(running_states),
                ),
            ),
        )
    ).all()

    # Do not skip teardown tasks
    pending_normal_tis = [ti for ti in pending_tis if not dag.task_dict[ti.task_id].is_teardown]

    if commit:
        for ti in pending_normal_tis:
            ti.set_state(TaskInstanceState.SKIPPED)

        # Mark the dag run to failed if there is no pending teardown (else this would not be scheduled later).
        if not any(dag.task_dict[ti.task_id].is_teardown for ti in (running_tis + pending_tis)):
            _set_dag_run_state(dag.dag_id, run_id, DagRunState.FAILED, session)

    return pending_normal_tis + set_state(
        tasks=running_tasks,
        run_id=run_id,
        state=TaskInstanceState.FAILED,
        commit=commit,
        session=session,
    )


def __set_dag_run_state_to_running_or_queued(
    *,
    new_state: DagRunState,
    dag: DAG,
    run_id: str | None = None,
    commit: bool = False,
    session: SASession,
) -> list[TaskInstance]:
    """
    Set the dag run for a specific logical date to running.

    :param dag: the DAG of which to alter state
    :param run_id: the id of the DagRun
    :param commit: commit DAG and tasks to be altered to the database
    :param session: database session
    :return: If commit is true, list of tasks that have been updated,
             otherwise list of tasks that will be updated
    """
    res: list[TaskInstance] = []
    if not dag:
        return res
    if not run_id:
        raise ValueError(f"DagRun with run_id: {run_id} not found")
    # Mark the dag run to running.
    if commit:
        _set_dag_run_state(dag.dag_id, run_id, new_state, session)

    # To keep the return type consistent with the other similar functions.
    return res


@provide_session
def set_dag_run_state_to_queued(
    *,
    dag: DAG,
    run_id: str | None = None,
    commit: bool = False,
    session: SASession = NEW_SESSION,
) -> list[TaskInstance]:
    """
    Set the dag run's state to queued.

    Set for a specific logical date and its task instances to queued.
    """
    return __set_dag_run_state_to_running_or_queued(
        new_state=DagRunState.QUEUED,
        dag=dag,
        run_id=run_id,
        commit=commit,
        session=session,
    )
