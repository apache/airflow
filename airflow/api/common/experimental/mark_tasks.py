# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime

from airflow.jobs import BackfillJob
from airflow.models import DagRun, TaskInstance
from airflow.operators.subdag_operator import SubDagOperator
from airflow.settings import Session
from airflow.utils.state import State

from sqlalchemy import or_


def _create_dagruns(dag, execution_dates, state, run_id_template):
    """
    Infers from the dates which dag runs need to be created and does so.
    :param dag: the dag to create dag runs for
    :param execution_dates: list of execution dates to evaluate
    :param state: the state to set the dag run to
    :param run_id_template:the template for run id to be with the execution date
    :return: newly created and existing dag runs for the execution dates supplied
    """
    # find out if we need to create any dag runs
    drs = DagRun.find(dag_id=dag.dag_id, execution_date=execution_dates)
    dates_to_create = list(set(execution_dates) - set([dr.execution_date for dr in drs]))

    for date in dates_to_create:
        dr = dag.create_dagrun(
            run_id=run_id_template.format(date.isoformat()),
            execution_date=date,
            start_date=datetime.datetime.now(),
            external_trigger=False,
            state=state,
        )
        drs.append(dr)

    return drs


def set_state(task, execution_date, upstream=False, downstream=False,
              future=False, past=False, state=State.SUCCESS, commit=False):
    """
    Set the state of a task instance and if needed its relatives. Can set state
    for future tasks (calculated from execution_date) and retroactively
    for past tasks. Will verify integrity of past dag runs in order to create
    tasks that did not exist. It will not create dag runs that are missing
    on the schedule (but it will as for subdag dag runs if needed).
    :param task: the task from which to work. task.task.dag needs to be set
    :param execution_date: the execution date from which to start looking
    :param upstream: Mark all parents (upstream tasks)
    :param downstream: Mark all siblings (downstream tasks) of task_id, including SubDags
    :param future: Mark all future tasks on the interval of the dag up until
        last execution date.
    :param past: Retroactively mark all tasks starting from start_date of the DAG
    :param state: State to which the tasks need to be set
    :param commit: Commit tasks to be altered to the database
    :return: list of tasks that have been created and updated
    """
    assert isinstance(execution_date, datetime.datetime)

    # microseconds are supported by the database, but is not handled
    # correctly by airflow on e.g. the filesystem and in other places
    execution_date = execution_date.replace(microsecond=0)

    assert task.dag is not None
    dag = task.dag

    latest_execution_date = dag.latest_execution_date
    assert latest_execution_date is not None

    # determine date range of dag runs and tasks to consider
    end_date = latest_execution_date if future else execution_date

    if 'start_date' in dag.default_args:
        start_date = dag.default_args['start_date']
    elif dag.start_date:
        start_date = dag.start_date
    else:
        start_date = execution_date

    start_date = execution_date if not past else start_date

    if dag.schedule_interval == '@once':
        dates = [start_date]
    else:
        dates = dag.date_range(start_date=start_date, end_date=end_date)

    # find relatives (siblings = downstream, parents = upstream) if needed
    task_ids = [task.task_id]
    if downstream:
        relatives = task.get_flat_relatives(upstream=False)
        task_ids += [t.task_id for t in relatives]
    if upstream:
        relatives = task.get_flat_relatives(upstream=True)
        task_ids += [t.task_id for t in relatives]

    # verify the integrity of the dag runs in case a task was added or removed
    # set the confirmed execution dates as they might be different
    # from what was provided
    confirmed_dates = []
    drs = DagRun.find(dag_id=dag.dag_id, execution_date=dates)
    for dr in drs:
        dr.dag = dag
        dr.verify_integrity()
        confirmed_dates.append(dr.execution_date)

    # go through subdagoperators and create dag runs. We will only work
    # within the scope of the subdag. We wont propagate to the parent dag,
    # but we will propagate from parent to subdag.
    session = Session()
    dags = [dag]
    sub_dag_ids = []
    while len(dags) > 0:
        current_dag = dags.pop()
        for task_id in task_ids:
            if not current_dag.has_task(task_id):
                continue

            current_task = current_dag.get_task(task_id)
            if isinstance(current_task, SubDagOperator):
                # this works as a kind of integrity check
                # it creates missing dag runs for subdagoperators,
                # maybe this should be moved to dagrun.verify_integrity
                drs = _create_dagruns(current_task.subdag,
                                      execution_dates=confirmed_dates,
                                      state=State.RUNNING,
                                      run_id_template=BackfillJob.ID_FORMAT_PREFIX)

                for dr in drs:
                    dr.dag = current_task.subdag
                    dr.verify_integrity()
                    if commit:
                        dr.state = state
                        session.merge(dr)

                dags.append(current_task.subdag)
                sub_dag_ids.append(current_task.subdag.dag_id)

    # now look for the task instances that are affected
    TI = TaskInstance

    # get all tasks of the main dag that will be affected by a state change
    qry_dag = session.query(TI).filter(
        TI.dag_id==dag.dag_id,
        TI.execution_date.in_(confirmed_dates),
        TI.task_id.in_(task_ids)).filter(
        or_(TI.state.is_(None),
            TI.state != state)
    )

    # get *all* tasks of the sub dags
    if len(sub_dag_ids) > 0:
        qry_sub_dag = session.query(TI).filter(
            TI.dag_id.in_(sub_dag_ids),
            TI.execution_date.in_(confirmed_dates)).filter(
            or_(TI.state.is_(None),
                TI.state != state)
        )

    if commit:
        tis_altered = qry_dag.with_for_update().all()
        if len(sub_dag_ids) > 0:
            tis_altered += qry_sub_dag.with_for_update().all()
        for ti in tis_altered:
            ti.state = state
        session.commit()
    else:
        tis_altered = qry_dag.all()
        if len(sub_dag_ids) > 0:
            tis_altered += qry_sub_dag.all()

    session.close()

    return tis_altered

