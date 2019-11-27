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

"""Clears task instances"""
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskreschedule import TaskReschedule
from airflow.utils import timezone
from airflow.utils.state import State


def clear_selected_task_instances(
          dag: DAG,
          task_id: str,
          start_date: Optional[datetime] = None,
          end_date: Optional[datetime] = None,
          upstream: bool = False,
          downstream: bool = False,
          session: Session = None) -> int:
    """
    Clears the state of task instances associated with the task, following
    the parameters specified.
    """
    qry = session.query(TaskInstance).filter(TaskInstance.dag_id == dag.dag_id)

    if start_date:
        qry = qry.filter(TaskInstance.execution_date >= start_date)
    if end_date:
        qry = qry.filter(TaskInstance.execution_date <= end_date)

    tasks = [task_id]

    if upstream:
        tasks += [
            t.task_id for t in self.get_flat_relatives(upstream=True)]

    if downstream:
        tasks += [
            t.task_id for t in self.get_flat_relatives(upstream=False)]

    qry = qry.filter(TaskInstance.task_id.in_(tasks))  # type: ignore
    count = qry.count()
    clear_task_instances(qry.all(), session, dag=dag)
    session.commit()
    return count


def clear_task_instances(tis: List[TaskInstance],
                         session: Session,
                         activate_dag_runs: bool = True,
                         dag: Optional[DAG] = None,
                         ):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids: List[int] = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the last attempted try number.
                ti.max_tries = max(ti.max_tries, ti.prev_attempted_tries)
            ti.state = State.NONE
            session.merge(ti)
        # Clear all reschedules related to the ti to clear
        session.query(TaskReschedule).filter(
            TaskReschedule.dag_id == ti.dag_id,
            TaskReschedule.task_id == ti.task_id,
            TaskReschedule.execution_date == ti.execution_date,
            TaskReschedule.try_number == ti.try_number
        ).delete()
    from airflow.jobs import BaseJob  # to avoid cyclic import

    if job_ids:
        for job in session.query(BaseJob).filter(BaseJob.id.in_(job_ids)).all():  # type: ignore
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        from airflow.models.dagrun import DagRun
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),  # type: ignore
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),  # type: ignore
        ).all()
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()
