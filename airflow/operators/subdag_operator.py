# -*- coding: utf-8 -*-
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

from sqlalchemy.orm.session import Session
from typing import Optional

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import create_session, provide_session


class SubDagOperator(BaseSensorOperator):
    """
    This runs a sub dag. By convention, a sub dag's dag_id
    should be prefixed by its parent and a dot. As in `parent.child`.

    Although SubDagOperator can occupy a pool/concurrency slot,
    user can specify the mode=reschedule so that the slot will be
    released periodically to avoid potential deadlock.

    :param subdag: the DAG object to run as a subdag of the current DAG.
    :param session: sqlalchemy session
    """

    ui_color = '#555'
    ui_fgcolor = '#fff'

    @provide_session
    @apply_defaults
    def __init__(
            self,
            subdag: DAG,
            session: Optional[Session] = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.subdag = subdag
        dag = kwargs.get('dag') or settings.CONTEXT_MANAGER_DAG
        if not dag:
            raise AirflowException('Please pass in the `dag` param or call '
                                   'within a DAG context manager')

        # validate subdag name
        if dag.dag_id + '.' + kwargs['task_id'] != subdag.dag_id:
            raise AirflowException(
                "The subdag's dag_id should have the form "
                "'{{parent_dag_id}}.{{this_task_id}}'. Expected "
                "'{d}.{t}'; received '{rcvd}'.".format(
                    d=dag.dag_id, t=kwargs['task_id'], rcvd=subdag.dag_id))

        # validate that subdag operator and subdag tasks don't have a
        # pool conflict
        if self.pool:
            conflicts = [t for t in subdag.tasks if t.pool == self.pool]
            if conflicts:
                # only query for pool conflicts if one may exist
                pool = (
                    session  # type: ignore
                    .query(Pool)
                    .filter(Pool.slots == 1)
                    .filter(Pool.pool == self.pool)
                    .first()
                )
                if pool and any(t.pool == self.pool for t in subdag.tasks):
                    raise AirflowException(
                        'SubDagOperator {sd} and subdag task{plural} {t} both '
                        'use pool {p}, but the pool only has 1 slot. The '
                        'subdag tasks will never run.'.format(
                            sd=self.task_id,
                            plural=len(conflicts) > 1,
                            t=', '.join(t.task_id for t in conflicts),
                            p=self.pool
                        )
                    )

    def _get_dagrun(self, execution_date):
        dag_runs = DagRun.find(
            dag_id=self.subdag.dag_id,
            execution_date=execution_date,
        )
        return dag_runs[0] if dag_runs else None

    def _reset_dag_run_and_task_instances(self, dag_run, execution_date):
        """
        Set the DagRun state to RUNNING and set the failed TaskInstances to None state
        for scheduler to pick up.

        :param dag_run: DAG run
        :param execution_date: Execution date
        :return: None
        """
        with create_session() as session:
            dag_run.state = State.RUNNING
            session.merge(dag_run)
            failed_task_instances = (
                session
                .query(TaskInstance)
                .filter(TaskInstance.dag_id == self.subdag.dag_id)
                .filter(TaskInstance.execution_date == execution_date)
                .filter(TaskInstance.state.in_([State.FAILED, State.UPSTREAM_FAILED]))
            )

            for task_instance in failed_task_instances:
                task_instance.state = State.NONE
                session.merge(task_instance)
            session.commit()

    def pre_execute(self, context):
        execution_date = context['execution_date']
        dag_run = self._get_dagrun(execution_date)
        if dag_run is None:
            dag_run = self.subdag.create_dagrun(
                run_id="scheduled__{}".format(execution_date.isoformat()),
                execution_date=execution_date,
                state=State.RUNNING,
                external_trigger=True,
            )

            self.log.info("Created DagRun: %s", dag_run.run_id)
        else:
            self.log.info("Found existing DagRun: %s", dag_run.run_id)
            if dag_run.state == State.FAILED:
                self._reset_dag_run_and_task_instances(dag_run, execution_date)

    def poke(self, context):
        execution_date = context['execution_date']
        dag_run = self._get_dagrun(execution_date=execution_date)
        return dag_run.state != State.RUNNING

    def post_execute(self, context, result=None):
        execution_date = context['execution_date']
        dag_run = self._get_dagrun(execution_date=execution_date)
        self.log.info("Execution finished. State is %s", dag_run.state)

        if dag_run.state != State.SUCCESS:
            raise AirflowException(
                "Expected state: SUCCESS. Actual state: {}".format(dag_run.state))
