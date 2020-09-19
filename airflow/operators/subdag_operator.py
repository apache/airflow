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
"""
The module which provides a way to nest your DAGs and so your levels of complexity.
"""
from enum import Enum
from typing import Dict, Optional

from sqlalchemy.orm.session import Session

from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.exceptions import AirflowException, TaskInstanceNotFound
from airflow.models import DagRun
from airflow.models.dag import DAG, DagContext
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType


class SkippedStatePropagationOptions(Enum):
    """
    Available options for skipped state propagation of subdag's tasks to parent dag tasks.
    """
    ALL_LEAVES = 'all_leaves'
    ANY_LEAF = 'any_leaf'


class SubDagOperator(BaseSensorOperator):
    """
    This runs a sub dag. By convention, a sub dag's dag_id
    should be prefixed by its parent and a dot. As in `parent.child`.

    Although SubDagOperator can occupy a pool/concurrency slot,
    user can specify the mode=reschedule so that the slot will be
    released periodically to avoid potential deadlock.

    :param subdag: the DAG object to run as a subdag of the current DAG.
    :param session: sqlalchemy session
    :param conf: Configuration for the subdag
    :type conf: dict
    :param propagate_skipped_state: by setting this argument you can define
        whether the skipped state of leaf task(s) should be propagated to the parent dag's downstream task.
    """

    ui_color = '#555'
    ui_fgcolor = '#fff'

    @provide_session
    @apply_defaults
    def __init__(self,
                 *,
                 subdag: DAG,
                 session: Optional[Session] = None,
                 conf: Optional[Dict] = None,
                 propagate_skipped_state: Optional[SkippedStatePropagationOptions] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.subdag = subdag
        self.conf = conf
        self.propagate_skipped_state = propagate_skipped_state

        self._validate_dag(kwargs)
        self._validate_pool(session)

    def _validate_dag(self, kwargs):
        dag = kwargs.get('dag') or DagContext.get_current_dag()

        if not dag:
            raise AirflowException('Please pass in the `dag` param or call within a DAG context manager')

        if dag.dag_id + '.' + kwargs['task_id'] != self.subdag.dag_id:
            raise AirflowException(
                "The subdag's dag_id should have the form '{{parent_dag_id}}.{{this_task_id}}'. "
                "Expected '{d}.{t}'; received '{rcvd}'.".format(
                    d=dag.dag_id, t=kwargs['task_id'], rcvd=self.subdag.dag_id)
            )

    def _validate_pool(self, session):
        if self.pool:
            conflicts = [t for t in self.subdag.tasks if t.pool == self.pool]
            if conflicts:
                # only query for pool conflicts if one may exist
                pool = (session
                        .query(Pool)
                        .filter(Pool.slots == 1)
                        .filter(Pool.pool == self.pool)
                        .first())
                if pool and any(t.pool == self.pool for t in self.subdag.tasks):
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
            failed_task_instances = (session
                                     .query(TaskInstance)
                                     .filter(TaskInstance.dag_id == self.subdag.dag_id)
                                     .filter(TaskInstance.execution_date == execution_date)
                                     .filter(TaskInstance.state.in_([State.FAILED, State.UPSTREAM_FAILED])))

            for task_instance in failed_task_instances:
                task_instance.state = State.NONE
                session.merge(task_instance)
            session.commit()

    def pre_execute(self, context):
        execution_date = context['execution_date']
        dag_run = self._get_dagrun(execution_date)

        if dag_run is None:
            dag_run = self.subdag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=execution_date,
                state=State.RUNNING,
                conf=self.conf,
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
                "Expected state: SUCCESS. Actual state: {}".format(dag_run.state)
            )

        if self.propagate_skipped_state and self._check_skipped_states(context):
            self._skip_downstream_tasks(context)

    def _check_skipped_states(self, context):
        leaves_tis = self._get_leaves_tis(context['execution_date'])

        if self.propagate_skipped_state == SkippedStatePropagationOptions.ANY_LEAF:
            return any(ti.state == State.SKIPPED for ti in leaves_tis)
        if self.propagate_skipped_state == SkippedStatePropagationOptions.ALL_LEAVES:
            return all(ti.state == State.SKIPPED for ti in leaves_tis)
        raise AirflowException(
            'Unimplemented SkippedStatePropagationOptions {} used.'.format(self.propagate_skipped_state))

    def _get_leaves_tis(self, execution_date):
        leaves_tis = []
        for leaf in self.subdag.leaves:
            try:
                ti = get_task_instance(
                    dag_id=self.subdag.dag_id,
                    task_id=leaf.task_id,
                    execution_date=execution_date
                )
                leaves_tis.append(ti)
            except TaskInstanceNotFound:
                continue
        return leaves_tis

    def _skip_downstream_tasks(self, context):
        self.log.info('Skipping downstream tasks because propagate_skipped_state is set to %s '
                      'and skipped task(s) were found.', self.propagate_skipped_state)

        downstream_tasks = context['task'].downstream_list
        self.log.debug('Downstream task_ids %s', downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['execution_date'], downstream_tasks)

        self.log.info('Done.')
