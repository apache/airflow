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
This module is deprecated. Please use :mod:`airflow.utils.task_group`.

The module which provides a way to nest your DAGs and so your levels of complexity.
"""
from __future__ import annotations

import warnings
from datetime import datetime
from enum import Enum

from sqlalchemy.orm.session import Session

from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning, TaskInstanceNotFound
from airflow.models import DagRun
from airflow.models.dag import DAG, DagContext
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType


class SkippedStatePropagationOptions(Enum):
    """Available options for skipped state propagation of subdag's tasks to parent dag tasks."""

    ALL_LEAVES = "all_leaves"
    ANY_LEAF = "any_leaf"


class SubDagOperator(BaseSensorOperator):
    """
    This class is deprecated, please use :class:`airflow.utils.task_group.TaskGroup`.

    This runs a sub dag. By convention, a sub dag's dag_id
    should be prefixed by its parent and a dot. As in `parent.child`.
    Although SubDagOperator can occupy a pool/concurrency slot,
    user can specify the mode=reschedule so that the slot will be
    released periodically to avoid potential deadlock.

    :param subdag: the DAG object to run as a subdag of the current DAG.
    :param session: sqlalchemy session
    :param conf: Configuration for the subdag
    :param propagate_skipped_state: by setting this argument you can define
        whether the skipped state of leaf task(s) should be propagated to the
        parent dag's downstream task.
    """

    ui_color = "#555"
    ui_fgcolor = "#fff"

    subdag: DAG

    @provide_session
    def __init__(
        self,
        *,
        subdag: DAG,
        session: Session = NEW_SESSION,
        conf: dict | None = None,
        propagate_skipped_state: SkippedStatePropagationOptions | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.subdag = subdag
        self.conf = conf
        self.propagate_skipped_state = propagate_skipped_state

        self._validate_dag(kwargs)
        self._validate_pool(session)

        warnings.warn(
            """This class is deprecated. Please use `airflow.utils.task_group.TaskGroup`.""",
            RemovedInAirflow3Warning,
            stacklevel=4,
        )

    def _validate_dag(self, kwargs):
        dag = kwargs.get("dag") or DagContext.get_current_dag()

        if not dag:
            raise AirflowException("Please pass in the `dag` param or call within a DAG context manager")

        if dag.dag_id + "." + kwargs["task_id"] != self.subdag.dag_id:
            raise AirflowException(
                f"The subdag's dag_id should have the form '{{parent_dag_id}}.{{this_task_id}}'. "
                f"Expected '{dag.dag_id}.{kwargs['task_id']}'; received '{self.subdag.dag_id}'."
            )

    def _validate_pool(self, session):
        if self.pool:
            conflicts = [t for t in self.subdag.tasks if t.pool == self.pool]
            if conflicts:
                # only query for pool conflicts if one may exist
                pool = session.query(Pool).filter(Pool.slots == 1).filter(Pool.pool == self.pool).first()
                if pool and any(t.pool == self.pool for t in self.subdag.tasks):
                    raise AirflowException(
                        f"SubDagOperator {self.task_id} and subdag task{'s' if len(conflicts) > 1 else ''} "
                        f"{', '.join(t.task_id for t in conflicts)} both use pool {self.pool}, "
                        f"but the pool only has 1 slot. The subdag tasks will never run."
                    )

    def _get_dagrun(self, execution_date):
        dag_runs = DagRun.find(
            dag_id=self.subdag.dag_id,
            execution_date=execution_date,
        )
        return dag_runs[0] if dag_runs else None

    def _reset_dag_run_and_task_instances(self, dag_run: DagRun, execution_date: datetime) -> None:
        """Set task instance states to allow for execution.

        The state of the DAG run will be set to RUNNING, and failed task
        instances to ``None`` for scheduler to pick up.

        :param dag_run: DAG run to reset.
        :param execution_date: Execution date to select task instances.
        """
        with create_session() as session:
            dag_run.state = DagRunState.RUNNING
            session.merge(dag_run)
            failed_task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == self.subdag.dag_id)
                .filter(TaskInstance.execution_date == execution_date)
                .filter(TaskInstance.state.in_((TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED)))
            )

            for task_instance in failed_task_instances:
                task_instance.state = None
                session.merge(task_instance)
            session.commit()

    def pre_execute(self, context):
        super().pre_execute(context)
        execution_date = context["execution_date"]
        dag_run = self._get_dagrun(execution_date)

        if dag_run is None:
            if context["data_interval_start"] is None or context["data_interval_end"] is None:
                data_interval: tuple[datetime, datetime] | None = None
            else:
                data_interval = (context["data_interval_start"], context["data_interval_end"])
            dag_run = self.subdag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=execution_date,
                state=DagRunState.RUNNING,
                conf=self.conf,
                external_trigger=True,
                data_interval=data_interval,
            )
            self.log.info("Created DagRun: %s", dag_run.run_id)
        else:
            self.log.info("Found existing DagRun: %s", dag_run.run_id)
            if dag_run.state == DagRunState.FAILED:
                self._reset_dag_run_and_task_instances(dag_run, execution_date)

    def poke(self, context: Context):
        execution_date = context["execution_date"]
        dag_run = self._get_dagrun(execution_date=execution_date)
        return dag_run.state != DagRunState.RUNNING

    def post_execute(self, context, result=None):
        super().post_execute(context)
        execution_date = context["execution_date"]
        dag_run = self._get_dagrun(execution_date=execution_date)
        self.log.info("Execution finished. State is %s", dag_run.state)

        if dag_run.state != DagRunState.SUCCESS:
            raise AirflowException(f"Expected state: SUCCESS. Actual state: {dag_run.state}")

        if self.propagate_skipped_state and self._check_skipped_states(context):
            self._skip_downstream_tasks(context)

    def _check_skipped_states(self, context):
        leaves_tis = self._get_leaves_tis(context["execution_date"])

        if self.propagate_skipped_state == SkippedStatePropagationOptions.ANY_LEAF:
            return any(ti.state == TaskInstanceState.SKIPPED for ti in leaves_tis)
        if self.propagate_skipped_state == SkippedStatePropagationOptions.ALL_LEAVES:
            return all(ti.state == TaskInstanceState.SKIPPED for ti in leaves_tis)
        raise AirflowException(
            f"Unimplemented SkippedStatePropagationOptions {self.propagate_skipped_state} used."
        )

    def _get_leaves_tis(self, execution_date):
        leaves_tis = []
        for leaf in self.subdag.leaves:
            try:
                ti = get_task_instance(
                    dag_id=self.subdag.dag_id, task_id=leaf.task_id, execution_date=execution_date
                )
                leaves_tis.append(ti)
            except TaskInstanceNotFound:
                continue
        return leaves_tis

    def _skip_downstream_tasks(self, context):
        self.log.info(
            "Skipping downstream tasks because propagate_skipped_state is set to %s "
            "and skipped task(s) were found.",
            self.propagate_skipped_state,
        )

        downstream_tasks = context["task"].downstream_list
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(
                context["dag_run"],
                context["execution_date"],
                downstream_tasks,
                map_index=context["ti"].map_index,
            )

        self.log.info("Done.")
