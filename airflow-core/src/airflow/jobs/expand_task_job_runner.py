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
from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job
from airflow.models import DagRun
from airflow.models.dag_version import DagVersion
from airflow.models.taskinstance import TaskInstance
from airflow.policies import task_instance_mutation_hook
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import create_session, NEW_SESSION
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

task_expansion_batch_size = conf.getint("scheduler", "task_expansion_batch_size", fallback=10)


def get_dag_run(dag_id: str, run_id: str, session: Session) -> DagRun | None:
    """
    Returns the TaskInstance for the task that is being expanded.
    """
    return session.scalars(
        select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id,
        )
    ).one_or_none()


class TaskExpansionJobRunner(BaseJobRunner, LoggingMixin):
    job_type = "TaskExpansionJob"

    def __init__(
        self,
        job: Job,
        task: MappedOperator,
        run_id: str,
        dag_version_id: DagVersion,
    ) -> None:
        super().__init__(job)
        self.job.dag_id = task.dag_id
        self.job.job_type = self.job_type
        self.task = task
        self.task.operator_class = import_string(f"{task._task_module}.{task._task_type}")
        self.run_id = run_id
        self.dag_version_id = dag_version_id

    @property
    def job_id(self) -> str:
        return self.job.id

    @property
    def dag_id(self) -> str:
        return self.task.dag_id

    @property
    def task_id(self) -> str:
        return self.task.task_id

    def expand_input(self, session: Session) -> Iterator[dict]:
        self.log.info("expand_input: %s", self.task.expand_input)
        context = {
            "task": self.task,
            "run_id": self.run_id,
        }
        return iter(self.task.expand_input.resolve(context, session))

    def expand_task(self, task_instance: TaskInstance, mapped_kwargs) -> TaskInstance:
        self.log.info("expand task: %s", task_instance.map_index)
        self.log.debug("unmap (%s): %s", self.task.operator_class, mapped_kwargs)

        operator = self.task.unmap(mapped_kwargs)

        self.log.info("operator (%s): %s", type(operator), operator)

        task_instance.task = operator
        task_instance_mutation_hook(task_instance)

        self.log.info("creating ti %s: (%s) %s", task_instance.map_index, task_instance.id, task_instance)

        return task_instance

    def _check_dag_run_state(self, dag_run: DagRun) -> None:
        if dag_run:
            self.log.info("dag_run_state: %s", dag_run.state)

            if dag_run.state == DagRunState.FAILED:
                self.log.info("DagRun %s for dag %s has failed, stopping expansion", self.run_id, self.dag_id)

                raise AirflowException(
                    f"Stopping expansion of tasks for DagRun {self.run_id} of DAG {self.dag_id} due to failure."
                )

    def _persist_task_instances(
        self, dag_run: DagRun, task_instances: list[TaskInstance], session: Session
    ) -> None:
        """
        Expands the task using the provided expand_input.
        """
        from airflow.models.taskmap import TaskMap

        if dag_run and task_instances:
            self.log.info("Persisting %d new task instances", len(task_instances))
            dag_run.task_instances.extend(task_instances)
            session.merge(dag_run)
            TaskMap.update_task_map_length(
                length=task_instances[-1].map_index + 1,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=dag_run.run_id,
                session=session,
            )
            session.flush()
            task_instances.clear()

    def expand_tasks(self, expand_input: Iterator[dict], job_id: str | None = None, session: Session = NEW_SESSION) -> list[TaskInstance]:
        """
        Expands the task using the provided expand_input.
        """
        max_map_index = TaskInstance.get_current_max_mapping(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            session=session,
        )
        dag_run = get_dag_run(dag_id=self.dag_id, run_id=self.run_id, session=session)
        unmapped_ti = TaskInstance.get_task_instance(dag_id=self.dag_id, task_id=self.task_id, run_id=self.run_id, session=session)

        self.log.info("expand_tasks: %s", session)
        self.log.info("max_map_index: %s", max_map_index)
        self.log.info("dag_version_id: %s", self.dag_version_id)
        self.log.info("dag_run: %s", dag_run)

        task_instances = []
        task_instances_batch = []

        for map_index, mapped_kwargs in enumerate(expand_input):
            if map_index > max_map_index:
                if map_index == 0 and unmapped_ti:
                    task_instance = unmapped_ti
                    task_instance.map_index = map_index
                else:
                    task_instance = TaskInstance(
                        task=self.task,
                        run_id=self.run_id,
                        map_index=map_index,
                        dag_version_id=self.dag_version_id,
                    )
                if job_id:
                    task_instance.queued_by_job_id = job_id
                task_instance = self.expand_task(task_instance, mapped_kwargs)
                task_instances.append(task_instance)
                task_instances_batch.append(task_instance)

                if len(task_instances_batch) == task_expansion_batch_size:
                    dag_run = get_dag_run(dag_id=self.dag_id, run_id=self.run_id, session=session)
                    self._check_dag_run_state(dag_run)
                    self._persist_task_instances(dag_run, task_instances_batch, session=session)

        self._persist_task_instances(dag_run, task_instances_batch, session=session)

        return task_instances

    def _execute(self) -> int | None:
        with create_session() as session:
            return len(self.expand_tasks(expand_input=self.expand_input(session=session), job_id=self.job_id, session=session))
