from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job
from airflow.models import DagRun
from airflow.models.dag_version import DagVersion
from airflow.models.expandinput import SchedulerDictOfListsExpandInput
from airflow.models.taskinstance import TaskInstance, get_current_max_mapping, get_task_instance
from airflow.models.xcom_arg import SchedulerPlainXComArg
from airflow.policies import task_instance_mutation_hook
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions._internal.mixins import ResolveMixin
from airflow.sdk.definitions._internal.types import NOTSET
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.execution_time.comms import XComResult
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

task_expansion_batch_size = conf.getint("scheduler", "task_expansion_batch_size", fallback=10)


def get_dag_run(dag_id: str, run_id: str, session: Session) -> DagRun | None:
    """
    Returns the TaskInstance for the task that is being expanded.
    """
    return session.scalars(
        select(DagRun)
        .where(
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
        self.task = task
        self.task.operator_class = import_string(f"{task._task_module}.{task._task_type}")
        self.run_id = run_id
        self.dag_version_id = dag_version_id

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

                raise AirflowException(f"Stopping expansion of tasks for DagRun {self.run_id} of DAG {self.dag_id} due to failure.")

    def _persist_task_instances(self, dag_run: DagRun, task_instances: list[TaskInstance], session: Session) -> None:
        if dag_run and task_instances:
            self.log.info("Persisting %d new task instances", len(task_instances))
            dag_run.task_instances.extend(task_instances)
            session.merge(dag_run)
            session.flush()
            session.commit()
            task_instances.clear()

    def expand_tasks(self, session: Session) -> int:
        """
        Expands the task using the provided expand_input.
        """
        counter = 0
        max_map_index = get_current_max_mapping(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            session=session,
        )
        dag_run = get_dag_run(dag_id=self.dag_id, run_id=self.run_id, session=session)

        self.log.info("expand_tasks: %s", session)
        self.log.info("max_map_index: %s", max_map_index)
        self.log.info("dag_version_id: %s", self.dag_version_id)
        self.log.info("dag_run: %s", dag_run)

        task_instances_batch = []

        for map_index, mapped_kwargs in enumerate(self.expand_input(session=session)):
            if map_index > max_map_index:
                task_instance = TaskInstance(
                    task=self.task,
                    run_id=self.run_id,
                    map_index=map_index,
                    dag_version_id=self.dag_version_id,
                )
                task_instances_batch.append(self.expand_task(task_instance, mapped_kwargs))
                counter += 1

                if len(task_instances_batch) ==  task_expansion_batch_size:
                    dag_run = get_dag_run(dag_id=self.dag_id, run_id=self.run_id, session=session)
                    self._check_dag_run_state(dag_run)
                    self._persist_task_instances(dag_run, task_instances_batch, session=session)

        self._persist_task_instances(dag_run, task_instances_batch, session=session)

        return counter

    def _execute(self) -> int | None:
        with create_session() as session:
            return self.expand_tasks(session=session)
