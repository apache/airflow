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

import time
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, run_job_async
from airflow.models import DagBag, DagRun
from airflow.policies import task_instance_mutation_hook
from airflow.sdk.definitions.mappedoperator import MappedOperator as TaskSDKMappedOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.dag import DAG
    from airflow.jobs.triggerer_job_runner import TriggerRunnerSupervisor
    from airflow.models.taskinstance import TaskInstance


class TaskExpansionJobRunner(BaseJobRunner, LoggingMixin):
    def __init__(
        self,
        job: Job,
        trigger_runner: TriggerRunnerSupervisor,
    ):
        super().__init__(job)
        self.trigger_runner = trigger_runner

    @property
    def job_id(self) -> str:
        return self.job.id

    def _check_dag_run_state(self, dag_run: DagRun) -> None:
        if dag_run:
            self.log.info("dag_run_state: %s", dag_run.state)

            if dag_run.state == DagRunState.FAILED:
                self.log.info(
                    "DagRun %s for dag %s has failed, stopping expansion", dag_run.run_id, dag_run.dag_id
                )

                raise AirflowException(
                    f"Stopping expansion of tasks for DagRun {dag_run.run_id} of DAG {dag_run.dag_id} due to failure."
                )

    def _persist_task_instances(
        self, dag_run: DagRun, task_instances: list[TaskInstance], session: Session
    ) -> None:
        """
        Expands the task using the provided expand_input.
        """
        if dag_run and task_instances:
            self.log.info("Persisting %d new task instances", len(task_instances))
            dag_run.task_instances.extend(task_instances)
            session.merge(dag_run)
            session.commit()
            task_instances.clear()

    def expand_unmapped_task_instance(
        self, dag_run: DagRun, unmapped_ti: TaskInstance, session: Session
    ) -> None:
        """
        Expands the task using the provided expand_input.
        """
        from airflow.models.taskinstance import TaskInstance

        self.log.info("task: %s", unmapped_ti.task)
        self.log.info("expand_tasks: %s", session)
        self.log.info("dag_version_id: %s", unmapped_ti.dag_version_id)
        self.log.info("dag_run: %s", dag_run)
        self.log.info("unmapped_ti state: %s", unmapped_ti.state)

        task_instances_batch = []

        task_expansion_batch_size = conf.getint("core", "parallelism")
        context = unmapped_ti.get_template_context(session=session)
        expand_input = unmapped_ti.task.expand_input.resolve(context)

        self.log.debug("expand_input: %s", expand_input)

        for map_index, _ in enumerate(expand_input):
            if map_index > 200:
                self.log.warning("Stop expanding tasks over %s!", map_index)
                break

            self.log.debug("map_index: %s", map_index)

            if map_index == 0:
                task_instance = unmapped_ti
                task_instance.map_index = map_index
            else:
                task_instance = TaskInstance(
                    task=unmapped_ti.task,
                    run_id=dag_run.run_id,
                    map_index=map_index,
                    dag_version_id=unmapped_ti.dag_version_id,
                )

            task_instance.queued_by_job_id = self.job_id
            task_instance_mutation_hook(task_instance)
            task_instances_batch.append(task_instance)

            if len(task_instances_batch) == task_expansion_batch_size:
                dag_run = DagRun.get_dag_run(
                    dag_id=unmapped_ti.dag_id, run_id=dag_run.run_id, session=session
                )
                self._check_dag_run_state(dag_run)
                self._persist_task_instances(dag_run, task_instances_batch, session=session)

        self._persist_task_instances(dag_run, task_instances_batch, session=session)

    @staticmethod
    def get_task(dag: DAG, task_instance: TaskInstance) -> TaskInstance:
        task_instance.task = dag.get_task(task_instance.task_id)
        return task_instance

    @staticmethod
    def has_mapped_operator(task_instance: TaskInstance) -> bool:
        return (
            isinstance(task_instance.task, TaskSDKMappedOperator)
            and task_instance.map_index == -1
            and task_instance.state in State.unfinished
        )

    def expand_tasks(self):
        with create_session(scoped=False) as session:
            dag_bag = DagBag()
            dag_runs = DagRun.get_running_dag_runs_to_examine(session=session)

            for dag_run in dag_runs:
                dag = dag_bag.get_dag(dag_run.dag_id)
                self.log.info("Checking for unmapped task instances on: %s", dag_run)
                for unmapped_ti in filter(
                    self.has_mapped_operator,
                    map(lambda task: self.get_task(dag, task), dag_run.task_instances),
                ):
                    try:
                        finished_tis = list(
                            map(
                                lambda task: self.get_task(dag, task),
                                filter(lambda ti: ti.state in State.finished, dag_run.task_instances),
                            )
                        )
                        dep_context = DepContext(
                            flag_upstream_failed=True,
                            ignore_unmapped_tasks=True,  # Ignore this Dep, as we will expand it if we can.
                            finished_tis=finished_tis,
                        )
                        self.log.info("Unmapped task state on: %s", unmapped_ti.state)
                        are_dependencies_met = unmapped_ti.are_dependencies_met(
                            dep_context=dep_context, session=session, verbose=True
                        )
                        self.log.info("Are dependencies met on %s: %s", unmapped_ti, are_dependencies_met)
                        if are_dependencies_met:
                            self.expand_unmapped_task_instance(dag_run, unmapped_ti, session=session)
                    except Exception:
                        self.log.exception(
                            "Unexpected error occurred during task expansion of %s", unmapped_ti
                        )

    def _execute(self) -> int | None:
        self.log.info("TaskExpansionJobRunner started")

        while self.trigger_runner.is_alive():
            self.expand_tasks()
            time.sleep(self.job.heartrate)

        self.log.info("TaskExpansionJobRunner stopped")


def task_expansion_run(triggerer_heartrate: float, trigger_runner: TriggerRunnerSupervisor):
    task_expansion_job_runner = TaskExpansionJobRunner(
        job=Job(heartrate=triggerer_heartrate), trigger_runner=trigger_runner
    )
    run_job_async(job=task_expansion_job_runner.job, execute_callable=task_expansion_job_runner._execute)
