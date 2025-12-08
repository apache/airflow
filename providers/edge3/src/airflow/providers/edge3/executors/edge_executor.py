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

import contextlib
from collections.abc import Sequence
from copy import deepcopy
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, inspect, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import Session

from airflow.cli.cli_config import GroupCommand
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.sdk import Stats, timezone
from airflow.providers.edge3.cli.edge_command import EDGE_COMMANDS
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, EdgeWorkerState, reset_metrics
from airflow.utils.db import DBLocks, create_global_lock
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    import argparse

    from sqlalchemy.engine.base import Engine

    from airflow.models.taskinstancekey import TaskInstanceKey

    # TODO: Airflow 2 type hints; remove when Airflow 2 support is removed
    CommandType = Sequence[str]
    # Task tuple to send to be executed
    TaskTuple = tuple[TaskInstanceKey, CommandType, str | None, Any | None]

PARALLELISM: int = conf.getint("core", "PARALLELISM")
DEFAULT_QUEUE: str = conf.get_mandatory_value("operators", "default_queue")


class EdgeExecutor(BaseExecutor):
    """Implementation of the EdgeExecutor to distribute work to Edge Workers via HTTP."""

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        self.last_reported_state: dict[TaskInstanceKey, TaskInstanceState] = {}

    def _check_db_schema(self, engine: Engine) -> None:
        """
        Check if already existing table matches the newest table schema.

        workaround as Airflow 2.x had no support for provider DB migrations,
        then it is possible to use alembic also for provider distributions.

        TODO(jscheffl): Change to alembic DB migrations in the future.
        """
        inspector = inspect(engine)
        edge_job_columns = None
        edge_job_command_len = None
        with contextlib.suppress(NoSuchTableError):
            edge_job_schema = inspector.get_columns("edge_job")
            edge_job_columns = [column["name"] for column in edge_job_schema]
            for column in edge_job_schema:
                if column["name"] == "command":
                    edge_job_command_len = column["type"].length  # type: ignore[attr-defined]

        # version 0.6.0rc1 added new column concurrency_slots
        if edge_job_columns and "concurrency_slots" not in edge_job_columns:
            EdgeJobModel.metadata.drop_all(engine, tables=[EdgeJobModel.__table__])

        # version 1.1.0 the command column was changed to VARCHAR(2048)
        elif edge_job_command_len and edge_job_command_len != 2048:
            with Session(engine) as session:
                query = "ALTER TABLE edge_job ALTER COLUMN command TYPE VARCHAR(2048);"
                session.execute(text(query))
                session.commit()

        edge_worker_columns = None
        with contextlib.suppress(NoSuchTableError):
            edge_worker_columns = [column["name"] for column in inspector.get_columns("edge_worker")]

        # version 0.14.0pre0 added new column maintenance_comment
        if edge_worker_columns and "maintenance_comment" not in edge_worker_columns:
            with Session(engine) as session:
                query = "ALTER TABLE edge_worker ADD maintenance_comment VARCHAR(1024);"
                session.execute(text(query))
                session.commit()

    @provide_session
    def start(self, session: Session = NEW_SESSION):
        """If EdgeExecutor provider is loaded first time, ensure table exists."""
        with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
            engine = session.get_bind().engine
            self._check_db_schema(engine)
            EdgeJobModel.metadata.create_all(engine)
            EdgeLogsModel.metadata.create_all(engine)
            EdgeWorkerModel.metadata.create_all(engine)

    def _process_tasks(self, task_tuples: list[TaskTuple]) -> None:
        """
        Temporary overwrite of _process_tasks function.

        Idea is to not change the interface of the execute_async function in BaseExecutor as it will be changed in Airflow 3.
        Edge worker needs task_instance in execute_async but BaseExecutor deletes this out of the self.queued_tasks.
        Store queued_tasks in own var to be able to access this in execute_async function.
        """
        self.edge_queued_tasks = deepcopy(self.queued_tasks)
        super()._process_tasks(task_tuples)  # type: ignore[misc]

    @provide_session
    def queue_workload(
        self,
        workload: workloads.All,
        session: Session = NEW_SESSION,
    ) -> None:
        """Put new workload to queue. Airflow 3 entry point to execute a task."""
        if not isinstance(workload, workloads.ExecuteTask):
            raise TypeError(f"Don't know how to queue workload of type {type(workload).__name__}")

        task_instance = workload.ti
        key = task_instance.key

        # Check if job already exists with same dag_id, task_id, run_id, map_index, try_number
        existing_job = (
            session.query(EdgeJobModel)
            .filter_by(
                dag_id=key.dag_id,
                task_id=key.task_id,
                run_id=key.run_id,
                map_index=key.map_index,
                try_number=key.try_number,
            )
            .first()
        )

        if existing_job:
            existing_job.state = TaskInstanceState.QUEUED
            existing_job.queue = task_instance.queue
            existing_job.concurrency_slots = task_instance.pool_slots
            existing_job.command = workload.model_dump_json()
        else:
            session.add(
                EdgeJobModel(
                    dag_id=key.dag_id,
                    task_id=key.task_id,
                    run_id=key.run_id,
                    map_index=key.map_index,
                    try_number=key.try_number,
                    state=TaskInstanceState.QUEUED,
                    queue=task_instance.queue,
                    concurrency_slots=task_instance.pool_slots,
                    command=workload.model_dump_json(),
                )
            )

    def _check_worker_liveness(self, session: Session) -> bool:
        """Reset worker state if heartbeat timed out."""
        changed = False
        heartbeat_interval: int = conf.getint("edge", "heartbeat_interval")
        lifeless_workers: list[EdgeWorkerModel] = (
            session.query(EdgeWorkerModel)
            .with_for_update(skip_locked=True)
            .filter(
                EdgeWorkerModel.state.not_in(
                    [
                        EdgeWorkerState.UNKNOWN,
                        EdgeWorkerState.OFFLINE,
                        EdgeWorkerState.OFFLINE_MAINTENANCE,
                    ]
                ),
                EdgeWorkerModel.last_update < (timezone.utcnow() - timedelta(seconds=heartbeat_interval * 5)),
            )
            .all()
        )

        for worker in lifeless_workers:
            changed = True
            #  If the worker dies in maintenance mode we want to remember it, so it can start in maintenance mode
            worker.state = (
                EdgeWorkerState.OFFLINE_MAINTENANCE
                if worker.state
                in (
                    EdgeWorkerState.MAINTENANCE_MODE,
                    EdgeWorkerState.MAINTENANCE_PENDING,
                    EdgeWorkerState.MAINTENANCE_REQUEST,
                )
                else EdgeWorkerState.UNKNOWN
            )
            reset_metrics(worker.worker_name)

        return changed

    def _update_orphaned_jobs(self, session: Session) -> bool:
        """Update status ob jobs when workers die and don't update anymore."""
        heartbeat_interval: int = conf.getint("scheduler", "task_instance_heartbeat_timeout")
        lifeless_jobs: list[EdgeJobModel] = (
            session.query(EdgeJobModel)
            .with_for_update(skip_locked=True)
            .filter(
                EdgeJobModel.state == TaskInstanceState.RUNNING,
                EdgeJobModel.last_update < (timezone.utcnow() - timedelta(seconds=heartbeat_interval)),
            )
            .all()
        )

        for job in lifeless_jobs:
            ti = TaskInstance.get_task_instance(
                dag_id=job.dag_id,
                run_id=job.run_id,
                task_id=job.task_id,
                map_index=job.map_index,
                session=session,
            )
            job.state = ti.state if ti and ti.state else TaskInstanceState.REMOVED

            if job.state != TaskInstanceState.RUNNING:
                # Edge worker does not backport emitted Airflow metrics, so export some metrics
                # Export metrics as failed as these jobs will be deleted in the future
                tags = {
                    "dag_id": job.dag_id,
                    "task_id": job.task_id,
                    "queue": job.queue,
                    "state": str(TaskInstanceState.FAILED),
                }
                Stats.incr(
                    f"edge_worker.ti.finish.{job.queue}.{TaskInstanceState.FAILED}.{job.dag_id}.{job.task_id}",
                    tags=tags,
                )
                Stats.incr("edge_worker.ti.finish", tags=tags)

        return bool(lifeless_jobs)

    def _purge_jobs(self, session: Session) -> bool:
        """Clean finished jobs."""
        purged_marker = False
        job_success_purge = conf.getint("edge", "job_success_purge")
        job_fail_purge = conf.getint("edge", "job_fail_purge")
        jobs: list[EdgeJobModel] = (
            session.query(EdgeJobModel)
            .with_for_update(skip_locked=True)
            .filter(
                EdgeJobModel.state.in_(
                    [
                        TaskInstanceState.RUNNING,
                        TaskInstanceState.SUCCESS,
                        TaskInstanceState.FAILED,
                        TaskInstanceState.REMOVED,
                        TaskInstanceState.RESTARTING,
                        TaskInstanceState.UP_FOR_RETRY,
                    ]
                )
            )
            .all()
        )

        # Sync DB with executor otherwise runs out of sync in multi scheduler deployment
        already_removed = self.running - set(job.key for job in jobs)
        self.running = self.running - already_removed

        for job in jobs:
            if job.key in self.running:
                if job.state == TaskInstanceState.RUNNING:
                    if (
                        job.key not in self.last_reported_state
                        or self.last_reported_state[job.key] != job.state
                    ):
                        self.running_state(job.key)
                    self.last_reported_state[job.key] = job.state
                elif job.state == TaskInstanceState.SUCCESS:
                    if job.key in self.last_reported_state:
                        del self.last_reported_state[job.key]
                    self.success(job.key)
                elif job.state in [
                    TaskInstanceState.FAILED,
                    TaskInstanceState.RESTARTING,
                    TaskInstanceState.UP_FOR_RETRY,
                ]:
                    if job.key in self.last_reported_state:
                        del self.last_reported_state[job.key]
                    self.fail(job.key)
                else:
                    self.last_reported_state[job.key] = TaskInstanceState(job.state)
            if (
                job.state == TaskInstanceState.SUCCESS
                and job.last_update_t < (datetime.now() - timedelta(minutes=job_success_purge)).timestamp()
            ) or (
                job.state
                in (
                    TaskInstanceState.FAILED,
                    TaskInstanceState.REMOVED,
                    TaskInstanceState.RESTARTING,
                    TaskInstanceState.UP_FOR_RETRY,
                )
                and job.last_update_t < (datetime.now() - timedelta(minutes=job_fail_purge)).timestamp()
            ):
                if job.key in self.last_reported_state:
                    del self.last_reported_state[job.key]
                purged_marker = True
                session.delete(job)
                session.execute(
                    delete(EdgeLogsModel).where(
                        EdgeLogsModel.dag_id == job.dag_id,
                        EdgeLogsModel.run_id == job.run_id,
                        EdgeLogsModel.task_id == job.task_id,
                        EdgeLogsModel.map_index == job.map_index,
                        EdgeLogsModel.try_number == job.try_number,
                    )
                )

        return purged_marker

    @provide_session
    def sync(self, session: Session = NEW_SESSION) -> None:
        """Sync will get called periodically by the heartbeat method."""
        with Stats.timer("edge_executor.sync.duration"):
            orphaned = self._update_orphaned_jobs(session)
            purged = self._purge_jobs(session)
            liveness = self._check_worker_liveness(session)
            if purged or liveness or orphaned:
                session.commit()

    def end(self) -> None:
        """End the executor."""
        self.log.info("Shutting down EdgeExecutor")

    def terminate(self):
        """Terminate the executor is not doing anything."""

    @provide_session
    def revoke_task(self, *, ti: TaskInstance, session: Session = NEW_SESSION):
        """
        Revoke a task instance from the executor.

        This method removes the task from the executor's internal state and deletes
        the corresponding EdgeJobModel record to prevent edge workers from picking it up.

        :param ti: Task instance to revoke
        :param session: Database session
        """
        # Remove from executor's internal state
        self.running.discard(ti.key)
        self.queued_tasks.pop(ti.key, None)
        if ti.key in self.last_reported_state:
            del self.last_reported_state[ti.key]

        # Delete the job from the database to prevent edge workers from picking it up
        session.execute(
            delete(EdgeJobModel).where(
                EdgeJobModel.dag_id == ti.dag_id,
                EdgeJobModel.task_id == ti.task_id,
                EdgeJobModel.run_id == ti.run_id,
                EdgeJobModel.map_index == ti.map_index,
                EdgeJobModel.try_number == ti.try_number,
            )
        )
        self.log.info("Revoked task instance %s from EdgeExecutor", ti.key)

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

        Anything that is not adopted will be cleared by the scheduler (and then become eligible for
        re-scheduling)

        :return: any TaskInstances that were unable to be adopted
        """
        # We handle all running tasks from the DB in sync, no adoption logic needed.
        return []

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        return [
            GroupCommand(
                name="edge",
                help="Edge Worker components",
                description=(
                    "Start and manage Edge Worker. Works only when using EdgeExecutor. For more information, "
                    "see https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/edge_executor.html"
                ),
                subcommands=EDGE_COMMANDS,
            ),
        ]


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return EdgeExecutor._get_parser()
