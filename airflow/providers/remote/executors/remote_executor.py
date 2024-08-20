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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete

from airflow.cli.cli_config import GroupCommand
from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.models.abstractoperator import DEFAULT_QUEUE
from airflow.models.taskinstance import TaskInstanceState
from airflow.providers.remote.cli.remote_command import REMOTE_COMMANDS
from airflow.providers.remote.models import RemoteJobModel
from airflow.providers.remote.models.remote_logs import RemoteLogsModel
from airflow.providers.remote.models.remote_worker import RemoteWorkerModel
from airflow.utils.db import DBLocks, create_global_lock
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    import argparse

    from sqlalchemy.orm import Session

    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

PARALLELISM: int = conf.getint("core", "PARALLELISM")


class RemoteExecutor(BaseExecutor):
    """Implementation of the remote executor to distribute work to remote workers via HTTP."""

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        self.last_reported_state: dict[TaskInstanceKey, TaskInstanceState] = {}

    @provide_session
    def start(self, session: Session = NEW_SESSION):
        """If Remote Executor provider is loaded first time, ensure table exists."""
        with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
            engine = session.get_bind().engine
            RemoteJobModel.metadata.create_all(engine)
            RemoteLogsModel.metadata.create_all(engine)
            RemoteWorkerModel.metadata.create_all(engine)

    @provide_session
    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """Execute asynchronously."""
        self.validate_airflow_tasks_run_command(command)
        session.add(
            RemoteJobModel(
                dag_id=key.dag_id,
                task_id=key.task_id,
                run_id=key.run_id,
                map_index=key.map_index,
                try_number=key.try_number,
                state=TaskInstanceState.QUEUED,
                queue=queue or DEFAULT_QUEUE,
                command=str(command),
            )
        )
        self.queued(key)

    @provide_session
    def sync(self, session: Session = NEW_SESSION) -> None:
        """Sync will get called periodically by the heartbeat method."""
        jobs: list[RemoteJobModel] = session.query(RemoteJobModel).all()
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
                elif job.state == TaskInstanceState.FAILED:
                    if job.key in self.last_reported_state:
                        del self.last_reported_state[job.key]
                    self.fail(job.key)
                else:
                    self.last_reported_state[job.key] = job.state
            if (
                job.state == TaskInstanceState.SUCCESS
                and job.last_update.timestamp() < (datetime.now() - timedelta(minutes=5)).timestamp()
            ) or (
                job.state == TaskInstanceState.FAILED
                and job.last_update.timestamp() < (datetime.now() - timedelta(hours=1)).timestamp()
            ):
                if job.key in self.last_reported_state:
                    del self.last_reported_state[job.key]
                session.delete(job)
                session.execute(
                    delete(RemoteLogsModel).where(
                        RemoteLogsModel.dag_id == job.dag_id,
                        RemoteLogsModel.run_id == job.run_id,
                        RemoteLogsModel.task_id == job.task_id,
                        RemoteLogsModel.map_index == job.map_index,
                        RemoteLogsModel.try_number == job.try_number,
                    )
                )
        session.commit()

    def end(self) -> None:
        """End the executor."""
        self.log.info("Shutting down RemoteExecutor")

    def terminate(self):
        """Terminate the executor is not doing anything."""

    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:  # pragma: no cover
        """
        Handle remnants of tasks that were failed because they were stuck in queued.

        Tasks can get stuck in queued. If such a task is detected, it will be marked
        as `UP_FOR_RETRY` if the task instance has remaining retries or marked as `FAILED`
        if it doesn't.

        :param tis: List of Task Instances to clean up
        :return: List of readable task instances for a warning message
        """
        raise NotImplementedError()

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        return [
            GroupCommand(
                name="remote",
                help="Remote worker components",
                description=(
                    "Start and manage remote worker. Works only when using RemoteExecutor. For more information, "
                    "see https://airflow.apache.org/docs/apache-airflow-providers-remote/stable/remote_executor.html"
                ),
                subcommands=REMOTE_COMMANDS,
            ),
        ]


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return RemoteExecutor._get_parser()
