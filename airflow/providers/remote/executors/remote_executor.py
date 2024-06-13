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

from typing import TYPE_CHECKING, Any

from airflow.cli.cli_config import GroupCommand
from airflow.executors.base_executor import BaseExecutor
from airflow.models.abstractoperator import DEFAULT_QUEUE
from airflow.models.taskinstance import TaskInstanceState
from airflow.providers.remote.cli.remote_command import REMOTE_COMMANDS
from airflow.providers.remote.models import RemoteJobModel
from airflow.utils.db import DBLocks, create_global_lock
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey


class RemoteExecutor(BaseExecutor):
    """Implementation of the remote executor to distribute work to remote workers via HTTP."""

    @provide_session
    def start(self, session: Session = NEW_SESSION):
        """If Remote Executor provider is loaded first time, ensure table exists."""
        with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
            engine = session.get_bind().engine
            RemoteJobModel.metadata.create_all(engine)

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

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        # TODO This might be used to clean the task table and upload completed logs

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        """
        Return the task logs.

        :param ti: A TaskInstance object
        :param try_number: current try_number to read log from
        :return: tuple of logs and messages
        """
        return [], []

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
