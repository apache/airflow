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

import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from flytekit.configuration import AuthType, Config, PlatformConfig
from flytekit.exceptions.user import FlyteEntityNotExistException
from flytekit.models.common import AuthRole
from flytekit.models.core import execution as core_execution_models
from flytekit.models.core.identifier import WorkflowExecutionIdentifier
from flytekit.remote.remote import FlyteRemote, Options

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AirflowFlyteHook(BaseHook):
    """
    Interact with the FlyteRemote API.

    :param flyte_conn_id: Required. The name of the Flyte connection to get
                          the connection information for Flyte.
    :param project: Optional. The project under consideration.
    :param domain: Optional. The domain under consideration.
    """

    SUCCEEDED = core_execution_models.WorkflowExecutionPhase.SUCCEEDED
    FAILED = core_execution_models.WorkflowExecutionPhase.FAILED
    TIMED_OUT = core_execution_models.WorkflowExecutionPhase.TIMED_OUT
    ABORTED = core_execution_models.WorkflowExecutionPhase.ABORTED

    flyte_conn_id = "flyte_default"
    conn_type = "flyte"

    def __init__(
        self, flyte_conn_id: str = flyte_conn_id, project: Optional[str] = None, domain: Optional[str] = None
    ) -> None:
        super().__init__()
        self.flyte_conn_id = flyte_conn_id
        self.flyte_conn = self.get_connection(self.flyte_conn_id)
        self.project = project or self.flyte_conn.extra_dejson.get("project")
        self.domain = domain or self.flyte_conn.extra_dejson.get("domain")

        if not (self.project and self.domain):
            raise AirflowException("Please provide a project and domain.")

    def execution_id(self, execution_name: str) -> WorkflowExecutionIdentifier:
        """Get the execution id."""
        return WorkflowExecutionIdentifier(self.project, self.domain, execution_name)

    def create_flyte_remote(self) -> FlyteRemote:
        """Create a FlyteRemote object."""
        remote = FlyteRemote(
            config=Config(
                platform=PlatformConfig(
                    endpoint=":".join([self.flyte_conn.host, self.flyte_conn.port])
                    if (self.flyte_conn.host and self.flyte_conn.port)
                    else (self.flyte_conn.host or "localhost:30081"),
                    insecure=self.flyte_conn.extra_dejson.get("insecure", False),
                    client_id=self.flyte_conn.login or None,
                    client_credentials_secret=self.flyte_conn.password or None,
                    command=self.flyte_conn.extra_dejson.get("command", None),
                    scopes=self.flyte_conn.extra_dejson.get("scopes", None),
                    auth_mode=AuthType(self.flyte_conn.extra_dejson.get("auth_mode", "standard")),
                )
            ),
        )
        return remote

    def trigger_execution(
        self,
        launchplan_name: Optional[str] = None,
        task_name: Optional[str] = None,
        max_parallelism: Optional[int] = None,
        raw_data_prefix: Optional[str] = None,
        assumable_iam_role: Optional[str] = None,
        kubernetes_service_account: Optional[str] = None,
        version: Optional[str] = None,
        inputs: Dict[str, Any] = {},
    ) -> str:
        """
        Trigger an execution.

        :param launchplan_name: Optional. The name of the launchplan to trigger.
        :param task_name: Optional. The name of the task to trigger.
        :param max_parallelism: Optional. The maximum number of parallel executions to allow.
        :param raw_data_prefix: Optional. The prefix to use for raw data.
        :param assumable_iam_role: Optional. The assumable IAM role to use.
        :param kubernetes_service_account: Optional. The kubernetes service account to use.
        :param version: Optional. The version of the launchplan to trigger.
        :param inputs: Optional. The inputs to the launchplan.
        """
        if (not (task_name or launchplan_name)) or (task_name and launchplan_name):
            raise AirflowException("Either task_name or launchplan_name is required.")

        remote = self.create_flyte_remote()
        try:
            if launchplan_name:
                flyte_entity = remote.fetch_launch_plan(
                    name=launchplan_name, project=self.project, domain=self.domain, version=version
                )
            elif task_name:
                flyte_entity = remote.fetch_task(
                    name=task_name, project=self.project, domain=self.domain, version=version
                )
        except FlyteEntityNotExistException as e:
            raise AirflowException(f"Failed to fetch entity: {e}")

        try:
            execution = remote.execute(
                flyte_entity,
                inputs=inputs,
                project=self.project,
                domain=self.domain,
                options=Options(
                    raw_data_prefix=raw_data_prefix,
                    max_parallelism=max_parallelism,
                    auth_role=AuthRole(
                        assumable_iam_role=assumable_iam_role,
                        kubernetes_service_account=kubernetes_service_account,
                    ),
                ),
            )
        except Exception as e:
            raise AirflowException(f"Failed to trigger execution: {e}")

        return execution.id.name

    def wait_for_execution(
        self,
        execution_name: str,
        timeout: Optional[timedelta] = None,
        poll_interval: timedelta = timedelta(seconds=30),
    ) -> None:
        """
        Helper method which polls an execution to check the status.

        :param execution: Required. The execution to check.
        :param timeout: Optional. The timeout to wait for the execution to finish.
        :param poll_interval: Optional. The interval between checks to poll the execution.
        """
        remote = self.create_flyte_remote()
        execution_id = self.execution_id(execution_name)

        time_to_give_up = datetime.max if timeout is None else datetime.utcnow() + timeout

        while datetime.utcnow() < time_to_give_up:
            time.sleep(poll_interval.total_seconds())

            phase = remote.client.get_execution(execution_id).closure.phase

            if phase == self.SUCCEEDED:
                return
            elif phase == self.FAILED:
                raise AirflowException(f"Execution {execution_name} failed")
            elif phase == self.TIMED_OUT:
                raise AirflowException(f"Execution {execution_name} timedout")
            elif phase == self.ABORTED:
                raise AirflowException(f"Execution {execution_name} aborted")
            else:
                continue

        raise AirflowException(f"Execution {execution_name} timedout")

    def terminate(
        self,
        execution_name: str,
        cause: str,
    ) -> None:
        """
        Terminate an execution.

        :param execution: Required. The execution to terminate.
        :param cause: Required. The cause of the termination.
        """
        remote = self.create_flyte_remote()
        execution_id = self.execution_id(execution_name)
        remote.client.terminate_execution(id=execution_id, cause=cause)
