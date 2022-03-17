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

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.flyte.hooks.flyte import AirflowFlyteHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirflowFlyteOperator(BaseOperator):
    """
    Launch Flyte executions from within Airflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirflowFlyteOperator`

    :param flyte_conn_id: Required. The connection to Flyte setup, containing metadata.
    :param project: Optional. The project to connect to.
    :param domain: Optional. The domain to connect to.
    :param launchplan_name: Optional. The name of the launchplan to trigger.
    :param task_name: Optional. The name of the task to trigger.
    :param max_parallelism: Optional. The maximum number of parallel executions to allow.
    :param raw_data_prefix: Optional. The prefix to use for raw data.
    :param assumable_iam_role: Optional. The IAM role to assume.
    :param kubernetes_service_account: Optional. The Kubernetes service account to use.
    :param version: Optional. The version of the launchplan/task to trigger.
    :param inputs: Optional. The inputs to the launchplan/task.
    :param timeout: Optional. The timeout to wait for the execution to finish.
    :param asynchronous: Optional. Whether to wait for the execution to finish or not.
    """

    template_fields: Sequence[str] = ("flyte_conn_id",)  # mypy fix

    def __init__(
        self,
        flyte_conn_id: str,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        launchplan_name: Optional[str] = None,
        task_name: Optional[str] = None,
        max_parallelism: Optional[int] = None,
        raw_data_prefix: Optional[str] = None,
        assumable_iam_role: Optional[str] = None,
        kubernetes_service_account: Optional[str] = None,
        version: Optional[str] = None,
        inputs: Dict[str, Any] = {},
        timeout: Optional[timedelta] = None,
        poll_interval: timedelta = timedelta(seconds=30),
        asynchronous: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.flyte_conn_id = flyte_conn_id
        self.project = project
        self.domain = domain
        self.launchplan_name = launchplan_name
        self.task_name = task_name
        self.max_parallelism = max_parallelism
        self.raw_data_prefix = raw_data_prefix
        self.assumable_iam_role = assumable_iam_role
        self.kubernetes_service_account = kubernetes_service_account
        self.version = version
        self.inputs = inputs
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.asynchronous = asynchronous
        self.execution_name: Optional[str] = None

        if (not (self.task_name or self.launchplan_name)) or (self.task_name and self.launchplan_name):
            raise AirflowException("Either task_name or launchplan_name is required.")

    def execute(self, context: "Context") -> str:
        """Trigger an execution and wait for it to finish."""
        hook = AirflowFlyteHook(flyte_conn_id=self.flyte_conn_id, project=self.project, domain=self.domain)
        self.execution_name = hook.trigger_execution(
            launchplan_name=self.launchplan_name,
            task_name=self.task_name,
            max_parallelism=self.max_parallelism,
            raw_data_prefix=self.raw_data_prefix,
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
        )
        self.log.info("Execution %s submitted", self.execution_name)

        if not self.asynchronous:
            self.log.info("Waiting for execution %s to complete", self.execution_name)
            hook.wait_for_execution(
                execution_name=self.execution_name,
                timeout=self.timeout,
                poll_interval=self.poll_interval,
            )
            self.log.info("Execution %s completed", self.execution_name)

        return self.execution_name

    def on_kill(self) -> None:
        """Kill the execution."""
        if self.execution_name:
            print(f"Killing execution {self.execution_name}")
            hook = AirflowFlyteHook(
                flyte_conn_id=self.flyte_conn_id, project=self.project, domain=self.domain
            )
            hook.terminate(
                execution_name=self.execution_name,
                cause="Killed by Airflow",
            )
