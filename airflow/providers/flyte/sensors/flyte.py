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

from typing import TYPE_CHECKING, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.flyte.hooks.flyte import AirflowFlyteHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirflowFlyteSensor(BaseSensorOperator):
    """
    Check for the status of a Flyte execution.

    :param execution_name: Required. The name of the execution to check.
    :param project: Optional. The project to connect to.
    :param domain: Optional. The domain to connect to.
    :param flyte_conn_id: Required. The name of the Flyte connection to
                          get the connection information for Flyte.
    """

    template_fields: Sequence[str] = ("execution_name",)  # mypy fix

    def __init__(
        self,
        execution_name: str,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        flyte_conn_id: str = "flyte_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.execution_name = execution_name
        self.project = project
        self.domain = domain
        self.flyte_conn_id = flyte_conn_id

    def poke(self, context: "Context") -> bool:
        """Check for the status of a Flyte execution."""
        hook = AirflowFlyteHook(flyte_conn_id=self.flyte_conn_id, project=self.project, domain=self.domain)
        remote = hook.create_flyte_remote()
        execution_id = hook.execution_id(self.execution_name)

        phase = remote.client.get_execution(execution_id).closure.phase

        if phase == hook.SUCCEEDED:
            return True
        elif phase == hook.FAILED:
            raise AirflowException(f"Execution {self.execution_name} failed")
        elif phase == hook.TIMED_OUT:
            raise AirflowException(f"Execution {self.execution_name} timedout")
        elif phase == hook.ABORTED:
            raise AirflowException(f"Execution {self.execution_name} aborted")

        self.log.info("Waiting for execution %s to complete", self.execution_name)
        return False
