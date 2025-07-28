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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.ssm import SsmRunCommandTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SsmRunCommandOperator(AwsBaseOperator[SsmHook]):
    """
    Executes the SSM Run Command to perform actions on managed instances.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SsmRunCommandOperator`

    :param document_name: The name of the Amazon Web Services Systems Manager document (SSM document) to run.
    :param run_command_kwargs: Optional parameters to pass to the send_command API.

    :param wait_for_completion: Whether to wait for cluster to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 120)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 75)
    :param deferrable: If True, the operator will wait asynchronously for the cluster to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = SsmHook
    template_fields: Sequence[str] = aws_template_fields(
        "document_name",
        "run_command_kwargs",
    )

    def __init__(
        self,
        *,
        document_name: str,
        run_command_kwargs: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

        self.document_name = document_name
        self.run_command_kwargs = run_command_kwargs or {}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running run command: {event}")

        self.log.info("SSM run command `%s` completed.", event["command_id"])
        return event["command_id"]

    def execute(self, context: Context):
        response = self.hook.conn.send_command(
            DocumentName=self.document_name,
            **self.run_command_kwargs,
        )

        command_id = response["Command"]["CommandId"]
        task_description = f"SSM run command {command_id} to complete."

        if self.deferrable:
            self.log.info("Deferring for %s", task_description)
            self.defer(
                trigger=SsmRunCommandTrigger(
                    command_id=command_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info("Waiting for %s", task_description)
            waiter = self.hook.get_waiter("command_executed")

            instance_ids = response["Command"]["InstanceIds"]
            for instance_id in instance_ids:
                waiter.wait(
                    CommandId=command_id,
                    InstanceId=instance_id,
                    WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
                )

        return command_id
