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

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.ssm import SsmRunCommandTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class SsmRunCommandCompletedSensor(AwsBaseSensor[SsmHook]):
    """
    Poll the state of an AWS SSM Run Command until completion.

    Waits until all instance jobs reach a terminal state. Fails if any
    instance job ends in a failed state.

    .. seealso::
        For more information on how to use this sensor, take a look at the
        guide:
        :ref:`howto/sensor:SsmRunCommandCompletedSensor`

    :param command_id: The ID of the AWS SSM Run Command.
    :param fail_on_nonzero_exit: If True (default), the sensor will fail when the command
        returns a non-zero exit code. If False, the sensor will complete successfully
        for both Success and Failed command statuses, allowing downstream tasks to handle
        exit codes. AWS-level failures (Cancelled, TimedOut) will still raise exceptions.
        (default: True)
    :param deferrable: If True, the sensor will operate in deferrable mode.
        This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting
        default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status
        of the job. (default: 120)
    :param max_retries: Number of times before returning the current state.
        (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used.
        If running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default
        boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore
        client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    INTERMEDIATE_STATES: tuple[str, ...] = (
        "Pending",
        "Delayed",
        "InProgress",
        "Cancelling",
    )
    FAILURE_STATES: tuple[str, ...] = ("Cancelled", "TimedOut", "Failed")
    SUCCESS_STATES: tuple[str, ...] = ("Success",)
    FAILURE_MESSAGE = "SSM run command sensor failed."

    aws_hook_class = SsmHook
    template_fields: Sequence[str] = aws_template_fields(
        "command_id",
    )

    def __init__(
        self,
        *,
        command_id,
        fail_on_nonzero_exit: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: int = 120,
        max_retries: int = 75,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.command_id = command_id
        self.fail_on_nonzero_exit = fail_on_nonzero_exit
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self.max_retries = max_retries

    def poke(self, context: Context):
        response = self.hook.conn.list_command_invocations(CommandId=self.command_id)
        command_invocations = response.get("CommandInvocations", [])

        if not command_invocations:
            self.log.info(
                "No command invocations found",
                "command_id=%s yet, waiting...",
                self.command_id,
            )
            return False

        for invocation in command_invocations:
            state = invocation["Status"]

            if state in self.FAILURE_STATES:
                # Check if we should tolerate this failure
                if self.fail_on_nonzero_exit:
                    raise RuntimeError(self.FAILURE_MESSAGE)  # Traditional behavior

                # Only fail on AWS-level issues, tolerate command failures
                if SsmHook.is_aws_level_failure(state):
                    raise RuntimeError(f"SSM command {self.command_id} {state}")

                # Command failed but we're tolerating it
                self.log.info(
                    "Command invocation has status %s. Continuing due to fail_on_nonzero_exit=False",
                    state,
                )

            if state in self.INTERMEDIATE_STATES:
                return False

        return True

    def execute(self, context: Context):
        if self.deferrable:
            self.defer(
                trigger=SsmRunCommandTrigger(
                    command_id=self.command_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                    fail_on_nonzero_exit=self.fail_on_nonzero_exit,
                ),
                method_name="execute_complete",
            )

        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise RuntimeError(f"Error while running run command: {event}")

        self.log.info("SSM run command `%s` completed.", event["command_id"])
