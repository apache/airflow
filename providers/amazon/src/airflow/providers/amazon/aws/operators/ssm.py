#!/usr/bin/env python
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

    :param instance_ids: The IDs of the managed nodes where the command should run. Required if `targets` is not provided.
    :param targets: A list of search criteria that targets managed nodes using a Key,Value combination.
        Required if `instance_ids` is not provided.
    :param document_name: The name of the Amazon Web Services Systems Manager document (SSM document) to run.
    :param document_version: Optional SSM document version to use in the request. Options are: $DEFAULT,
        $LATEST, or a specific version number
    :param document_hash: Optional Sha256 or Sha1 hash created by the system when the document was created.
    :param document_hash_type: Optional type of hash provided in `document_hash`. Valid values are "Sha256" or "Sha1".
    :param timeout_seconds: Optional. If this time is reached and the command hasn't already started running, it won't run.
    :param comment: Optional user-specified information about the command.
    :param parameters: Optional dictionary of parameter names and their values to pass to the SSM document.
    :param output_s3_bucket_name: Optional name of the S3 bucket where command execution responses should be stored.
    :param output_s3_key_prefix: Optional prefix to use when storing command output in the specified S3 bucket
    :param max_concurrency: Optional maximum number of instances that can execute the command simultaneously.
        Specify as a number (e.g., "10") or a percentage (e.g., "10%"). (Default: 50)
    :param max_errors: Optional maximum number of errors allowed without the command failing.
        Specify as a number (e.g., "10") or a percentage (e.g., "10%"). (Default: 0)
    :param service_role_arn: Optional ARN of the IAM service role to use to publish Amazon SNS notifications
        for Run Command commands.
    :param notification_config: Optional configurations for sending notifications. Includes the parameters
        `NotificationArn`, `NotificationEvents`, and `NotificationType`.
    :param cloudwatch_output_config: Optional configuration for sending command output to Amazon CloudWatch.
        Can include `CloudWatchOutputEnabled` (boolean) and `CloudWatchLogGroupName`.
    :param alarm_configuration: Optional configuration for triggering alarms based on command status.
        Can include `IgnorePollAlarmFailure` (boolean) and a list of `Alarms` (each with `Name`).

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
        "document_version",
        "document_hash",
        "document_hash_type",
        "comment",
        "output_s3_bucket_name",
        "output_s3_key_prefix",
        "max_concurrency",
        "max_errors",
        "service_role_arn",
    )

    def __init__(
        self,
        *,
        instance_ids: list[str],
        targets: list[dict[str, str | list[str]]] | None = None,
        document_name: str,
        document_version: str | None = None,
        document_hash: str | None = None,
        document_hash_type: str | None = None,
        timeout_seconds: int | None = None,
        comment: str | None = None,
        parameters: dict[str, list[str]] | None = None,
        output_s3_bucket_name: str | None = None,
        output_s3_key_prefix: str | None = None,
        max_concurrency: str | None = None,
        max_errors: str | None = None,
        service_role_arn: str | None = None,
        notification_config: dict[str, str | list[str]] | None = None,
        cloudwatch_output_config: dict[str, str | bool] | None = None,
        alarm_configuration: dict[str, bool | list[dict[str, str]]] | None = None,
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

        self.instance_ids = instance_ids
        self.targets = targets
        self.document_name = document_name
        self.document_version = document_version
        self.document_hash = document_hash
        self.document_hash_type = document_hash_type
        self.timeout_seconds = timeout_seconds
        self.comment = comment
        self.parameters = parameters
        self.output_s3_bucket_name = output_s3_bucket_name
        self.output_s3_key_prefix = output_s3_key_prefix
        self.max_concurrency = max_concurrency
        self.max_errors = max_errors
        self.service_role_arn = service_role_arn
        self.notification_config = notification_config
        self.cloudwatch_output_config = cloudwatch_output_config
        self.alarm_configuration = alarm_configuration

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error while running run command: {event}")

        self.log.info("SSM run command `%s` completed.", event["command_id"])
        return event["command_id"]

    def execute(self, context: Context):
        response = self.hook.conn.send_command(
            InstanceIds=self.instance_ids,
            Targets=self.targets,
            DocumentName=self.document_name,
            DocumentVersion=self.document_version,
            DocumentHash=self.document_hash,
            DocumentHashType=self.document_hash_type,
            TimeoutSeconds=self.timeout_seconds,
            Comment=self.comment,
            Parameters=self.parameters,
            OutputS3BucketName=self.output_s3_bucket_name,
            OutputS3KeyPrefix=self.output_s3_key_prefix,
            MaxConcurrency=self.max_concurrency,
            MaxErrors=self.max_errors,
            ServiceRoleArn=self.service_role_arn,
            NotificationConfig=self.notification_config,
            CloudWatchOutputConfig=self.cloudwatch_output_config,
            AlarmConfiguration=self.alarm_configuration,
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
            waiter = self.hook.get_waiter("run_command_instance_complete")
            for instance_id in self.instance_ids:
                waiter.wait(
                    CommandId=command_id,
                    InstanceId=instance_id,
                    WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
                )

        return command_id
