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

from typing import TYPE_CHECKING, Any, Sequence

from botocore.exceptions import ClientError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.kinesis_analytics import (
    KinesisAnalyticsV2ApplicationOperationCompleteTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class KinesisAnalyticsV2CreateApplicationOperator(AwsBaseOperator[KinesisAnalyticsV2Hook]):
    """
    Creates a Managed Service for Apache Flink application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KinesisAnalyticsV2CreateApplicationOperator`

    :param application_name: The name of your application. (templated)
    :param run_time_environment: The runtime environment for the application. (templated)
    :param service_execution_role: The IAM role used by the application to access Kinesis data streams,
        Kinesis Data Firehose delivery streams, Amazon S3 objects, and other external resources. (templated)
    :param create_application_kwargs: Create application extra properties. (templated)
    :param application_description: A summary description of the application. (templated)
    :param wait_for_completion: Whether to wait for job to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = KinesisAnalyticsV2Hook

    template_fields: Sequence[str] = aws_template_fields(
        "application_name",
        "run_time_environment",
        "service_execution_role",
        "create_application_kwargs",
        "application_description",
    )
    template_fields_renderers: dict = {
        "create_application_kwargs": "json",
    }

    def __int__(
        self,
        application_name: str,
        run_time_environment: str,
        service_execution_role: str,
        create_application_kwargs: dict[str, Any] | None = None,
        application_description: str = "Managed Service for Apache Flink application created from Airflow",
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.application_name = application_name
        self.run_time_environment = run_time_environment
        self.service_execution_role = service_execution_role
        self.create_application_kwargs = create_application_kwargs or {}
        self.application_description = application_description
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> str:
        self.log.info("Creating AWS Managed Service for Apache Flink application %s.", self.application_name)
        try:
            response = self.hook.conn.create_application(
                ApplicationName=self.application_name,
                ApplicationDescription=self.application_description,
                RuntimeEnvironment=self.run_time_environment,
                ServiceExecutionRole=self.service_execution_role,
                **self.create_application_kwargs,
            )
        except ClientError as error:
            raise AirflowException(
                f"AWS Managed Service for Apache Flink application creation failed: {error.response['Error']['Message']}"
            )

        if self.wait_for_completion:
            self.log.info(
                "Waiting for AWS Managed Service for Apache Flink application to create %s",
                response["ApplicationARN"],
            )

            self.hook.get_waiter("create_application_complete").wait(
                ApplicationName=self.application_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        self.log.info(
            "Completed creation of AWS Managed Service for Apache Flink application %s.",
            self.application_name,
        )

        return response["ApplicationARN"]


class KinesisAnalyticsV2StartApplicationOperator(AwsBaseOperator[KinesisAnalyticsV2Hook]):
    """
    Starts a Managed Service for Apache Flink application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KinesisAnalyticsV2StartApplicationOperator`

    :param application_name: The name of your application. (templated)
    :param run_configuration: Application properties to start Apache Flink Job. (templated)

    :param wait_for_completion: Whether to wait for job to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)
    :param deferrable: If True, the operator will wait asynchronously for the job to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = KinesisAnalyticsV2Hook

    template_fields: Sequence[str] = aws_template_fields(
        "application_name",
        "run_configuration",
    )
    template_fields_renderers: dict = {
        "run_configuration": "json",
    }

    def __init__(
        self,
        application_name: str,
        run_configuration: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.application_name = application_name
        self.run_configuration = run_configuration or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        msg = "AWS Managed Service for Apache Flink application"

        try:
            self.log.info("Starting %s.", msg, self.application_name)
            response = self.hook.conn.start_application(
                ApplicationName=self.application_name, RunConfiguration=self.run_configuration
            )
        except ClientError as error:
            raise AirflowException(
                f"{msg} {self.application_name} start failed: {error.response['Error']['Message']}"
            )

        describe_response = self.hook.conn.describe_application(ApplicationName=self.application_name)

        operation_id = response.get("OperationId")

        waiter_name, waiter_args, status_queries = self.hook.get_waiter_details(
            application_name=self.application_name, operation_id=operation_id
        )

        if self.deferrable:
            self.log.info("Deferring for %s to start: %s.", msg, self.application_name)
            self.defer(
                trigger=KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
                    application_name=self.application_name,
                    operation_id=operation_id,
                    waiter_name=waiter_name,
                    waiter_args=waiter_args,
                    status_queries=status_queries,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting for %s to start: %s.", msg, self.application_name)

            self.hook.get_waiter(waiter_name).wait(
                **waiter_args,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        self.log.info("%s started successfully %s.", msg, self.application_name)

        return describe_response["ApplicationDetail"]["ApplicationARN"]

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        response = self.hook.conn.describe_application(
            ApplicationName=self.application_name,
        )

        if event["status"] != "success":
            error = (
                self.hook.conn.describe_application_operation(
                    ApplicationName=event["application_name"], OperationId=event["operation_id"]
                )
                if event.get("operation_id")
                else event
            )

            raise AirflowException(
                "Error while starting AWS Managed Service for Apache Flink application error: %s.", error
            )

        self.log.info(
            "AWS Managed Service for Apache Flink application %s started successfully %s",
            self.application_name,
        )

        return response["ApplicationDetail"]["ApplicationARN"]


class KinesisAnalyticsV2StopApplicationOperator(AwsBaseOperator[KinesisAnalyticsV2Hook]):
    """
    Stop a Managed Service for Apache Flink application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KinesisAnalyticsV2StopApplicationOperator`

    :param application_name: The name of your application. (templated)
    :param force: Set to true to force the application to stop. f you set Force to true, Managed Service for
        Apache Flink stops the application without taking a snapshot (templated)

    :param wait_for_completion: Whether to wait for job to stop. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 60)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 20)
    :param deferrable: If True, the operator will wait asynchronously for the job to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = KinesisAnalyticsV2Hook

    template_fields: Sequence[str] = aws_template_fields(
        "application_name",
        "force",
    )

    def __init__(
        self,
        application_name: str,
        force: bool = False,
        wait_for_completion: bool = True,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.application_name = application_name
        self.force = force
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> str:
        msg = "AWS Managed Service for Apache Flink application"
        try:
            self.log.info("Stopping %s.", msg, self.application_name)

            response = self.hook.conn.stop_application(
                ApplicationName=self.application_name, Force=self.force
            )
        except ClientError as error:
            raise AirflowException(
                f" {msg} {self.application_name} to stop failed: {error.response['Error']['Message']}"
            )

        operation_id = response.get("OperationId")
        describe_response = self.hook.conn.describe_application(ApplicationName=self.application_name)

        waiter_name, waiter_args, status_queries = self.hook.get_waiter_details(
            application_name=self.application_name, operation_id=operation_id
        )

        if self.deferrable:
            self.log.info("Deferring for %s to stop: %s.", msg, self.application_name)
            self.defer(
                trigger=KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
                    application_name=self.application_name,
                    operation_id=operation_id,
                    waiter_name=waiter_name,
                    waiter_args=waiter_args,
                    status_queries=status_queries,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting for %s to stop: %s.", msg, self.application_name)

            self.hook.get_waiter(waiter_name).wait(
                **waiter_args,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        self.log.info("%s stopped successfully %s.", msg, self.application_name)

        return describe_response["ApplicationDetail"]["ApplicationARN"]

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        response = self.hook.conn.describe_application(
            ApplicationName=self.application_name,
        )

        if event["status"] != "success":
            event = validate_execute_complete_event(event)

            response = self.hook.conn.describe_application(
                ApplicationName=self.application_name,
            )

            if event["status"] != "success":
                error = (
                    self.hook.conn.describe_application_operation(
                        ApplicationName=event["application_name"], OperationId=event["operation_id"]
                    )
                    if event.get("operation_id")
                    else event
                )

                raise AirflowException(
                    "Error while stopping AWS Managed Service for Apache Flink application: %s.", error
                )

            self.log.info(
                "AWS Managed Service for Apache Flink application %s stopped successfully %s",
                self.application_name,
            )

        return response["ApplicationDetail"]["ApplicationARN"]
