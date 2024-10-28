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
    Creates an AWS Managed Service for Apache Flink application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KinesisAnalyticsV2CreateApplicationOperator`

    :param application_name: The name of application. (templated)
    :param runtime_environment: The runtime environment for the application. (templated)
    :param service_execution_role: The IAM role used by the application to access services. (templated)
    :param create_application_kwargs: Create application extra properties. (templated)
    :param application_description: A summary description of the application. (templated)

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
    ui_color = "#44b5e2"

    template_fields: Sequence[str] = aws_template_fields(
        "application_name",
        "runtime_environment",
        "service_execution_role",
        "create_application_kwargs",
        "application_description",
    )
    template_fields_renderers: dict = {
        "create_application_kwargs": "json",
    }

    def __init__(
        self,
        application_name: str,
        runtime_environment: str,
        service_execution_role: str,
        create_application_kwargs: dict[str, Any] | None = None,
        application_description: str = "Managed Service for Apache Flink application created from Airflow",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.application_name = application_name
        self.runtime_environment = runtime_environment
        self.service_execution_role = service_execution_role
        self.create_application_kwargs = create_application_kwargs or {}
        self.application_description = application_description

    def execute(self, context: Context) -> dict[str, str]:
        self.log.info("Creating AWS Managed Service for Apache Flink application %s.", self.application_name)
        try:
            response = self.hook.conn.create_application(
                ApplicationName=self.application_name,
                ApplicationDescription=self.application_description,
                RuntimeEnvironment=self.runtime_environment,
                ServiceExecutionRole=self.service_execution_role,
                **self.create_application_kwargs,
            )
        except ClientError as error:
            raise AirflowException(
                f"AWS Managed Service for Apache Flink application creation failed: {error.response['Error']['Message']}"
            )

        self.log.info(
            "AWS Managed Service for Apache Flink application created successfully %s.",
            self.application_name,
        )

        return {"ApplicationARN": response["ApplicationDetail"]["ApplicationARN"]}


class KinesisAnalyticsV2StartApplicationOperator(AwsBaseOperator[KinesisAnalyticsV2Hook]):
    """
    Starts an AWS Managed Service for Apache Flink application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KinesisAnalyticsV2StartApplicationOperator`

    :param application_name: The name of application. (templated)
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
    ui_color = "#44b5e2"

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

    def execute(self, context: Context) -> dict[str, Any]:
        msg = "AWS Managed Service for Apache Flink application"

        try:
            self.log.info("Starting %s %s.", msg, self.application_name)
            self.hook.conn.start_application(
                ApplicationName=self.application_name, RunConfiguration=self.run_configuration
            )
        except ClientError as error:
            raise AirflowException(
                f"Failed to start {msg} {self.application_name}: {error.response['Error']['Message']}"
            )

        describe_response = self.hook.conn.describe_application(ApplicationName=self.application_name)

        if self.deferrable:
            self.log.info("Deferring for %s to start: %s.", msg, self.application_name)
            self.defer(
                trigger=KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
                    application_name=self.application_name,
                    waiter_name="application_start_complete",
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

            self.hook.get_waiter("application_start_complete").wait(
                ApplicationName=self.application_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        self.log.info("%s started successfully %s.", msg, self.application_name)

        return {"ApplicationARN": describe_response["ApplicationDetail"]["ApplicationARN"]}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(
                "Error while starting AWS Managed Service for Apache Flink application: %s", event
            )

        response = self.hook.conn.describe_application(
            ApplicationName=event["application_name"],
        )

        self.log.info(
            "AWS Managed Service for Apache Flink application %s started successfully.",
            event["application_name"],
        )

        return {"ApplicationARN": response["ApplicationDetail"]["ApplicationARN"]}


class KinesisAnalyticsV2StopApplicationOperator(AwsBaseOperator[KinesisAnalyticsV2Hook]):
    """
    Stop an AWS Managed Service for Apache Flink application.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KinesisAnalyticsV2StopApplicationOperator`

    :param application_name: The name of your application. (templated)
    :param force: Set to true to force the application to stop. If you set Force to true, Managed Service for
        Apache Flink stops the application without taking a snapshot. (templated)

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
    ui_color = "#44b5e2"

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

    def execute(self, context: Context) -> dict[str, Any]:
        msg = "AWS Managed Service for Apache Flink application"

        try:
            self.log.info("Stopping %s %s.", msg, self.application_name)

            self.hook.conn.stop_application(ApplicationName=self.application_name, Force=self.force)
        except ClientError as error:
            raise AirflowException(
                f"Failed to stop {msg} {self.application_name}: {error.response['Error']['Message']}"
            )

        describe_response = self.hook.conn.describe_application(ApplicationName=self.application_name)

        if self.deferrable:
            self.log.info("Deferring for %s to stop: %s.", msg, self.application_name)
            self.defer(
                trigger=KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
                    application_name=self.application_name,
                    waiter_name="application_stop_complete",
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

            self.hook.get_waiter("application_stop_complete").wait(
                ApplicationName=self.application_name,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        self.log.info("%s stopped successfully %s.", msg, self.application_name)

        return {"ApplicationARN": describe_response["ApplicationDetail"]["ApplicationARN"]}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException("Error while stopping AWS Managed Service for Apache Flink application")

        response = self.hook.conn.describe_application(
            ApplicationName=event["application_name"],
        )

        self.log.info(
            "AWS Managed Service for Apache Flink application %s stopped successfully.",
            event["application_name"],
        )

        return {"ApplicationARN": response["ApplicationDetail"]["ApplicationARN"]}
