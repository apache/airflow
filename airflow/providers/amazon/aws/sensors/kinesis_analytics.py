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

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.kinesis_analytics import (
    KinesisAnalyticsV2ApplicationOperationCompleteTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class KinesisAnalyticsV2StartApplicationCompletedSensor(AwsBaseSensor[KinesisAnalyticsV2Hook]):
    """
    Waits for AWS Managed Service for Apache Flink application to start.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:KinesisAnalyticsV2StartApplicationCompletedSensor`

    :param application_name: Application name.

    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 120)
    :param max_retries: Number of times before returning the current state. (default: 75)
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
    ui_color = "#66c3ff"

    INTERMEDIATE_STATES: tuple[str, ...] = ("STARTING", "UPDATING", "AUTOSCALING")
    FAILURE_STATES: tuple[str, ...] = (
        "DELETING",
        "STOPPING",
        "READY",
        "FORCE_STOPPING",
        "ROLLING_BACK",
        "MAINTENANCE",
        "ROLLED_BACK",
    )
    SUCCESS_STATES: tuple[str, ...] = ("RUNNING",)
    FAILURE_MESSAGE = "AWS Managed Service for Apache Flink application start failed."

    template_fields: Sequence[str] = aws_template_fields("application_name")

    def __init__(
        self,
        *,
        application_name: str,
        max_retries: int = 75,
        poke_interval: int = 120,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.max_retries = max_retries
        self.poke_interval = poke_interval
        self.deferrable = deferrable

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
                    application_name=self.application_name,
                    waiter_name="application_start_complete",
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)

    def poke(self, context: Context, **kwargs) -> bool:
        status = self.hook.conn.describe_application(ApplicationName=self.application_name)[
            "ApplicationDetail"
        ]["ApplicationStatus"]

        self.log.info(
            "Poking for AWS Managed Service for Apache Flink application: %s status: %s",
            self.application_name,
            status,
        )

        if status in self.FAILURE_STATES:
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            if self.soft_fail:
                raise AirflowSkipException(self.FAILURE_MESSAGE)
            raise AirflowException(self.FAILURE_MESSAGE)

        if status in self.SUCCESS_STATES:
            self.log.info(
                "AWS Managed Service for Apache Flink application started successfully `%s`.",
                self.application_name,
            )
            return True

        return False


class KinesisAnalyticsV2StopApplicationCompletedSensor(AwsBaseSensor[KinesisAnalyticsV2Hook]):
    """
    Waits for AWS Managed Service for Apache Flink application to stop.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:KinesisAnalyticsV2StopApplicationCompletedSensor`

    :param application_name: Application name.

    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 120)
    :param max_retries: Number of times before returning the current state. (default: 75)
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
    ui_color = "#66c3ff"

    INTERMEDIATE_STATES: tuple[str, ...] = (
        "STARTING",
        "UPDATING",
        "AUTOSCALING",
        "RUNNING",
        "STOPPING",
        "FORCE_STOPPING",
    )
    FAILURE_STATES: tuple[str, ...] = ("DELETING", "ROLLING_BACK", "MAINTENANCE", "ROLLED_BACK")
    SUCCESS_STATES: tuple[str, ...] = ("READY",)
    FAILURE_MESSAGE = "AWS Managed Service for Apache Flink application stop failed."

    template_fields: Sequence[str] = aws_template_fields("application_name")

    def __init__(
        self,
        *,
        application_name: str,
        max_retries: int = 75,
        poke_interval: int = 120,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.max_retries = max_retries
        self.poke_interval = poke_interval
        self.deferrable = deferrable

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
                    application_name=self.application_name,
                    waiter_name="application_stop_complete",
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)

    def poke(self, context: Context, **kwargs) -> bool:
        status = self.hook.conn.describe_application(ApplicationName=self.application_name)[
            "ApplicationDetail"
        ]["ApplicationStatus"]

        self.log.info(
            "Poking for AWS Managed Service for Apache Flink application: %s status: %s",
            self.application_name,
            status,
        )

        if status in self.FAILURE_STATES:
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            if self.soft_fail:
                raise AirflowSkipException(self.FAILURE_MESSAGE)
            raise AirflowException(self.FAILURE_MESSAGE)

        if status in self.SUCCESS_STATES:
            self.log.info(
                "AWS Managed Service for Apache Flink application stopped successfully `%s`.",
                self.application_name,
            )
            return True

        return False
