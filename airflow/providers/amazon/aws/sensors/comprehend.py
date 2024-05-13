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

import abc
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.comprehend import ComprehendPiiEntitiesDetectionJobCompletedTrigger
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ComprehendBaseSensor(AwsBaseSensor[ComprehendHook]):
    """
    General sensor behavior for Amazon Comprehend.

    Subclasses must implement following methods:
        - ``get_state()``

    Subclasses must set the following fields:
        - ``INTERMEDIATE_STATES``
        - ``FAILURE_STATES``
        - ``SUCCESS_STATES``
        - ``FAILURE_MESSAGE``

    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    """

    aws_hook_class = ComprehendHook

    INTERMEDIATE_STATES: tuple[str, ...] = ()
    FAILURE_STATES: tuple[str, ...] = ()
    SUCCESS_STATES: tuple[str, ...] = ()
    FAILURE_MESSAGE = ""

    ui_color = "#66c3ff"

    def __init__(
        self,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.deferrable = deferrable

    def poke(self, context: Context, **kwargs) -> bool:
        state = self.get_state()
        if state in self.FAILURE_STATES:
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            if self.soft_fail:
                raise AirflowSkipException(self.FAILURE_MESSAGE)
            raise AirflowException(self.FAILURE_MESSAGE)

        return state not in self.INTERMEDIATE_STATES

    @abc.abstractmethod
    def get_state(self) -> str:
        """Implement in subclasses."""


class ComprehendStartPiiEntitiesDetectionJobCompletedSensor(ComprehendBaseSensor):
    """
    Poll the state of the pii entities detection job until it reaches a completed state; fails if the job fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:ComprehendStartPiiEntitiesDetectionJobCompletedSensor`

    :param job_id: The id of the Comprehend pii entities detection job.

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

    INTERMEDIATE_STATES: tuple[str, ...] = ("IN_PROGRESS",)
    FAILURE_STATES: tuple[str, ...] = ("FAILED", "STOP_REQUESTED", "STOPPED")
    SUCCESS_STATES: tuple[str, ...] = ("COMPLETED",)
    FAILURE_MESSAGE = "Comprehend start pii entities detection job sensor failed."

    template_fields: Sequence[str] = aws_template_fields("job_id")

    def __init__(
        self,
        *,
        job_id: str,
        max_retries: int = 75,
        poke_interval: int = 120,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.max_retries = max_retries
        self.poke_interval = poke_interval

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=ComprehendPiiEntitiesDetectionJobCompletedTrigger(
                    job_id=self.job_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)

    def get_state(self) -> str:
        return self.hook.conn.describe_pii_entities_detection_job(JobId=self.job_id)[
            "PiiEntitiesDetectionJobProperties"
        ]["JobStatus"]
