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

from airflow.providers.amazon.aws.exceptions import QuickSightIngestionFailedError
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.quicksight import QuickSightIngestionCompletedTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class QuickSightSensor(AwsBaseSensor[QuickSightHook]):
    """
    Watches for the status of an Amazon QuickSight Ingestion.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:QuickSightSensor`

    :param data_set_id:  ID of the dataset used in the ingestion.
    :param ingestion_id: ID for the ingestion.
    :param max_retries: Number of times before returning the current state. (default: 75)
    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires
        aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
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

    aws_hook_class = QuickSightHook
    template_fields: Sequence[str] = ("data_set_id", "ingestion_id", "aws_conn_id")

    def __init__(
        self,
        *,
        data_set_id: str,
        ingestion_id: str,
        max_retries: int = 75,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_set_id = data_set_id
        self.ingestion_id = ingestion_id
        self.max_retries = max_retries
        self.deferrable = deferrable
        self.success_status = "COMPLETED"
        self.errored_statuses = ("FAILED", "CANCELLED")

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=QuickSightIngestionCompletedTrigger(
                    data_set_id=self.data_set_id,
                    ingestion_id=self.ingestion_id,
                    aws_account_id=self.hook.account_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
        else:
            return super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)
        if validated_event["status"] != "success":
            raise QuickSightIngestionFailedError(
                f"Error while waiting for the Amazon QuickSight ingestion: {validated_event}"
            )
        self.log.info("Amazon QuickSight SPICE ingestion `%s` completed.", validated_event["ingestion_id"])

    def poke(self, context: Context) -> bool:
        """
        Pokes until the QuickSight Ingestion has successfully finished.

        :param context: The task context during execution.
        :return: True if it COMPLETED and False if not.
        """
        self.log.info("Poking for Amazon QuickSight Ingestion ID: %s", self.ingestion_id)
        quicksight_ingestion_state = self.hook.get_status(None, self.data_set_id, self.ingestion_id)
        self.log.info("QuickSight Status: %s", quicksight_ingestion_state)
        if quicksight_ingestion_state in self.errored_statuses:
            error = self.hook.get_error_info(None, self.data_set_id, self.ingestion_id)
            raise QuickSightIngestionFailedError(f"The QuickSight Ingestion failed. Error info: {error}")
        return quicksight_ingestion_state == self.success_status
