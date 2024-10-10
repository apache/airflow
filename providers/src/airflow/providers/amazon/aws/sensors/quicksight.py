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

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor

if TYPE_CHECKING:
    from airflow.utils.context import Context


class QuickSightSensor(AwsBaseSensor[QuickSightHook]):
    """
    Watches for the status of an Amazon QuickSight Ingestion.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:QuickSightSensor`

    :param data_set_id:  ID of the dataset used in the ingestion.
    :param ingestion_id: ID for the ingestion.
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

    def __init__(self, *, data_set_id: str, ingestion_id: str, **kwargs):
        super().__init__(**kwargs)
        self.data_set_id = data_set_id
        self.ingestion_id = ingestion_id
        self.success_status = "COMPLETED"
        self.errored_statuses = ("FAILED", "CANCELLED")

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
            raise AirflowException(f"The QuickSight Ingestion failed. Error info: {error}")
        return quicksight_ingestion_state == self.success_status
