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

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class QuickSightSensor(BaseSensorOperator):
    """
    Watches for the status of an Amazon QuickSight Ingestion.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:QuickSightSensor`

    :param data_set_id:  ID of the dataset used in the ingestion.
    :param ingestion_id: ID for the ingestion.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    """

    template_fields: Sequence[str] = ('data_set_id', 'ingestion_id', 'aws_conn_id')

    def __init__(
        self,
        *,
        data_set_id: str,
        ingestion_id: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.data_set_id = data_set_id
        self.ingestion_id = ingestion_id
        self.aws_conn_id = aws_conn_id
        self.success_status = "COMPLETED"
        self.errored_statuses = ("FAILED", "CANCELLED")
        self.quicksight_hook: QuickSightHook | None = None
        self.sts_hook: StsHook | None = None

    def poke(self, context: Context):
        """
        Pokes until the QuickSight Ingestion has successfully finished.

        :param context: The task context during execution.
        :return: True if it COMPLETED and False if not.
        :rtype: bool
        """
        quicksight_hook = self.get_quicksight_hook
        sts_hook = self.get_sts_hook
        self.log.info("Poking for Amazon QuickSight Ingestion ID: %s", self.ingestion_id)
        aws_account_id = sts_hook.get_account_number()
        quicksight_ingestion_state = quicksight_hook.get_status(
            aws_account_id, self.data_set_id, self.ingestion_id
        )
        self.log.info("QuickSight Status: %s", quicksight_ingestion_state)
        if quicksight_ingestion_state in self.errored_statuses:
            raise AirflowException("The QuickSight Ingestion failed!")
        return quicksight_ingestion_state == self.success_status

    @cached_property
    def get_quicksight_hook(self):
        return QuickSightHook(aws_conn_id=self.aws_conn_id)

    @cached_property
    def get_sts_hook(self):
        return StsHook(aws_conn_id=self.aws_conn_id)
