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

from typing import TYPE_CHECKING, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class QuickSightSensor(BaseSensorOperator):
    """
    Watches for the status of a QuickSight Ingestion.

    :param aws_account_id: An AWS Account ID
    :param data_set_id: QuickSight Data Set ID
    :param ingestion_id: QuickSight Ingestion ID
    :param aws_conn_id: aws connection to use, defaults to "aws_default"
    """

    def __init__(
        self,
        *,
        aws_account_id: str,
        data_set_id: str,
        ingestion_id: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_account_id = aws_account_id
        self.data_set_id = data_set_id
        self.aws_account_id = aws_account_id
        self.ingestion_id = ingestion_id
        self.aws_conn_id = aws_conn_id
        self.success_status = "COMPLETED"
        self.errored_statuses = ("FAILED", "CANCELLED")
        self.hook: Optional[QuickSightHook] = None

    def poke(self, context: "Context"):
        """
        Pokes until the QuickSight Ingestion has successfully finished.

        :param context: The task context during execution.
        :return: True if it COMPLETED and False if not.
        :rtype: bool
        """
        hook = self.get_hook()
        self.log.info("Poking for Amazon QuickSight Ingestion ID: %s", self.ingestion_id)
        quicksight_ingestion_state = hook.get_status(self.aws_account_id, self.data_set_id, self.ingestion_id)
        self.log.info("QuickSight Status: %s", quicksight_ingestion_state)
        if quicksight_ingestion_state in self.errored_statuses:
            raise AirflowException("The QuickSight Ingestion failed!")
        return quicksight_ingestion_state == self.success_status

    def get_hook(self) -> QuickSightHook:
        """Returns a new or pre-existing QuickSightHook"""
        if self.hook:
            return self.hook

        self.hook = QuickSightHook(aws_conn_id=self.aws_conn_id)
        return self.hook
