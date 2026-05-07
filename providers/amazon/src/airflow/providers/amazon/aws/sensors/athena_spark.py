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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AthenaSparkSensor(BaseSensorOperator):
    """
    Poll the status of an AWS Athena Spark calculation until it reaches a terminal state.

    :param calculation_execution_id: The ID of the calculation to monitor. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    template_fields: Sequence[str] = ("calculation_execution_id",)
    ui_color = "#44e2b5"

    def __init__(
        self,
        *,
        calculation_execution_id: str,
        aws_conn_id: str = "aws_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.calculation_execution_id = calculation_execution_id
        self.aws_conn_id = aws_conn_id

    def poke(self, context: Context) -> bool:
        """Check the current status of the Spark calculation."""
        del context
        hook = AthenaHook(aws_conn_id=self.aws_conn_id)
        state = hook.check_calculation_status(self.calculation_execution_id)

        self.log.info("Calculation %s state is: %s", self.calculation_execution_id, state)

        if state in hook.SPARK_FAILURE_STATES:
            raise AirflowException(f"Calculation {self.calculation_execution_id} failed with state: {state}")

        return state == "COMPLETED"
