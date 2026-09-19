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
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor

if TYPE_CHECKING:
    from airflow.sdk import Context


class AthenaSparkSensor(AwsBaseSensor[AthenaHook]):
    """
    Waits for an Amazon Athena Spark calculation to reach a terminal state.

    :param calculation_execution_id: Athena Spark calculation execution ID. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials. Defaults to ``aws_default``.
    :param region_name: AWS region name. Defaults to ``None``. (templated)
    """

    aws_hook_class = AthenaHook

    template_fields: Sequence[str] = (
        "calculation_execution_id",
        "aws_conn_id",
        "region_name",
    )

    def __init__(
        self,
        *,
        calculation_execution_id: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(aws_conn_id=aws_conn_id, region_name=region_name, **kwargs)
        self.calculation_execution_id = calculation_execution_id

    def poke(self, context: Context) -> bool:
        state = self.hook.check_spark_calculation_status(
            calculation_execution_id=self.calculation_execution_id,
        )

        self.log.info("Calculation %s state is: %s", self.calculation_execution_id, state)

        if state in AthenaHook.SPARK_SUCCESS_STATES:
            self.log.info("Athena Spark calculation %s completed.", self.calculation_execution_id)
            return True

        if state in AthenaHook.SPARK_FAILURE_STATES:
            reason = self.hook.get_spark_calculation_state_change_reason(
                calculation_execution_id=self.calculation_execution_id,
            )
            raise RuntimeError(
                f"Athena Spark calculation {self.calculation_execution_id} failed with state {state}. "
                f"Reason: {reason or 'Unknown'}"
            )

        if state in AthenaHook.SPARK_INTERMEDIATE_STATES:
            self.log.info(
                "Athena Spark calculation %s is in state %s.",
                self.calculation_execution_id,
                state,
            )
            return False

        raise RuntimeError(
            f"Unexpected Athena Spark calculation state for {self.calculation_execution_id}: {state!r}"
        )
