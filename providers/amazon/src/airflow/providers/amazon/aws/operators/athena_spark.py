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

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.sdk import Context


class AthenaSparkOperator(AwsBaseOperator[AthenaHook]):
    """
    Run an Apache Spark calculation in an Amazon Athena session.

    Submits a calculation, such as PySpark code, via the Athena API, polls until
    the calculation reaches a terminal state, and returns execution metadata.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.athena.AthenaHook`
        - `Athena for Apache Spark
          <https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-api-list.html>`__

    :param session_id: The Athena session ID in which to run the calculation. (templated)
    :param code_block: The calculation code, such as PySpark, to execute. (templated)
    :param description: Optional description of the calculation. Defaults to None.
    :param client_request_token: Optional idempotency token for the submission. Defaults to None.
    :param waiter_delay: Seconds to wait between status checks. Defaults to 30.
    :param waiter_max_attempts: Maximum number of polling attempts before timing out. Defaults to 120.
        To limit total task time, use execution_timeout on the task as well.
    :param log_query: Whether to log submission details. Defaults to True.
    :param aws_conn_id: The Airflow connection used for AWS credentials. Defaults to ``aws_default``.
    :param region_name: AWS region. If not set, default boto3 behavior is used. Defaults to None.
    :param verify: Whether to verify SSL certificates. Defaults to None.
    :param botocore_config: Optional botocore configuration dict. Defaults to None.
    """

    aws_hook_class = AthenaHook
    ui_color = "#44b5e2"
    template_fields: Sequence[str] = aws_template_fields("session_id", "code_block", "description")
    template_ext: Sequence[str] = (".py",)
    template_fields_renderers = {"code_block": "python"}

    def __init__(
        self,
        *,
        session_id: str,
        code_block: str,
        description: str | None = None,
        client_request_token: str | None = None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 120,
        log_query: bool = True,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
            **kwargs,
        )
        self.session_id = session_id
        self.code_block = code_block
        self.description = description
        self.client_request_token = client_request_token
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.log_query = log_query
        self._calculation_execution_id: str | None = None

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "log_query": self.log_query}

    def execute(self, context: Context) -> dict[str, Any]:
        """Submit the Spark calculation, poll until terminal state, then return metadata."""
        self.log.info("Starting Athena Spark calculation in session %s", self.session_id)

        calculation_execution_id = self.hook.start_spark_calculation(
            session_id=self.session_id,
            code_block=self.code_block,
            description=self.description,
            client_request_token=self.client_request_token,
        )
        self._calculation_execution_id = calculation_execution_id

        self.log.info("Calculation submitted. CalculationExecutionId: %s", calculation_execution_id)

        final_state = self._poll_until_terminal(calculation_execution_id)
        return self._handle_terminal_state(calculation_execution_id, final_state)

    def _poll_until_terminal(self, calculation_execution_id: str) -> str:
        """Poll calculation status until a terminal state or timeout."""
        for attempt in range(1, self.waiter_max_attempts + 1):
            state = self.hook.check_spark_calculation_status(calculation_execution_id)

            if state is None:
                raise RuntimeError(
                    f"Malformed or missing status for calculation {calculation_execution_id}. "
                    "Cannot continue polling."
                )

            self.log.info(
                "CalculationExecutionId: %s, current state: %s (attempt %d/%d)",
                calculation_execution_id,
                state,
                attempt,
                self.waiter_max_attempts,
            )

            if state in AthenaHook.SPARK_TERMINAL_STATES:
                return state

            if attempt != self.waiter_max_attempts:
                time.sleep(self.waiter_delay)

        self._stop_calculation(calculation_execution_id)
        raise RuntimeError(
            f"Polling timed out after {self.waiter_max_attempts} attempts for calculation "
            f"{calculation_execution_id}. Use execution_timeout or increase waiter_max_attempts."
        )

    def _stop_calculation(self, calculation_execution_id: str) -> None:
        self.log.info("Stopping Athena Spark calculation %s", calculation_execution_id)
        try:
            self.hook.stop_spark_calculation(calculation_execution_id)
        except Exception:
            self.log.warning(
                "Failed to stop Athena Spark calculation %s",
                calculation_execution_id,
                exc_info=True,
            )

    def _handle_terminal_state(self, calculation_execution_id: str, state: str) -> dict[str, Any]:
        """Resolve terminal state: raise on failure/cancel, build and return metadata."""
        reason = self.hook.get_spark_calculation_state_change_reason(calculation_execution_id)
        execution_info = self.hook.get_spark_calculation_info(calculation_execution_id)
        status = execution_info.get("Status", {})
        result_info = execution_info.get("Result", {})

        submission_time = status.get("SubmissionDateTime")
        completion_time = status.get("CompletionDateTime")

        result = {
            "calculation_execution_id": calculation_execution_id,
            "state": state,
            "state_change_reason": reason,
            "submission_time": str(submission_time) if submission_time else None,
            "completion_time": str(completion_time) if completion_time else None,
            "session_id": execution_info.get("SessionId") or self.session_id,
            "working_directory": execution_info.get("WorkingDirectory"),
            "stdout_s3_uri": result_info.get("StdOutS3Uri"),
            "stderr_s3_uri": result_info.get("StdErrorS3Uri"),
            "result_s3_uri": result_info.get("ResultS3Uri"),
            "result_type": result_info.get("ResultType"),
        }

        if state in AthenaHook.SPARK_FAILURE_STATES:
            self.log.error(
                "Calculation failed. CalculationExecutionId: %s, state: %s, reason: %s",
                calculation_execution_id,
                state,
                reason,
            )
            raise RuntimeError(
                f"Athena Spark calculation ended in {state}. "
                f"CalculationExecutionId: {calculation_execution_id}. "
                f"Reason: {reason or 'No reason provided.'}"
            )

        if state not in AthenaHook.SPARK_SUCCESS_STATES:
            raise RuntimeError(
                f"Unexpected terminal state: {state} for calculation {calculation_execution_id}. "
                f"Expected one of: {', '.join(AthenaHook.SPARK_TERMINAL_STATES)}."
            )

        self.log.info(
            "Calculation completed successfully. CalculationExecutionId: %s",
            calculation_execution_id,
        )
        return result

    def on_kill(self) -> None:
        """Request cancellation of the calculation when the task is killed."""
        if self._calculation_execution_id:
            self.log.info(
                "Received kill signal for Athena Spark calculation %s", self._calculation_execution_id
            )
            self._stop_calculation(self._calculation_execution_id)
