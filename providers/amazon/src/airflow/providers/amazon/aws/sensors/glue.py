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

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.glue import (
    GlueDataQualityRuleRecommendationRunCompleteTrigger,
    GlueDataQualityRuleSetEvaluationRunCompleteTrigger,
    GlueJobCompleteTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class GlueJobSensor(AwsBaseSensor[GlueJobHook]):
    """
    Waits for an AWS Glue Job to reach any of the status below.

    'FAILED', 'STOPPED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueJobSensor`

    :param job_name: The AWS Glue Job unique name
    :param run_id: The AWS Glue current running job identifier
    :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)
    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 120)
    :param max_retries: Number of times before returning the current state. (default: 60)

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

    SUCCESS_STATES = ("SUCCEEDED",)
    FAILURE_STATES = ("FAILED", "STOPPED", "TIMEOUT")

    aws_hook_class = GlueJobHook
    template_fields: Sequence[str] = aws_template_fields("job_name", "run_id")

    def __init__(
        self,
        *,
        job_name: str,
        run_id: str,
        verbose: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: int = 120,
        max_retries: int = 60,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.run_id = run_id
        self.verbose = verbose
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.aws_conn_id = aws_conn_id
        self.next_log_tokens = GlueJobHook.LogContinuationTokens()

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=GlueJobCompleteTrigger(
                    job_name=self.job_name,
                    run_id=self.run_id,
                    verbose=self.verbose,
                    aws_conn_id=self.aws_conn_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    region_name=self.region_name,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            message = f"Error: AWS Glue Job: {validated_event}"
            raise AirflowException(message)

        self.log.info("AWS Glue Job completed.")

    def poke(self, context: Context) -> bool:
        self.log.info("Poking for job run status :for Glue Job %s and ID %s", self.job_name, self.run_id)
        job_state = self.hook.get_job_state(job_name=self.job_name, run_id=self.run_id)

        try:
            if job_state in self.SUCCESS_STATES:
                self.log.info("Exiting Job %s Run State: %s", self.run_id, job_state)
                return True
            if job_state in self.FAILURE_STATES:
                job_error_message = "Exiting Job %s Run State: %s", self.run_id, job_state
                self.log.info(job_error_message)
                raise AirflowException(job_error_message)
            return False
        finally:
            if self.verbose:
                self.hook.print_job_logs(
                    job_name=self.job_name,
                    run_id=self.run_id,
                    continuation_tokens=self.next_log_tokens,
                )


class GlueDataQualityRuleSetEvaluationRunSensor(AwsBaseSensor[GlueDataQualityHook]):
    """
    Waits for an AWS Glue data quality ruleset evaluation run to reach any of the status below.

    'FAILED', 'STOPPED', 'STOPPING', 'TIMEOUT', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueDataQualityRuleSetEvaluationRunSensor`

    :param evaluation_run_id: The AWS Glue data quality ruleset evaluation run identifier.
    :param verify_result_status: Validate all the ruleset rules evaluation run results,
        If any of the rule status is Fail or Error then an exception is thrown. (default: True)
    :param show_results: Displays all the ruleset rules evaluation run results. (default: True)
    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 120)
    :param max_retries: Number of times before returning the current state. (default: 60)

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

    SUCCESS_STATES = ("SUCCEEDED",)

    FAILURE_STATES = ("FAILED", "STOPPED", "STOPPING", "TIMEOUT")

    aws_hook_class = GlueDataQualityHook
    template_fields: Sequence[str] = aws_template_fields("evaluation_run_id")

    def __init__(
        self,
        *,
        evaluation_run_id: str,
        show_results: bool = True,
        verify_result_status: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: int = 120,
        max_retries: int = 60,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.evaluation_run_id = evaluation_run_id
        self.show_results = show_results
        self.verify_result_status = verify_result_status
        self.aws_conn_id = aws_conn_id
        self.max_retries = max_retries
        self.poke_interval = poke_interval
        self.deferrable = deferrable

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=GlueDataQualityRuleSetEvaluationRunCompleteTrigger(
                    evaluation_run_id=self.evaluation_run_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            message = f"Error: AWS Glue data quality ruleset evaluation run: {validated_event}"
            raise AirflowException(message)

        self.hook.validate_evaluation_run_results(
            evaluation_run_id=validated_event["evaluation_run_id"],
            show_results=self.show_results,
            verify_result_status=self.verify_result_status,
        )

        self.log.info("AWS Glue data quality ruleset evaluation run completed.")

    def poke(self, context: Context):
        self.log.info(
            "Poking for AWS Glue data quality ruleset evaluation run RunId: %s", self.evaluation_run_id
        )

        response = self.hook.conn.get_data_quality_ruleset_evaluation_run(RunId=self.evaluation_run_id)

        status = response.get("Status")

        if status in self.SUCCESS_STATES:
            self.hook.validate_evaluation_run_results(
                evaluation_run_id=self.evaluation_run_id,
                show_results=self.show_results,
                verify_result_status=self.verify_result_status,
            )

            self.log.info(
                "AWS Glue data quality ruleset evaluation run completed RunId: %s Run State: %s",
                self.evaluation_run_id,
                response["Status"],
            )

            return True

        if status in self.FAILURE_STATES:
            job_error_message = (
                f"Error: AWS Glue data quality ruleset evaluation run RunId: {self.evaluation_run_id} Run "
                f"Status: {status}"
                f": {response.get('ErrorString')}"
            )
            self.log.info(job_error_message)
            raise AirflowException(job_error_message)
        return False


class GlueDataQualityRuleRecommendationRunSensor(AwsBaseSensor[GlueDataQualityHook]):
    """
    Waits for an AWS Glue data quality recommendation run to reach any of the status below.

    'FAILED', 'STOPPED', 'STOPPING', 'TIMEOUT', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueDataQualityRuleRecommendationRunSensor`

    :param recommendation_run_id: The AWS Glue data quality rule recommendation run identifier.
    :param show_results: Displays the recommended ruleset (a set of rules), when recommendation run completes. (default: True)
    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 120)
    :param max_retries: Number of times before returning the current state. (default: 60)

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

    SUCCESS_STATES = ("SUCCEEDED",)

    FAILURE_STATES = ("FAILED", "STOPPED", "STOPPING", "TIMEOUT")

    aws_hook_class = GlueDataQualityHook
    template_fields: Sequence[str] = aws_template_fields("recommendation_run_id")

    def __init__(
        self,
        *,
        recommendation_run_id: str,
        show_results: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: int = 120,
        max_retries: int = 60,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.recommendation_run_id = recommendation_run_id
        self.show_results = show_results
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=GlueDataQualityRuleRecommendationRunCompleteTrigger(
                    recommendation_run_id=self.recommendation_run_id,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            message = f"Error: AWS Glue data quality recommendation run: {validated_event}"
            raise AirflowException(message)

        if self.show_results:
            self.hook.log_recommendation_results(run_id=self.recommendation_run_id)

        self.log.info("AWS Glue data quality recommendation run completed.")

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Poking for AWS Glue data quality recommendation run RunId: %s", self.recommendation_run_id
        )

        response = self.hook.conn.get_data_quality_rule_recommendation_run(RunId=self.recommendation_run_id)

        status = response.get("Status")

        if status in self.SUCCESS_STATES:
            if self.show_results:
                self.hook.log_recommendation_results(run_id=self.recommendation_run_id)

            self.log.info(
                "AWS Glue data quality recommendation run completed RunId: %s Run State: %s",
                self.recommendation_run_id,
                response["Status"],
            )

            return True

        if status in self.FAILURE_STATES:
            job_error_message = (
                f"Error: AWS Glue data quality recommendation run RunId: {self.recommendation_run_id} Run "
                f"Status: {status}"
                f": {response.get('ErrorString')}"
            )
            self.log.info(job_error_message)
            raise AirflowException(job_error_message)
        return False
