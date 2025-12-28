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
from airflow.providers.amazon.aws.hooks.glue_databrew import GlueDataBrewHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.glue_databrew import GlueDataBrewJobCompleteTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class GlueDataBrewStartJobOperator(AwsBaseOperator[GlueDataBrewHook]):
    """
    Start an AWS Glue DataBrew job.

    AWS Glue DataBrew is a visual data preparation tool that makes it easier
    for data analysts and data scientists to clean and normalize data
    to prepare it for analytics and machine learning (ML).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueDataBrewStartJobOperator`

    :param job_name: unique job name per AWS Account
    :param wait_for_completion: Whether to wait for job run completion. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks. Default is 30.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 60)
    :return: dictionary with key run_id and value of the resulting job's run_id.

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

    aws_hook_class = GlueDataBrewHook

    template_fields: Sequence[str] = aws_template_fields(
        "job_name",
        "wait_for_completion",
        "waiter_delay",
        "waiter_max_attempts",
        "deferrable",
    )

    def __init__(
        self,
        job_name: str,
        wait_for_completion: bool = True,
        delay: int | None = None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context):
        job = self.hook.conn.start_job_run(Name=self.job_name)
        run_id = job["RunId"]

        self.log.info("AWS Glue DataBrew Job: %s. Run Id: %s submitted.", self.job_name, run_id)

        if self.deferrable:
            self.log.info("Deferring job %s with run_id %s", self.job_name, run_id)
            self.defer(
                trigger=GlueDataBrewJobCompleteTrigger(
                    job_name=self.job_name,
                    run_id=run_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info(
                "Waiting for AWS Glue DataBrew Job: %s. Run Id: %s to complete.", self.job_name, run_id
            )
            status = self.hook.job_completion(
                job_name=self.job_name,
                delay=self.waiter_delay,
                run_id=run_id,
                max_attempts=self.waiter_max_attempts,
            )
            self.log.info("Glue DataBrew Job: %s status: %s", self.job_name, status)

        return {"run_id": run_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException("Error while running AWS Glue DataBrew job: %s", validated_event)

        run_id = validated_event.get("run_id", "")
        status = validated_event.get("status", "")

        self.log.info("AWS Glue DataBrew runID: %s completed with status: %s", run_id, status)

        return {"run_id": run_id}
