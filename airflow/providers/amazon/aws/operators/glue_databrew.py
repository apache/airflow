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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue_databrew import GlueDataBrewHook
from airflow.providers.amazon.aws.triggers.glue_databrew import GlueDataBrewJobCompleteTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueDataBrewStartJobOperator(BaseOperator):
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
    :param delay: Time in seconds to wait between status checks. Default is 30.
    :return: dictionary with key run_id and value of the resulting job's run_id.
    """

    template_fields: Sequence[str] = (
        "job_name",
        "wait_for_completion",
        "delay",
        "deferrable",
    )

    def __init__(
        self,
        job_name: str,
        wait_for_completion: bool = True,
        delay: int = 30,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.delay = delay
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> GlueDataBrewHook:
        return GlueDataBrewHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context):
        job = self.hook.conn.start_job_run(Name=self.job_name)
        run_id = job["RunId"]

        self.log.info("AWS Glue DataBrew Job: %s. Run Id: %s submitted.", self.job_name, run_id)

        if self.deferrable:
            self.log.info("Deferring job %s with run_id %s", self.job_name, run_id)
            self.defer(
                trigger=GlueDataBrewJobCompleteTrigger(
                    aws_conn_id=self.aws_conn_id, job_name=self.job_name, run_id=run_id, delay=self.delay
                ),
                method_name="execute_complete",
            )

        elif self.wait_for_completion:
            self.log.info(
                "Waiting for AWS Glue DataBrew Job: %s. Run Id: %s to complete.", self.job_name, run_id
            )
            status = self.hook.job_completion(job_name=self.job_name, delay=self.delay, run_id=run_id)
            self.log.info("Glue DataBrew Job: %s status: %s", self.job_name, status)

        return {"run_id": run_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        event = validate_execute_complete_event(event)

        run_id = event.get("run_id", "")
        status = event.get("status", "")

        self.log.info("AWS Glue DataBrew runID: %s completed with status: %s", run_id, status)

        return {"run_id": run_id}
