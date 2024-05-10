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

import os
import urllib.parse
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.glue import GlueJobRunDetailsLink
from airflow.providers.amazon.aws.triggers.glue import GlueJobCompleteTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueJobOperator(BaseOperator):
    """Create an AWS Glue Job.

    AWS Glue is a serverless Spark ETL service for running Spark Jobs on the AWS
    cloud. Language support: Python and Scala.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueJobOperator`

    :param job_name: unique job name per AWS Account
    :param script_location: location of ETL script. Must be a local or S3 path
    :param job_desc: job description details
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :param script_args: etl script arguments and AWS Glue arguments (templated)
    :param retry_limit: The maximum number of times to retry this job if it fails
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job.
    :param region_name: aws region name (example: us-east-1)
    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
    :param iam_role_name: AWS IAM Role for Glue Job Execution. If set `iam_role_arn` must equal None.
    :param iam_role_arn: AWS IAM ARN for Glue Job Execution. If set `iam_role_name` must equal None.
    :param create_job_kwargs: Extra arguments for Glue Job Creation
    :param run_job_kwargs: Extra arguments for Glue Job Run
    :param wait_for_completion: Whether to wait for job run completion. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param verbose: If True, Glue Job Run logs show in the Airflow Task Logs.  (default: False)
    :param update_config: If True, Operator will update job configuration.  (default: False)
    :param replace_script_file: If True, the script file will be replaced in S3. (default: False)
    :param stop_job_run_on_kill: If True, Operator will stop the job run when task is killed.
    """

    template_fields: Sequence[str] = (
        "job_name",
        "script_location",
        "script_args",
        "create_job_kwargs",
        "s3_bucket",
        "iam_role_name",
        "iam_role_arn",
    )
    template_ext: Sequence[str] = ()
    template_fields_renderers = {
        "script_args": "json",
        "create_job_kwargs": "json",
    }
    ui_color = "#ededed"

    operator_extra_links = (GlueJobRunDetailsLink(),)

    def __init__(
        self,
        *,
        job_name: str = "aws_glue_default_job",
        job_desc: str = "AWS Glue Job with Airflow",
        script_location: str | None = None,
        concurrent_run_limit: int | None = None,
        script_args: dict | None = None,
        retry_limit: int = 0,
        num_of_dpus: int | float | None = None,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        s3_bucket: str | None = None,
        iam_role_name: str | None = None,
        iam_role_arn: str | None = None,
        create_job_kwargs: dict | None = None,
        run_job_kwargs: dict | None = None,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        verbose: bool = False,
        replace_script_file: bool = False,
        update_config: bool = False,
        job_poll_interval: int | float = 6,
        stop_job_run_on_kill: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_location = script_location
        self.concurrent_run_limit = concurrent_run_limit or 1
        self.script_args = script_args or {}
        self.retry_limit = retry_limit
        self.num_of_dpus = num_of_dpus
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.iam_role_name = iam_role_name
        self.iam_role_arn = iam_role_arn
        self.s3_protocol = "s3://"
        self.s3_artifacts_prefix = "artifacts/glue-scripts/"
        self.create_job_kwargs = create_job_kwargs
        self.run_job_kwargs = run_job_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.verbose = verbose
        self.update_config = update_config
        self.replace_script_file = replace_script_file
        self.deferrable = deferrable
        self.job_poll_interval = job_poll_interval
        self.stop_job_run_on_kill = stop_job_run_on_kill
        self._job_run_id: str | None = None

    @cached_property
    def glue_job_hook(self) -> GlueJobHook:
        if self.script_location is None:
            s3_script_location = None
        elif not self.script_location.startswith(self.s3_protocol):
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            script_name = os.path.basename(self.script_location)
            s3_hook.load_file(
                self.script_location,
                self.s3_artifacts_prefix + script_name,
                bucket_name=self.s3_bucket,
                replace=self.replace_script_file,
            )
            s3_script_location = f"s3://{self.s3_bucket}/{self.s3_artifacts_prefix}{script_name}"
        else:
            s3_script_location = self.script_location
        return GlueJobHook(
            job_name=self.job_name,
            desc=self.job_desc,
            concurrent_run_limit=self.concurrent_run_limit,
            script_location=s3_script_location,
            retry_limit=self.retry_limit,
            num_of_dpus=self.num_of_dpus,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            s3_bucket=self.s3_bucket,
            iam_role_name=self.iam_role_name,
            iam_role_arn=self.iam_role_arn,
            create_job_kwargs=self.create_job_kwargs,
            update_config=self.update_config,
            job_poll_interval=self.job_poll_interval,
        )

    def execute(self, context: Context):
        """Execute AWS Glue Job from Airflow.

        :return: the current Glue job ID.
        """
        self.log.info(
            "Initializing AWS Glue Job: %s. Wait for completion: %s",
            self.job_name,
            self.wait_for_completion,
        )
        glue_job_run = self.glue_job_hook.initialize_job(self.script_args, self.run_job_kwargs)
        self._job_run_id = glue_job_run["JobRunId"]
        glue_job_run_url = GlueJobRunDetailsLink.format_str.format(
            aws_domain=GlueJobRunDetailsLink.get_aws_domain(self.glue_job_hook.conn_partition),
            region_name=self.glue_job_hook.conn_region_name,
            job_name=urllib.parse.quote(self.job_name, safe=""),
            job_run_id=self._job_run_id,
        )
        GlueJobRunDetailsLink.persist(
            context=context,
            operator=self,
            region_name=self.glue_job_hook.conn_region_name,
            aws_partition=self.glue_job_hook.conn_partition,
            job_name=urllib.parse.quote(self.job_name, safe=""),
            job_run_id=self._job_run_id,
        )
        self.log.info("You can monitor this Glue Job run at: %s", glue_job_run_url)

        if self.deferrable:
            self.defer(
                trigger=GlueJobCompleteTrigger(
                    job_name=self.job_name,
                    run_id=self._job_run_id,
                    verbose=self.verbose,
                    aws_conn_id=self.aws_conn_id,
                    job_poll_interval=self.job_poll_interval,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            glue_job_run = self.glue_job_hook.job_completion(self.job_name, self._job_run_id, self.verbose)
            self.log.info(
                "AWS Glue Job: %s status: %s. Run Id: %s",
                self.job_name,
                glue_job_run["JobRunState"],
                self._job_run_id,
            )
        else:
            self.log.info("AWS Glue Job: %s. Run Id: %s", self.job_name, self._job_run_id)
        return self._job_run_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        event = validate_execute_complete_event(event)

        if event["status"] != "success":
            raise AirflowException(f"Error in glue job: {event}")
        return event["value"]

    def on_kill(self):
        """Cancel the running AWS Glue Job."""
        if self.stop_job_run_on_kill:
            self.log.info("Stopping AWS Glue Job: %s. Run Id: %s", self.job_name, self._job_run_id)
            response = self.glue_job_hook.conn.batch_stop_job_run(
                JobName=self.job_name,
                JobRunIds=[self._job_run_id],
            )
            if not response["SuccessfulSubmissions"]:
                self.log.error("Failed to stop AWS Glue Job: %s. Run Id: %s", self.job_name, self._job_run_id)
