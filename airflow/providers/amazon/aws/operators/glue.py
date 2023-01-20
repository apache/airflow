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

import os.path
import urllib.parse
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.glue import GlueJobRunDetailsLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueJobOperator(BaseOperator):
    """
    Creates an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala

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
    :param iam_role_name: AWS IAM Role for Glue Job Execution
    :param create_job_kwargs: Extra arguments for Glue Job Creation
    :param run_job_kwargs: Extra arguments for Glue Job Run
    :param wait_for_completion: Whether or not wait for job run completion. (default: True)
    :param verbose: If True, Glue Job Run logs show in the Airflow Task Logs.  (default: False)
    """

    template_fields: Sequence[str] = (
        "job_name",
        "script_location",
        "script_args",
        "create_job_kwargs",
        "s3_bucket",
        "iam_role_name",
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
        num_of_dpus: int | None = None,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        s3_bucket: str | None = None,
        iam_role_name: str | None = None,
        create_job_kwargs: dict | None = None,
        run_job_kwargs: dict | None = None,
        wait_for_completion: bool = True,
        verbose: bool = False,
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
        self.s3_protocol = "s3://"
        self.s3_artifacts_prefix = "artifacts/glue-scripts/"
        self.create_job_kwargs = create_job_kwargs
        self.run_job_kwargs = run_job_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.verbose = verbose

    def execute(self, context: Context):
        """
        Executes AWS Glue Job from Airflow

        :return: the id of the current glue job.
        """
        if self.script_location is None:
            s3_script_location = None
        elif not self.script_location.startswith(self.s3_protocol):
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            script_name = os.path.basename(self.script_location)
            s3_hook.load_file(
                self.script_location, self.s3_artifacts_prefix + script_name, bucket_name=self.s3_bucket
            )
            s3_script_location = f"s3://{self.s3_bucket}/{self.s3_artifacts_prefix}{script_name}"
        else:
            s3_script_location = self.script_location
        glue_job = GlueJobHook(
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
            create_job_kwargs=self.create_job_kwargs,
        )
        self.log.info(
            "Initializing AWS Glue Job: %s. Wait for completion: %s",
            self.job_name,
            self.wait_for_completion,
        )
        glue_job_run = glue_job.initialize_job(self.script_args, self.run_job_kwargs)
        glue_job_run_url = GlueJobRunDetailsLink.format_str.format(
            aws_domain=GlueJobRunDetailsLink.get_aws_domain(glue_job.conn_partition),
            region_name=glue_job.conn_region_name,
            job_name=urllib.parse.quote(self.job_name, safe=""),
            job_run_id=glue_job_run["JobRunId"],
        )
        GlueJobRunDetailsLink.persist(
            context=context,
            operator=self,
            region_name=glue_job.conn_region_name,
            aws_partition=glue_job.conn_partition,
            job_name=urllib.parse.quote(self.job_name, safe=""),
            job_run_id=glue_job_run["JobRunId"],
        )
        self.log.info("You can monitor this Glue Job run at: %s", glue_job_run_url)
        if self.wait_for_completion:
            glue_job_run = glue_job.job_completion(self.job_name, glue_job_run["JobRunId"], self.verbose)
            self.log.info(
                "AWS Glue Job: %s status: %s. Run Id: %s",
                self.job_name,
                glue_job_run["JobRunState"],
                glue_job_run["JobRunId"],
            )
        else:
            self.log.info("AWS Glue Job: %s. Run Id: %s", self.job_name, glue_job_run["JobRunId"])
        return glue_job_run["JobRunId"]
