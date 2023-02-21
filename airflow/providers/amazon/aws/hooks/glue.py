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

import boto3

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

DEFAULT_LOG_SUFFIX = "output"
FAILURE_LOG_SUFFIX = "error"
# A filter value of ' ' translates to "match all".
# see: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html
DEFAULT_LOG_FILTER = " "
FAILURE_LOG_FILTER = "?ERROR ?Exception"


class GlueJobHook(AwsBaseHook):
    """
    Interact with AWS Glue.
    Provide thick wrapper around :external+boto3:py:class:`boto3.client("glue") <Glue.Client>`.

    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
    :param job_name: unique job name per AWS account
    :param desc: job description
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :param script_location: path to etl script on s3
    :param retry_limit: Maximum number of times to retry this job if it fails
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job
    :param region_name: aws region name (example: us-east-1)
    :param iam_role_name: AWS IAM Role for Glue Job Execution
    :param create_job_kwargs: Extra arguments for Glue Job Creation

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    JOB_POLL_INTERVAL = 6  # polls job status after every JOB_POLL_INTERVAL seconds

    def __init__(
        self,
        s3_bucket: str | None = None,
        job_name: str | None = None,
        desc: str | None = None,
        concurrent_run_limit: int = 1,
        script_location: str | None = None,
        retry_limit: int = 0,
        num_of_dpus: int | float | None = None,
        iam_role_name: str | None = None,
        create_job_kwargs: dict | None = None,
        *args,
        **kwargs,
    ):
        self.job_name = job_name
        self.desc = desc
        self.concurrent_run_limit = concurrent_run_limit
        self.script_location = script_location
        self.retry_limit = retry_limit
        self.s3_bucket = s3_bucket
        self.role_name = iam_role_name
        self.s3_glue_logs = "logs/glue-logs/"
        self.create_job_kwargs = create_job_kwargs or {}

        worker_type_exists = "WorkerType" in self.create_job_kwargs
        num_workers_exists = "NumberOfWorkers" in self.create_job_kwargs

        if worker_type_exists and num_workers_exists:
            if num_of_dpus is not None:
                raise ValueError("Cannot specify num_of_dpus with custom WorkerType")
        elif not worker_type_exists and num_workers_exists:
            raise ValueError("Need to specify custom WorkerType when specifying NumberOfWorkers")
        elif worker_type_exists and not num_workers_exists:
            raise ValueError("Need to specify NumberOfWorkers when specifying custom WorkerType")
        elif num_of_dpus is None:
            self.num_of_dpus: int | float = 10
        else:
            self.num_of_dpus = num_of_dpus

        kwargs["client_type"] = "glue"
        super().__init__(*args, **kwargs)

    def create_glue_job_config(self) -> dict:
        if self.s3_bucket is None:
            raise ValueError("Could not initialize glue job, error: Specify Parameter `s3_bucket`")

        default_command = {
            "Name": "glueetl",
            "ScriptLocation": self.script_location,
        }
        command = self.create_job_kwargs.pop("Command", default_command)

        s3_log_path = f"s3://{self.s3_bucket}/{self.s3_glue_logs}{self.job_name}"
        execution_role = self.get_iam_execution_role()

        ret_config = {
            "Name": self.job_name,
            "Description": self.desc,
            "LogUri": s3_log_path,
            "Role": execution_role["Role"]["Arn"],
            "ExecutionProperty": {"MaxConcurrentRuns": self.concurrent_run_limit},
            "Command": command,
            "MaxRetries": self.retry_limit,
            **self.create_job_kwargs,
        }

        if hasattr(self, "num_of_dpus"):
            ret_config["MaxCapacity"] = self.num_of_dpus

        return ret_config

    def list_jobs(self) -> list:
        """
        Get list of Jobs.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_jobs`
        """
        return self.get_conn().get_jobs()

    def get_iam_execution_role(self) -> dict:
        """Get IAM Role for job execution."""
        try:
            iam_client = self.get_session(region_name=self.region_name).client(
                "iam", endpoint_url=self.conn_config.endpoint_url, config=self.config, verify=self.verify
            )
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
            self.log.info("Iam Role Name: %s", self.role_name)
            return glue_execution_role
        except Exception as general_error:
            self.log.error("Failed to create aws glue job, error: %s", general_error)
            raise

    def initialize_job(
        self,
        script_arguments: dict | None = None,
        run_kwargs: dict | None = None,
    ) -> dict[str, str]:
        """
        Initializes connection with AWS Glue to run job.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.start_job_run`
        """
        script_arguments = script_arguments or {}
        run_kwargs = run_kwargs or {}

        try:
            job_name = self.create_or_update_glue_job()
            return self.get_conn().start_job_run(JobName=job_name, Arguments=script_arguments, **run_kwargs)
        except Exception as general_error:
            self.log.error("Failed to run aws glue job, error: %s", general_error)
            raise

    def get_job_state(self, job_name: str, run_id: str) -> str:
        """
        Get state of the Glue job.
        The job state can be running, finished, failed, stopped or timeout.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_job_run`

        :param job_name: unique job name per AWS account
        :param run_id: The job-run ID of the predecessor job run
        :return: State of the Glue job
        """
        job_run = self.get_conn().get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=True)
        return job_run["JobRun"]["JobRunState"]

    def print_job_logs(
        self,
        job_name: str,
        run_id: str,
        job_failed: bool = False,
        next_token: str | None = None,
    ) -> str | None:
        """Prints the batch of logs to the Airflow task log and returns nextToken."""
        log_client = boto3.client("logs")
        response = {}

        filter_pattern = FAILURE_LOG_FILTER if job_failed else DEFAULT_LOG_FILTER
        log_group_prefix = self.conn.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["LogGroupName"]
        log_group_suffix = FAILURE_LOG_SUFFIX if job_failed else DEFAULT_LOG_SUFFIX
        log_group_name = f"{log_group_prefix}/{log_group_suffix}"

        try:
            if next_token:
                response = log_client.filter_log_events(
                    logGroupName=log_group_name,
                    logStreamNames=[run_id],
                    filterPattern=filter_pattern,
                    nextToken=next_token,
                )
            else:
                response = log_client.filter_log_events(
                    logGroupName=log_group_name,
                    logStreamNames=[run_id],
                    filterPattern=filter_pattern,
                )
            if len(response["events"]):
                messages = "\t".join([event["message"] for event in response["events"]])
                self.log.info("Glue Job Run Logs:\n\t%s", messages)

        except log_client.exceptions.ResourceNotFoundException:
            self.log.warning(
                "No new Glue driver logs found. This might be because there are no new logs, "
                "or might be an error.\nIf the error persists, check the CloudWatch dashboard "
                f"at: https://{self.conn_region_name}.console.aws.amazon.com/cloudwatch/home"
            )

        # If no new log events are available, filter_log_events will return None.
        # In that case, check the same token again next pass.
        return response.get("nextToken") or next_token

    def job_completion(self, job_name: str, run_id: str, verbose: bool = False) -> dict[str, str]:
        """
        Waits until Glue job with job_name completes or fails and return final state if finished.
        Raises AirflowException when the job failed.

        :param job_name: unique job name per AWS account
        :param run_id: The job-run ID of the predecessor job run
        :param verbose: If True, more Glue Job Run logs show in the Airflow Task Logs.  (default: False)
        :return: Dict of JobRunState and JobRunId
        """
        failed_states = ["FAILED", "TIMEOUT"]
        finished_states = ["SUCCEEDED", "STOPPED"]
        next_log_token = None
        job_failed = False

        while True:
            try:
                job_run_state = self.get_job_state(job_name, run_id)
                if job_run_state in finished_states:
                    self.log.info("Exiting Job %s Run State: %s", run_id, job_run_state)
                    return {"JobRunState": job_run_state, "JobRunId": run_id}
                if job_run_state in failed_states:
                    job_failed = True
                    job_error_message = f"Exiting Job {run_id} Run State: {job_run_state}"
                    self.log.info(job_error_message)
                    raise AirflowException(job_error_message)
                else:
                    self.log.info(
                        "Polling for AWS Glue Job %s current run state with status %s",
                        job_name,
                        job_run_state,
                    )
                    time.sleep(self.JOB_POLL_INTERVAL)
            finally:
                if verbose:
                    next_log_token = self.print_job_logs(
                        job_name=job_name,
                        run_id=run_id,
                        job_failed=job_failed,
                        next_token=next_log_token,
                    )

    def has_job(self, job_name) -> bool:
        """
        Checks if the job already exists.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.get_job`

        :param job_name: unique job name per AWS account
        :return: Returns True if the job already exists and False if not.
        """
        self.log.info("Checking if job already exists: %s", job_name)

        try:
            self.get_conn().get_job(JobName=job_name)
            return True
        except self.get_conn().exceptions.EntityNotFoundException:
            return False

    def update_job(self, **job_kwargs) -> bool:
        """
        Updates job configurations.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.update_job`

        :param job_kwargs: Keyword args that define the configurations used for the job
        :return: True if job was updated and false otherwise
        """
        job_name = job_kwargs.pop("Name")
        current_job = self.get_conn().get_job(JobName=job_name)["Job"]

        update_config = {
            key: value for key, value in job_kwargs.items() if current_job.get(key) != job_kwargs[key]
        }
        if update_config != {}:
            self.log.info("Updating job: %s", job_name)
            self.get_conn().update_job(JobName=job_name, JobUpdate=job_kwargs)
            self.log.info("Updated configurations: %s", update_config)
            return True
        else:
            return False

    def create_or_update_glue_job(self) -> str | None:
        """
        Creates (or updates) and returns the Job name.

        .. seealso::
            - :external+boto3:py:meth:`Glue.Client.update_job`
            - :external+boto3:py:meth:`Glue.Client.create_job`

        :return:Name of the Job
        """
        config = self.create_glue_job_config()

        if self.has_job(self.job_name):
            self.update_job(**config)
        else:
            self.log.info("Creating job: %s", self.job_name)
            self.get_conn().create_job(**config)

        return self.job_name
