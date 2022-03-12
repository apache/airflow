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

import time
import warnings
from typing import Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class GlueJobHook(AwsBaseHook):
    """
    Interact with AWS Glue - create job, trigger, crawler

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
    """

    JOB_POLL_INTERVAL = 6  # polls job status after every JOB_POLL_INTERVAL seconds

    def __init__(
        self,
        s3_bucket: Optional[str] = None,
        job_name: Optional[str] = None,
        desc: Optional[str] = None,
        concurrent_run_limit: int = 1,
        script_location: Optional[str] = None,
        retry_limit: int = 0,
        num_of_dpus: Optional[int] = None,
        iam_role_name: Optional[str] = None,
        create_job_kwargs: Optional[dict] = None,
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
        self.s3_glue_logs = 'logs/glue-logs/'
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
            self.num_of_dpus = 10
        else:
            self.num_of_dpus = num_of_dpus

        kwargs['client_type'] = 'glue'
        super().__init__(*args, **kwargs)

    def list_jobs(self) -> List:
        """:return: Lists of Jobs"""
        conn = self.get_conn()
        return conn.get_jobs()

    def get_iam_execution_role(self) -> Dict:
        """:return: iam role for job execution"""
        session, endpoint_url = self._get_credentials(region_name=self.region_name)
        iam_client = session.client('iam', endpoint_url=endpoint_url, config=self.config, verify=self.verify)

        try:
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
            self.log.info("Iam Role Name: %s", self.role_name)
            return glue_execution_role
        except Exception as general_error:
            self.log.error("Failed to create aws glue job, error: %s", general_error)
            raise

    def initialize_job(
        self,
        script_arguments: Optional[dict] = None,
        run_kwargs: Optional[dict] = None,
    ) -> Dict[str, str]:
        """
        Initializes connection with AWS Glue
        to run job
        :return:
        """
        glue_client = self.get_conn()
        script_arguments = script_arguments or {}
        run_kwargs = run_kwargs or {}

        try:
            job_name = self.get_or_create_glue_job()
            job_run = glue_client.start_job_run(JobName=job_name, Arguments=script_arguments, **run_kwargs)
            return job_run
        except Exception as general_error:
            self.log.error("Failed to run aws glue job, error: %s", general_error)
            raise

    def get_job_state(self, job_name: str, run_id: str) -> str:
        """
        Get state of the Glue job. The job state can be
        running, finished, failed, stopped or timeout.
        :param job_name: unique job name per AWS account
        :param run_id: The job-run ID of the predecessor job run
        :return: State of the Glue job
        """
        glue_client = self.get_conn()
        job_run = glue_client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=True)
        job_run_state = job_run['JobRun']['JobRunState']
        return job_run_state

    def job_completion(self, job_name: str, run_id: str) -> Dict[str, str]:
        """
        Waits until Glue job with job_name completes or
        fails and return final state if finished.
        Raises AirflowException when the job failed
        :param job_name: unique job name per AWS account
        :param run_id: The job-run ID of the predecessor job run
        :return: Dict of JobRunState and JobRunId
        """
        failed_states = ['FAILED', 'TIMEOUT']
        finished_states = ['SUCCEEDED', 'STOPPED']

        while True:
            job_run_state = self.get_job_state(job_name, run_id)
            if job_run_state in finished_states:
                self.log.info("Exiting Job %s Run State: %s", run_id, job_run_state)
                return {'JobRunState': job_run_state, 'JobRunId': run_id}
            if job_run_state in failed_states:
                job_error_message = "Exiting Job " + run_id + " Run State: " + job_run_state
                self.log.info(job_error_message)
                raise AirflowException(job_error_message)
            else:
                self.log.info(
                    "Polling for AWS Glue Job %s current run state with status %s", job_name, job_run_state
                )
                time.sleep(self.JOB_POLL_INTERVAL)

    def get_or_create_glue_job(self) -> str:
        """
        Creates(or just returns) and returns the Job name
        :return:Name of the Job
        """
        glue_client = self.get_conn()
        try:
            get_job_response = glue_client.get_job(JobName=self.job_name)
            self.log.info("Job Already exist. Returning Name of the job")
            return get_job_response['Job']['Name']

        except glue_client.exceptions.EntityNotFoundException:
            self.log.info("Job doesn't exist. Now creating and running AWS Glue Job")
            if self.s3_bucket is None:
                raise AirflowException('Could not initialize glue job, error: Specify Parameter `s3_bucket`')
            s3_log_path = f's3://{self.s3_bucket}/{self.s3_glue_logs}{self.job_name}'
            execution_role = self.get_iam_execution_role()
            try:
                if "WorkerType" in self.create_job_kwargs and "NumberOfWorkers" in self.create_job_kwargs:
                    create_job_response = glue_client.create_job(
                        Name=self.job_name,
                        Description=self.desc,
                        LogUri=s3_log_path,
                        Role=execution_role['Role']['Arn'],
                        ExecutionProperty={"MaxConcurrentRuns": self.concurrent_run_limit},
                        Command={"Name": "glueetl", "ScriptLocation": self.script_location},
                        MaxRetries=self.retry_limit,
                        **self.create_job_kwargs,
                    )
                else:
                    create_job_response = glue_client.create_job(
                        Name=self.job_name,
                        Description=self.desc,
                        LogUri=s3_log_path,
                        Role=execution_role['Role']['Arn'],
                        ExecutionProperty={"MaxConcurrentRuns": self.concurrent_run_limit},
                        Command={"Name": "glueetl", "ScriptLocation": self.script_location},
                        MaxRetries=self.retry_limit,
                        MaxCapacity=self.num_of_dpus,
                        **self.create_job_kwargs,
                    )
                return create_job_response['Name']
            except Exception as general_error:
                self.log.error("Failed to create aws glue job, error: %s", general_error)
                raise


class AwsGlueJobHook(GlueJobHook):
    """
    This hook is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.glue.GlueJobHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This hook is deprecated. "
            "Please use :class:`airflow.providers.amazon.aws.hooks.glue.GlueJobHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
