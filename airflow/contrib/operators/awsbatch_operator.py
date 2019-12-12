# -*- coding: utf-8 -*-
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
#
import sys

from math import pow
from random import randint
from time import sleep

import botocore.exceptions
import botocore.waiter

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class AWSBatchOperator(BaseOperator):
    """
    Execute a job on AWS Batch Service

    .. warning: the queue parameter was renamed to job_queue to segregate the
                internal CeleryExecutor queue from the AWS Batch internal queue.

    :param job_name: the name for the job that will run on AWS Batch (templated)
    :type job_name: str
    :param job_definition: the job definition name on AWS Batch
    :type job_definition: str
    :param job_queue: the queue name on AWS Batch
    :type job_queue: str
    :param overrides: the same parameter that boto3 will receive on
        containerOverrides (templated)
        http://boto3.readthedocs.io/en/latest/reference/services/batch.html#Batch.Client.submit_job
    :type overrides: dict
    :param array_properties: the same parameter that boto3 will receive on
        arrayProperties
        http://boto3.readthedocs.io/en/latest/reference/services/batch.html#Batch.Client.submit_job
    :type array_properties: dict
    :param parameters: the same parameter that boto3 will receive on
        parameters (templated)
        http://boto3.readthedocs.io/en/latest/reference/services/batch.html#Batch.Client.submit_job
    :type parameters: dict
    :param max_retries: exponential backoff retries while waiter is not
        merged, 4200 = 48 hours
    :type max_retries: int
    :param status_retries: number of retries to get job description (status), 10
    :type status_retries: int
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str
    """

    MAX_RETRIES = 4200
    STATUS_RETRIES = 10

    ui_color = "#c3dae0"
    client = None
    arn = None
    template_fields = (
        "job_name",
        "overrides",
        "parameters",
    )

    @apply_defaults
    def __init__(
        self,
        job_name,
        job_definition,
        job_queue,
        overrides,
        array_properties=None,
        parameters=None,
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        aws_conn_id=None,
        region_name=None,
        **kwargs
    ):
        super(AWSBatchOperator, self).__init__(**kwargs)

        self.job_name = job_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.overrides = overrides
        self.array_properties = array_properties or {}
        self.parameters = parameters
        self.max_retries = max_retries
        self.status_retries = status_retries

        self.jobId = None  # pylint: disable=invalid-name
        self.jobName = None  # pylint: disable=invalid-name

        self.hook = self.get_hook()

    def execute(self, context):
        self.log.info(
            "Running AWS Batch Job - Job definition: %s - on queue %s",
            self.job_definition,
            self.job_queue,
        )
        self.log.info("AWSBatchOperator overrides: %s", self.overrides)

        self.client = self.hook.get_client_type("batch", region_name=self.region_name)

        try:

            response = self.client.submit_job(
                jobName=self.job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                arrayProperties=self.array_properties,
                parameters=self.parameters,
                containerOverrides=self.overrides,
            )

            self.log.info("AWS Batch Job started: %s", response)

            self.jobId = response["jobId"]
            self.jobName = response["jobName"]

            self._wait_for_task_ended()
            self._check_success_task()

            self.log.info("AWS Batch Job has been successfully executed: %s", response)
        except Exception as e:
            self.log.info("AWS Batch Job has failed executed")
            raise AirflowException(e)

    def _wait_for_task_ended(self):
        """
        Try to use a waiter from the below pull request

            * https://github.com/boto/botocore/pull/1307

        If the waiter is not available apply a exponential backoff

            * docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        try:
            waiter = self.client.get_waiter("job_execution_complete")
            waiter.config.max_attempts = sys.maxsize  # timeout is managed by airflow
            waiter.wait(jobs=[self.jobId])
        except ValueError:
            self._poll_for_task_ended()

    def _poll_for_task_ended(self):
        """
        Poll for job status

            * docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        # Allow a batch job some time to spin up.  A random interval
        # decreases the chances of exceeding an AWS API throttle
        # limit when there are many concurrent tasks.
        pause = randint(5, 30)

        tries = 0
        while tries < self.max_retries:
            tries += 1
            self.log.info(
                "AWS Batch job (%s) status check (%d of %d) in the next %.2f seconds",
                self.jobId,
                tries,
                self.max_retries,
                pause,
            )
            sleep(pause)

            response = self._get_job_description()
            jobs = response.get("jobs")
            status = jobs[-1]["status"]  # check last job status
            self.log.info("AWS Batch job (%s) status: %s", self.jobId, status)

            # status options: 'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'
            if status in ["SUCCEEDED", "FAILED"]:
                break

            pause = 1 + pow(tries * 0.3, 2)

    def _get_job_description(self):
        """
        Get job description

            * https://docs.aws.amazon.com/batch/latest/APIReference/API_DescribeJobs.html
            * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
        """
        tries = 0
        while tries < self.status_retries:
            tries += 1
            try:
                response = self.client.describe_jobs(jobs=[self.jobId])  # type: ignore
                if response and response.get("jobs"):
                    return response
                else:
                    self.log.error("Job description has no jobs (%s): %s", self.jobId, response)
            except botocore.exceptions.ClientError as err:
                response = err.response
                self.log.error("Job description error (%s): %s", self.jobId, response)
                if tries < self.status_retries:
                    error = response.get("Error", {})
                    if error.get("Code") == "TooManyRequestsException":
                        pause = randint(1, 10)  # avoid excess requests with a random pause
                        self.log.info(
                            "AWS Batch job (%s) status retry (%d of %d) in the next %.2f seconds",
                            self.jobId,
                            tries,
                            self.status_retries,
                            pause,
                        )
                        sleep(pause)
                        continue

        msg = "Failed to get job description ({})".format(self.jobId)
        raise AirflowException(msg)

    def _check_success_task(self):
        """
        Check the final status of the batch job; the job status options are:
        'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'
        """
        response = self._get_job_description()
        jobs = response.get("jobs")

        matching_jobs = [job for job in jobs if job["jobId"] == self.jobId]
        if not matching_jobs:
            raise AirflowException(
                "Job ({}) has no job description {}".format(self.jobId, response)
            )

        job = matching_jobs[0]
        self.log.info("AWS Batch stopped, check status: %s", job)
        job_status = job["status"]
        if job_status == "FAILED":
            reason = job["statusReason"]
            raise AirflowException("Job ({}) failed with status {}".format(self.jobId, reason))
        elif job_status in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]:
            raise AirflowException(
                "Job ({}) is still pending {}".format(self.jobId, job_status)
            )

    def get_hook(self):
        return AwsHook(aws_conn_id=self.aws_conn_id)

    def on_kill(self):
        response = self.client.terminate_job(jobId=self.jobId, reason="Task killed by the user")
        self.log.info(response)
