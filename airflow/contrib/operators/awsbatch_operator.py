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
from typing import Optional

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.typing import Protocol
from airflow.utils.decorators import apply_defaults


class BatchProtocol(Protocol):
    def submit_job(self, jobName, jobQueue, jobDefinition, containerOverrides):
        ...

    def get_waiter(self, x: str):
        ...

    def describe_jobs(self, jobs):
        ...

    def terminate_job(self, jobId: str, reason: str):
        ...


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
        containerOverrides (templated):
        http://boto3.readthedocs.io/en/latest/reference/services/batch.html#submit_job
    :type overrides: dict
    :param array_properties: the same parameter that boto3 will receive on
        arrayProperties:
        http://boto3.readthedocs.io/en/latest/reference/services/batch.html#submit_job
    :type array_properties: dict
    :param max_retries: exponential backoff retries while waiter is not
        merged, 4200 = 48 hours
    :type max_retries: int
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str
    """

    ui_color = '#c3dae0'
    client = None  # type: Optional[BatchProtocol]
    arn = None  # type: Optional[str]
    template_fields = ('job_name', 'overrides',)

    @apply_defaults
    def __init__(self, job_name, job_definition, job_queue, overrides, array_properties=None,
                 max_retries=4200, aws_conn_id=None, region_name=None, **kwargs):
        super().__init__(**kwargs)

        self.job_name = job_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.overrides = overrides
        self.array_properties = array_properties or {}
        self.max_retries = max_retries

        self.jobId = None  # pylint: disable=invalid-name
        self.jobName = None  # pylint: disable=invalid-name

        self.hook = self.get_hook()

    def execute(self, context):
        self.log.info(
            'Running AWS Batch Job - Job definition: %s - on queue %s',
            self.job_definition, self.job_queue
        )
        self.log.info('AWSBatchOperator overrides: %s', self.overrides)

        self.client = self.hook.get_client_type(
            'batch',
            region_name=self.region_name
        )

        try:
            response = self.client.submit_job(
                jobName=self.job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                arrayProperties=self.array_properties,
                containerOverrides=self.overrides)

            self.log.info('AWS Batch Job started: %s', response)

            self.jobId = response['jobId']
            self.jobName = response['jobName']

            self._wait_for_task_ended()

            self._check_success_task()

            self.log.info('AWS Batch Job has been successfully executed: %s', response)
        except Exception as e:
            self.log.info('AWS Batch Job has failed executed')
            raise AirflowException(e)

    def _wait_for_task_ended(self):
        """
        Try to use a waiter from the below pull request

            * https://github.com/boto/botocore/pull/1307

        If the waiter is not available apply a exponential backoff

            * docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        try:
            waiter = self.client.get_waiter('job_execution_complete')
            waiter.config.max_attempts = sys.maxsize  # timeout is managed by airflow
            waiter.wait(jobs=[self.jobId])
        except ValueError:
            # If waiter not available use expo

            # Allow a batch job some time to spin up.  A random interval
            # decreases the chances of exceeding an AWS API throttle
            # limit when there are many concurrent tasks.
            pause = randint(5, 30)

            retries = 1
            while retries <= self.max_retries:
                self.log.info('AWS Batch job (%s) status check (%d of %d) in the next %.2f seconds',
                              self.jobId, retries, self.max_retries, pause)
                sleep(pause)

                response = self.client.describe_jobs(jobs=[self.jobId])
                status = response['jobs'][-1]['status']
                self.log.info('AWS Batch job (%s) status: %s', self.jobId, status)
                if status in ['SUCCEEDED', 'FAILED']:
                    break

                retries += 1
                pause = 1 + pow(retries * 0.3, 2)

    def _check_success_task(self):
        response = self.client.describe_jobs(
            jobs=[self.jobId],
        )

        self.log.info('AWS Batch stopped, check status: %s', response)
        if len(response.get('jobs')) < 1:
            raise AirflowException('No job found for {}'.format(response))

        for job in response['jobs']:
            job_status = job['status']
            if job_status == 'FAILED':
                reason = job['statusReason']
                raise AirflowException('Job failed with status {}'.format(reason))
            elif job_status in [
                'SUBMITTED',
                'PENDING',
                'RUNNABLE',
                'STARTING',
                'RUNNING'
            ]:
                raise AirflowException(
                    'This task is still pending {}'.format(job_status))

    def get_hook(self):
        return AwsHook(
            aws_conn_id=self.aws_conn_id
        )

    def on_kill(self):
        response = self.client.terminate_job(
            jobId=self.jobId,
            reason='Task killed by the user')

        self.log.info(response)
