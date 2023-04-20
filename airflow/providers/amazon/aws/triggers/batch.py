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

from typing import Any, AsyncIterator

from airflow.providers.amazon.aws.hooks.batch_client import BatchClientAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BatchOperatorTrigger(BaseTrigger):
    """
    Checks for the state of a previously submitted job to AWS Batch.
    BatchOperatorTrigger is fired as deferred class with params to poll the job state in Triggerer

    :param job_name: the name for the job that will run on AWS Batch (templated)
    :param job_definition: the job definition name on AWS Batch
    :param job_queue: the queue name on AWS Batch
    :param overrides: the `containerOverrides` parameter for boto3 (templated)
    :param container_overrides: the `containerOverrides` parameter for boto3 (templated)
    :param node_overrides: the `nodeOverrides` parameter for boto3 (templated)
    :param array_properties: the `arrayProperties` parameter for boto3
    :param parameters: the `parameters` for boto3 (templated)
    :param job_id: the job ID, usually unknown (None) until the
        submit_job operation gets the jobId defined by AWS Batch
    :param waiters: a :class:`.BatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :param region_name: AWS region name to use .
        Override the region_name in connection (if provided)
    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted
    """

    def __init__(
        self,
        job_name: str,
        job_definition: str,
        job_queue: str,
        container_overrides: dict | None = None,
        array_properties: dict | None = None,
        node_overrides: dict | None = None,
        parameters: dict | None = None,
        job_id: str | None = None,
        waiters: Any | None = None,
        max_retries: int | None = None,
        status_retries: int | None = None,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        tags: dict | None = None,
    ):
        super().__init__()
        self.job_name = job_name
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.container_overrides = container_overrides or {}
        self.array_properties = array_properties or {}
        self.node_overrides = node_overrides or {}
        self.parameters = parameters or {}
        self.job_id = job_id
        self.waiters = waiters
        self.max_retries = max_retries
        self.status_retries = status_retries
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.tags = tags or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BatchOperatorTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.batch.BatchOperatorTrigger",
            {
                "job_name": self.job_name,
                "job_definition": self.job_definition,
                "job_queue": self.job_queue,
                "container_overrides": self.container_overrides,
                "array_properties": self.array_properties,
                "node_overrides": self.node_overrides,
                "parameters": self.parameters,
                "job_id": self.job_id,
                "waiters": self.waiters,
                "max_retries": self.max_retries,
                "status_retries": self.status_retries,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "tags": self.tags,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """
        Make async connection using aiobotocore library to AWS Batch,
        periodically poll for the job status on the Triggerer

        The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.

        So the status options that this will poll for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'
        """
        hook = BatchClientAsyncHook(job_id=self.job_id, waiters=self.waiters, aws_conn_id=self.aws_conn_id)
        try:
            response = await hook.monitor_job()
            if response:
                yield TriggerEvent(response)
            else:
                error_message = f"{self.job_id} failed"
                yield TriggerEvent({"status": "error", "message": error_message})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
