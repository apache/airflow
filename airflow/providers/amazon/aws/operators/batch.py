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
"""
An Airflow operator for AWS Batch services

.. seealso::

    - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.links.batch import (
    BatchJobDefinitionLink,
    BatchJobDetailsLink,
    BatchJobQueueLink,
)
from airflow.providers.amazon.aws.links.logs import CloudWatchEventsLink
from airflow.providers.amazon.aws.utils import trim_none_values

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BatchOperator(BaseOperator):
    """
    Execute a job on AWS Batch

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BatchOperator`

    :param job_name: the name for the job that will run on AWS Batch (templated)
    :param job_definition: the job definition name on AWS Batch
    :param job_queue: the queue name on AWS Batch
    :param overrides: the `containerOverrides` parameter for boto3 (templated)
    :param array_properties: the `arrayProperties` parameter for boto3
    :param parameters: the `parameters` for boto3 (templated)
    :param job_id: the job ID, usually unknown (None) until the
        submit_job operation gets the jobId defined by AWS Batch
    :param waiters: an :py:class:`.BatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted

    .. note::
        Any custom waiters must return a waiter for these calls:
        .. code-block:: python

            waiter = waiters.get_waiter("JobExists")
            waiter = waiters.get_waiter("JobRunning")
            waiter = waiters.get_waiter("JobComplete")
    """

    ui_color = "#c3dae0"
    arn: str | None = None
    template_fields: Sequence[str] = (
        "job_id",
        "job_name",
        "job_definition",
        "job_queue",
        "overrides",
        "array_properties",
        "parameters",
        "waiters",
        "tags",
        "wait_for_completion",
    )
    template_fields_renderers = {"overrides": "json", "parameters": "json"}

    @property
    def operator_extra_links(self):
        op_extra_links = [BatchJobDetailsLink()]
        if self.wait_for_completion:
            op_extra_links.extend([BatchJobDefinitionLink(), BatchJobQueueLink()])
        if not self.array_properties:
            # There is no CloudWatch Link to the parent Batch Job available.
            op_extra_links.append(CloudWatchEventsLink())

        return tuple(op_extra_links)

    def __init__(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_queue: str,
        overrides: dict,
        array_properties: dict | None = None,
        parameters: dict | None = None,
        job_id: str | None = None,
        waiters: Any | None = None,
        max_retries: int | None = None,
        status_retries: int | None = None,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        tags: dict | None = None,
        wait_for_completion: bool = True,
        **kwargs,
    ):

        BaseOperator.__init__(self, **kwargs)
        self.job_id = job_id
        self.job_name = job_name
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.overrides = overrides or {}
        self.array_properties = array_properties or {}
        self.parameters = parameters or {}
        self.waiters = waiters
        self.tags = tags or {}
        self.wait_for_completion = wait_for_completion
        self.hook = BatchClientHook(
            max_retries=max_retries,
            status_retries=status_retries,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def execute(self, context: Context):
        """
        Submit and monitor an AWS Batch job

        :raises: AirflowException
        """
        self.submit_job(context)

        if self.wait_for_completion:
            self.monitor_job(context)

        return self.job_id

    def on_kill(self):
        response = self.hook.client.terminate_job(jobId=self.job_id, reason="Task killed by the user")
        self.log.info("AWS Batch job (%s) terminated: %s", self.job_id, response)

    def submit_job(self, context: Context):
        """
        Submit an AWS Batch job

        :raises: AirflowException
        """
        self.log.info(
            "Running AWS Batch job - job definition: %s - on queue %s",
            self.job_definition,
            self.job_queue,
        )
        self.log.info("AWS Batch job - container overrides: %s", self.overrides)

        try:
            response = self.hook.client.submit_job(
                jobName=self.job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                arrayProperties=self.array_properties,
                parameters=self.parameters,
                containerOverrides=self.overrides,
                tags=self.tags,
            )
        except Exception as e:
            self.log.error(
                "AWS Batch job failed submission - job definition: %s - on queue %s",
                self.job_definition,
                self.job_queue,
            )
            raise AirflowException(e)

        self.job_id = response["jobId"]
        self.log.info("AWS Batch job (%s) started: %s", self.job_id, response)
        BatchJobDetailsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            job_id=self.job_id,
        )

    def monitor_job(self, context: Context):
        """
        Monitor an AWS Batch job
        monitor_job can raise an exception or an AirflowTaskTimeout can be raised if execution_timeout
        is given while creating the task. These exceptions should be handled in taskinstance.py
        instead of here like it was previously done

        :raises: AirflowException
        """
        if not self.job_id:
            raise AirflowException("AWS Batch job - job_id was not found")

        try:
            job_desc = self.hook.get_job_description(self.job_id)
            job_definition_arn = job_desc["jobDefinition"]
            job_queue_arn = job_desc["jobQueue"]
            self.log.info(
                "AWS Batch job (%s) Job Definition ARN: %r, Job Queue ARN: %r",
                self.job_id,
                job_definition_arn,
                job_queue_arn,
            )
        except KeyError:
            self.log.warning("AWS Batch job (%s) can't get Job Definition ARN and Job Queue ARN", self.job_id)
        else:
            BatchJobDefinitionLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                job_definition_arn=job_definition_arn,
            )
            BatchJobQueueLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                job_queue_arn=job_queue_arn,
            )

        if self.waiters:
            self.waiters.wait_for_job(self.job_id)
        else:
            self.hook.wait_for_job(self.job_id)

        awslogs = self.hook.get_job_awslogs_info(self.job_id)
        if awslogs:
            self.log.info("AWS Batch job (%s) CloudWatch Events details found: %s", self.job_id, awslogs)
            CloudWatchEventsLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                **awslogs,
            )

        self.hook.check_job_success(self.job_id)
        self.log.info("AWS Batch job (%s) succeeded", self.job_id)


class BatchCreateComputeEnvironmentOperator(BaseOperator):
    """
    Create an AWS Batch compute environment

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BatchCreateComputeEnvironmentOperator`

    :param compute_environment_name: the name of the AWS batch compute environment (templated)

    :param environment_type: the type of the compute-environment

    :param state: the state of the compute-environment

    :param compute_resources: details about the resources managed by the compute-environment (templated).
        See more details here
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.create_compute_environment

    :param unmanaged_v_cpus: the maximum number of vCPU for an unmanaged compute environment.
        This parameter is only supported when the ``type`` parameter is set to ``UNMANAGED``.

    :param service_role: the IAM role that allows Batch to make calls to other AWS services on your behalf
        (templated)

    :param tags: the tags that you apply to the compute-environment to help you categorize and organize your
        resources

    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None

    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None

    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.

    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    """

    template_fields: Sequence[str] = (
        "compute_environment_name",
        "compute_resources",
        "service_role",
    )
    template_fields_renderers = {"compute_resources": "json"}

    def __init__(
        self,
        compute_environment_name: str,
        environment_type: str,
        state: str,
        compute_resources: dict,
        unmanaged_v_cpus: int | None = None,
        service_role: str | None = None,
        tags: dict | None = None,
        max_retries: int | None = None,
        status_retries: int | None = None,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.compute_environment_name = compute_environment_name
        self.environment_type = environment_type
        self.state = state
        self.unmanaged_v_cpus = unmanaged_v_cpus
        self.compute_resources = compute_resources
        self.service_role = service_role
        self.tags = tags or {}
        self.max_retries = max_retries
        self.status_retries = status_retries
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    @cached_property
    def hook(self):
        """Create and return a BatchClientHook"""
        return BatchClientHook(
            max_retries=self.max_retries,
            status_retries=self.status_retries,
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )

    def execute(self, context: Context):
        """Create an AWS batch compute environment"""
        kwargs: dict[str, Any] = {
            "computeEnvironmentName": self.compute_environment_name,
            "type": self.environment_type,
            "state": self.state,
            "unmanagedvCpus": self.unmanaged_v_cpus,
            "computeResources": self.compute_resources,
            "serviceRole": self.service_role,
            "tags": self.tags,
        }
        self.hook.client.create_compute_environment(**trim_none_values(kwargs))

        self.log.info("AWS Batch compute environment created successfully")
