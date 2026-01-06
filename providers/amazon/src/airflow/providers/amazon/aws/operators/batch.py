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
AWS Batch services.

.. seealso::

    - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html
    - https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.links.batch import (
    BatchJobDefinitionLink,
    BatchJobDetailsLink,
    BatchJobQueueLink,
)
from airflow.providers.amazon.aws.links.logs import CloudWatchEventsLink
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.batch import (
    BatchCreateComputeEnvironmentTrigger,
    BatchJobTrigger,
)
from airflow.providers.amazon.aws.utils import trim_none_values, validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher
from airflow.providers.common.compat.sdk import AirflowException, conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class BatchOperator(AwsBaseOperator[BatchClientHook]):
    """
    Execute a job on AWS Batch.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BatchOperator`

    :param job_name: the name for the job that will run on AWS Batch (templated)
    :param job_definition: the job definition name on AWS Batch
    :param job_queue: the queue name on AWS Batch
    :param container_overrides: the `containerOverrides` parameter for boto3 (templated)
    :param ecs_properties_override: the `ecsPropertiesOverride` parameter for boto3 (templated)
    :param eks_properties_override: the `eksPropertiesOverride` parameter for boto3 (templated)
    :param node_overrides: the `nodeOverrides` parameter for boto3 (templated)
    :param share_identifier: The share identifier for the job. Don't specify this parameter if the job queue
        doesn't have a scheduling policy.
    :param scheduling_priority_override: The scheduling priority for the job.
        Jobs with a higher scheduling priority are scheduled before jobs with a lower scheduling priority.
        This overrides any scheduling priority in the job definition
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
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted
    :param deferrable: Run operator in the deferrable mode.
    :param awslogs_enabled: Specifies whether logs from CloudWatch
        should be printed or not, False.
        If it is an array job, only the logs of the first task will be printed.
    :param awslogs_fetch_interval: The interval with which cloudwatch logs are to be fetched, 30 sec.
    :param poll_interval: (Deferrable mode only) Time in seconds to wait between polling.
    :param submit_job_timeout: Execution timeout in seconds for submitted batch job.

    .. note::
        Any custom waiters must return a waiter for these calls:
        .. code-block:: python

            waiter = waiters.get_waiter("JobExists")
            waiter = waiters.get_waiter("JobRunning")
            waiter = waiters.get_waiter("JobComplete")
    """

    aws_hook_class = BatchClientHook

    ui_color = "#c3dae0"
    arn: str | None = None
    template_fields: Sequence[str] = aws_template_fields(
        "job_id",
        "job_name",
        "job_definition",
        "job_queue",
        "container_overrides",
        "array_properties",
        "ecs_properties_override",
        "eks_properties_override",
        "node_overrides",
        "parameters",
        "retry_strategy",
        "waiters",
        "tags",
        "wait_for_completion",
        "awslogs_enabled",
        "awslogs_fetch_interval",
    )
    template_fields_renderers = {
        "container_overrides": "json",
        "parameters": "json",
        "ecs_properties_override": "json",
        "eks_properties_override": "json",
        "node_overrides": "json",
        "retry_strategy": "json",
    }

    operator_extra_links = (
        BatchJobDetailsLink(),
        BatchJobDefinitionLink(),
        BatchJobQueueLink(),
        CloudWatchEventsLink(),
    )

    def __init__(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_queue: str,
        container_overrides: dict | None = None,
        array_properties: dict | None = None,
        ecs_properties_override: dict | None = None,
        eks_properties_override: dict | None = None,
        node_overrides: dict | None = None,
        share_identifier: str | None = None,
        scheduling_priority_override: int | None = None,
        parameters: dict | None = None,
        retry_strategy: dict | None = None,
        job_id: str | None = None,
        waiters: Any | None = None,
        max_retries: int = 4200,
        status_retries: int | None = None,
        tags: dict | None = None,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 30,
        awslogs_enabled: bool = False,
        awslogs_fetch_interval: timedelta = timedelta(seconds=30),
        submit_job_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.job_name = job_name
        self.job_definition = job_definition
        self.job_queue = job_queue

        self.container_overrides = container_overrides
        self.ecs_properties_override = ecs_properties_override
        self.eks_properties_override = eks_properties_override
        self.node_overrides = node_overrides
        self.share_identifier = share_identifier
        self.scheduling_priority_override = scheduling_priority_override
        self.array_properties = array_properties
        self.parameters = parameters or {}
        self.retry_strategy = retry_strategy
        self.waiters = waiters
        self.tags = tags or {}
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.awslogs_enabled = awslogs_enabled
        self.awslogs_fetch_interval = awslogs_fetch_interval
        self.submit_job_timeout = submit_job_timeout

        # params for hook
        self.max_retries = max_retries
        self.status_retries = status_retries

    @property
    def _hook_parameters(self):
        return {
            **super()._hook_parameters,
            "max_retries": self.max_retries,
            "status_retries": self.status_retries,
        }

    def execute(self, context: Context) -> str | None:
        """
        Submit and monitor an AWS Batch job.

        :raises: AirflowException
        """
        self.submit_job(context)

        if self.deferrable:
            if not self.job_id:
                raise AirflowException("AWS Batch job - job_id was not found")

            job = self.hook.get_job_description(self.job_id)
            job_status = job.get("status")
            if job_status == self.hook.SUCCESS_STATE:
                self.log.info("Job completed.")
                return self.job_id
            if job_status == self.hook.FAILURE_STATE:
                raise AirflowException(f"Error while running job: {self.job_id} is in {job_status} state")
            if job_status in self.hook.INTERMEDIATE_STATES:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=BatchJobTrigger(
                        job_id=self.job_id,
                        waiter_max_attempts=self.max_retries,
                        aws_conn_id=self.aws_conn_id,
                        region_name=self.region_name,
                        waiter_delay=self.poll_interval,
                    ),
                    method_name="execute_complete",
                )

            raise AirflowException(f"Unexpected status: {job_status}")

        if self.wait_for_completion:
            self.monitor_job(context)

        return self.job_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error while running job: {validated_event}")

        self.job_id = validated_event["job_id"]

        # Fetch logs if awslogs_enabled
        if self.awslogs_enabled:
            self.monitor_job(context)  # fetch logs, no need to return

        self.log.info("Job completed successfully for job_id: %s", self.job_id)
        return self.job_id

    def on_kill(self):
        response = self.hook.client.terminate_job(jobId=self.job_id, reason="Task killed by the user")
        self.log.info("AWS Batch job (%s) terminated: %s", self.job_id, response)

    def submit_job(self, context: Context):
        """
        Submit an AWS Batch job.

        :raises: AirflowException
        """
        self.log.info(
            "Running AWS Batch job - job definition: %s - on queue %s",
            self.job_definition,
            self.job_queue,
        )

        if self.container_overrides:
            self.log.info("AWS Batch job - container overrides: %s", self.container_overrides)
        if self.array_properties:
            self.log.info("AWS Batch job - array properties: %s", self.array_properties)
        if self.ecs_properties_override:
            self.log.info("AWS Batch job - ECS properties: %s", self.ecs_properties_override)
        if self.eks_properties_override:
            self.log.info("AWS Batch job - EKS properties: %s", self.eks_properties_override)
        if self.node_overrides:
            self.log.info("AWS Batch job - node properties: %s", self.node_overrides)

        args = {
            "jobName": self.job_name,
            "jobQueue": self.job_queue,
            "jobDefinition": self.job_definition,
            "arrayProperties": self.array_properties,
            "parameters": self.parameters,
            "tags": self.tags,
            "containerOverrides": self.container_overrides,
            "ecsPropertiesOverride": self.ecs_properties_override,
            "eksPropertiesOverride": self.eks_properties_override,
            "nodeOverrides": self.node_overrides,
            "retryStrategy": self.retry_strategy,
            "shareIdentifier": self.share_identifier,
            "schedulingPriorityOverride": self.scheduling_priority_override,
        }

        if self.submit_job_timeout:
            args["timeout"] = {"attemptDurationSeconds": self.submit_job_timeout}

        try:
            response = self.hook.client.submit_job(**trim_none_values(args))
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
        Monitor an AWS Batch job.

        This can raise an exception or an AirflowTaskTimeout if the task was
        created with ``execution_timeout``.
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

        if self.awslogs_enabled:
            if self.waiters:
                self.waiters.wait_for_job(self.job_id, get_batch_log_fetcher=self._get_batch_log_fetcher)
            else:
                self.hook.wait_for_job(self.job_id, get_batch_log_fetcher=self._get_batch_log_fetcher)
        else:
            if self.waiters:
                self.waiters.wait_for_job(self.job_id)
            else:
                self.hook.wait_for_job(self.job_id)

        awslogs = []
        try:
            awslogs = self.hook.get_job_all_awslogs_info(self.job_id)
        except AirflowException as ae:
            self.log.warning("Cannot determine where to find the AWS logs for this Batch job: %s", ae)

        if awslogs:
            self.log.info("AWS Batch job (%s) CloudWatch Events details found. Links to logs:", self.job_id)
            link_builder = CloudWatchEventsLink()
            for log in awslogs:
                self.log.info(link_builder.format_link(**log))
            if len(awslogs) > 1:
                # there can be several log streams on multi-node jobs
                self.log.warning(
                    "out of all those logs, we can only link to one in the UI. Using the first one."
                )

            CloudWatchEventsLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                **awslogs[0],
            )

        self.hook.check_job_success(self.job_id)
        self.log.info("AWS Batch job (%s) succeeded", self.job_id)

    def _get_batch_log_fetcher(self, job_id: str) -> AwsTaskLogFetcher | None:
        awslog_info = self.hook.get_job_awslogs_info(job_id)

        if not awslog_info:
            return None

        return AwsTaskLogFetcher(
            aws_conn_id=self.aws_conn_id,
            region_name=awslog_info["awslogs_region"],
            log_group=awslog_info["awslogs_group"],
            log_stream_name=awslog_info["awslogs_stream_name"],
            fetch_interval=self.awslogs_fetch_interval,
            logger=self.log,
        )


class BatchCreateComputeEnvironmentOperator(AwsBaseOperator[BatchClientHook]):
    """
    Create an AWS Batch compute environment.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BatchCreateComputeEnvironmentOperator`

    :param compute_environment_name: Name of the AWS batch compute
        environment (templated).
    :param environment_type: Type of the compute-environment.
    :param state: State of the compute-environment.
    :param compute_resources: Details about the resources managed by the
        compute-environment (templated). More details:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.create_compute_environment
    :param unmanaged_v_cpus: Maximum number of vCPU for an unmanaged compute
        environment. This parameter is only supported when the ``type``
        parameter is set to ``UNMANAGED``.
    :param service_role: IAM role that allows Batch to make calls to other AWS
        services on your behalf (templated).
    :param tags: Tags that you apply to the compute-environment to help you
        categorize and organize your resources.
    :param poll_interval: How long to wait in seconds between 2 polls at the environment status.
        Only useful when deferrable is True.
    :param max_retries: How many times to poll for the environment status.
        Only useful when deferrable is True.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param deferrable: If True, the operator will wait asynchronously for the environment to be created.
        This mode requires aiobotocore module to be installed. (default: False)
    """

    aws_hook_class = BatchClientHook

    template_fields: Sequence[str] = aws_template_fields(
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
        poll_interval: int = 30,
        max_retries: int | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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
        self.poll_interval = poll_interval
        self.max_retries = max_retries or 120
        self.deferrable = deferrable

    def execute(self, context: Context):
        """Create an AWS batch compute environment."""
        kwargs: dict[str, Any] = {
            "computeEnvironmentName": self.compute_environment_name,
            "type": self.environment_type,
            "state": self.state,
            "unmanagedvCpus": self.unmanaged_v_cpus,
            "computeResources": self.compute_resources,
            "serviceRole": self.service_role,
            "tags": self.tags,
        }
        response = self.hook.client.create_compute_environment(**trim_none_values(kwargs))
        arn = response["computeEnvironmentArn"]

        if self.deferrable:
            self.defer(
                trigger=BatchCreateComputeEnvironmentTrigger(
                    arn, self.poll_interval, self.max_retries, self.aws_conn_id, self.region_name
                ),
                method_name="execute_complete",
            )

        self.log.info("AWS Batch compute environment created successfully")
        return arn

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(
                f"Error while waiting for the compute environment to be ready: {validated_event}"
            )
        return validated_event["value"]
