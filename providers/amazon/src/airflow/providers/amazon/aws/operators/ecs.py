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

import re
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates, EcsHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.ecs import (
    ClusterActiveTrigger,
    ClusterInactiveTrigger,
    TaskDoneTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.identifiers import generate_uuid
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    import boto3

    from airflow.models import TaskInstance
    from airflow.utils.context import Context


class EcsBaseOperator(AwsBaseOperator[EcsHook]):
    """This is the base operator for all Elastic Container Service operators."""

    aws_hook_class = EcsHook

    @cached_property
    def client(self) -> boto3.client:
        """Create and return the EcsHook's client."""
        return self.hook.conn

    def execute(self, context: Context):
        """Must overwrite in child classes."""
        raise NotImplementedError("Please implement execute() in subclass")

    def _complete_exec_with_cluster_desc(self, context, event=None):
        """To be used as trigger callback for operators that return the cluster description."""
        if event["status"] != "success":
            raise AirflowException(f"Error while waiting for operation on cluster to complete: {event}")
        cluster_arn = event.get("arn")
        # We cannot get the cluster definition from the waiter on success, so we have to query it here.
        details = self.hook.conn.describe_clusters(clusters=[cluster_arn])["clusters"][0]
        return details


class EcsCreateClusterOperator(EcsBaseOperator):
    """
    Creates an AWS ECS cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsCreateClusterOperator`

    :param cluster_name: The name of your cluster. If you don't specify a name for your
        cluster, you create a cluster that's named default.
    :param create_cluster_kwargs: Extra arguments for Cluster Creation.
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    :param waiter_delay: The amount of time in seconds to wait between attempts,
        if not set then the default waiter value will be used.
    :param waiter_max_attempts: The maximum number of attempts to be made,
        if not set then the default waiter value will be used.
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    """

    template_fields: Sequence[str] = aws_template_fields(
        "cluster_name",
        "create_cluster_kwargs",
        "wait_for_completion",
        "deferrable",
    )

    def __init__(
        self,
        *,
        cluster_name: str,
        create_cluster_kwargs: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 15,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.create_cluster_kwargs = create_cluster_kwargs or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context):
        self.log.info(
            "Creating cluster %r using the following values: %s",
            self.cluster_name,
            self.create_cluster_kwargs,
        )
        result = self.client.create_cluster(clusterName=self.cluster_name, **self.create_cluster_kwargs)
        cluster_details = result["cluster"]
        cluster_state = cluster_details.get("status")

        if cluster_state == EcsClusterStates.ACTIVE:
            # In some circumstances the ECS Cluster is created immediately,
            # and there is no reason to wait for completion.
            self.log.info("Cluster %r in state: %r.", self.cluster_name, cluster_state)
        elif self.deferrable:
            self.defer(
                trigger=ClusterActiveTrigger(
                    cluster_arn=cluster_details["clusterArn"],
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                ),
                method_name="_complete_exec_with_cluster_desc",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay + 60),
            )
        elif self.wait_for_completion:
            waiter = self.hook.get_waiter("cluster_active")
            waiter.wait(
                clusters=[cluster_details["clusterArn"]],
                WaiterConfig=prune_dict(
                    {
                        "Delay": self.waiter_delay,
                        "MaxAttempts": self.waiter_max_attempts,
                    }
                ),
            )

        return cluster_details


class EcsDeleteClusterOperator(EcsBaseOperator):
    """
    Deletes an AWS ECS cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsDeleteClusterOperator`

    :param cluster_name: The short name or full Amazon Resource Name (ARN) of the cluster to delete.
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    :param waiter_delay: The amount of time in seconds to wait between attempts,
        if not set then the default waiter value will be used.
    :param waiter_max_attempts: The maximum number of attempts to be made,
        if not set then the default waiter value will be used.
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    """

    template_fields: Sequence[str] = ("cluster_name", "wait_for_completion", "deferrable")

    def __init__(
        self,
        *,
        cluster_name: str,
        wait_for_completion: bool = True,
        waiter_delay: int = 15,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context):
        self.log.info("Deleting cluster %r.", self.cluster_name)
        result = self.client.delete_cluster(cluster=self.cluster_name)
        cluster_details = result["cluster"]
        cluster_state = cluster_details.get("status")

        if cluster_state == EcsClusterStates.INACTIVE:
            # if the cluster doesn't have capacity providers that are associated with it,
            # the deletion is instantaneous, and we don't need to wait for it.
            self.log.info("Cluster %r in state: %r.", self.cluster_name, cluster_state)
        elif self.deferrable:
            self.defer(
                trigger=ClusterInactiveTrigger(
                    cluster_arn=cluster_details["clusterArn"],
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                ),
                method_name="_complete_exec_with_cluster_desc",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay + 60),
            )
        elif self.wait_for_completion:
            waiter = self.hook.get_waiter("cluster_inactive")
            waiter.wait(
                clusters=[cluster_details["clusterArn"]],
                WaiterConfig=prune_dict(
                    {
                        "Delay": self.waiter_delay,
                        "MaxAttempts": self.waiter_max_attempts,
                    }
                ),
            )

        return cluster_details


class EcsDeregisterTaskDefinitionOperator(EcsBaseOperator):
    """
    Deregister a task definition on AWS ECS.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsDeregisterTaskDefinitionOperator`

    :param task_definition: The family and revision (family:revision) or full Amazon Resource Name (ARN)
        of the task definition to deregister. If you use a family name, you must specify a revision.
    """

    template_fields: Sequence[str] = ("task_definition",)

    def __init__(
        self,
        *,
        task_definition: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.task_definition = task_definition

    def execute(self, context: Context):
        self.log.info("Deregistering task definition %s.", self.task_definition)
        result = self.client.deregister_task_definition(taskDefinition=self.task_definition)
        task_definition_details = result["taskDefinition"]
        task_definition_arn = task_definition_details["taskDefinitionArn"]
        self.log.info(
            "Task Definition %r in state: %r.", task_definition_arn, task_definition_details.get("status")
        )
        return task_definition_arn


class EcsRegisterTaskDefinitionOperator(EcsBaseOperator):
    """
    Register a task definition on AWS ECS.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsRegisterTaskDefinitionOperator`

    :param family: The family name of a task definition to create.
    :param container_definitions: A list of container definitions in JSON format that describe
        the different containers that make up your task.
    :param register_task_kwargs: Extra arguments for Register Task Definition.
    """

    template_fields: Sequence[str] = (
        "family",
        "container_definitions",
        "register_task_kwargs",
    )

    def __init__(
        self,
        *,
        family: str,
        container_definitions: list[dict],
        register_task_kwargs: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.family = family
        self.container_definitions = container_definitions
        self.register_task_kwargs = register_task_kwargs or {}

    def execute(self, context: Context):
        self.log.info(
            "Registering task definition %s using the following values: %s",
            self.family,
            self.register_task_kwargs,
        )
        self.log.info("Using container definition %s", self.container_definitions)
        response = self.client.register_task_definition(
            family=self.family,
            containerDefinitions=self.container_definitions,
            **self.register_task_kwargs,
        )
        task_definition_details = response["taskDefinition"]
        task_definition_arn = task_definition_details["taskDefinitionArn"]

        self.log.info(
            "Task Definition %r in state: %r.", task_definition_arn, task_definition_details.get("status")
        )
        context["ti"].xcom_push(key="task_definition_arn", value=task_definition_arn)
        return task_definition_arn


class EcsRunTaskOperator(EcsBaseOperator):
    """
    Execute a task on AWS ECS (Elastic Container Service).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsRunTaskOperator`

    :param task_definition: the task definition name on Elastic Container Service
    :param cluster: the cluster name on Elastic Container Service
    :param overrides: the same parameter that boto3 will receive (templated):
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html).
    :param region: region name to use in AWS Hook.
        Override the region in connection (if provided)
    :param launch_type: the launch type on which to run your task ('EC2', 'EXTERNAL', or 'FARGATE')
    :param capacity_provider_strategy: the capacity provider strategy to use for the task.
        When capacity_provider_strategy is specified, the launch_type parameter is omitted.
        If no capacity_provider_strategy or launch_type is specified,
        the default capacity provider strategy for the cluster is used.
    :param volume_configurations: the volume configurations to use when using capacity provider. The name of the volume must match
                                  the name from the task definition.
                                  You can configure the settings like size, volume type, IOPS, throughput and others mentioned in
                                  (https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_TaskManagedEBSVolumeConfiguration.html)
    :param group: the name of the task group associated with the task
    :param placement_constraints: an array of placement constraint objects to use for
        the task
    :param placement_strategy: an array of placement strategy objects to use for
        the task
    :param platform_version: the platform version on which your task is running
    :param network_configuration: the network configuration for the task
    :param tags: a dictionary of tags in the form of {'tagKey': 'tagValue'}.
    :param awslogs_group: the CloudWatch group where your ECS container logs are stored.
        Only required if you want logs to be shown in the Airflow UI after your job has
        finished.
    :param awslogs_region: the region in which your CloudWatch logs are stored.
        If None, this is the same as the `region` parameter. If that is also None,
        this is the default AWS region based on your connection settings.
    :param awslogs_stream_prefix: the stream prefix that is used for the CloudWatch logs.
        This should match the prefix specified in the log configuration of the task definition.
        Only required if you want logs to be shown in the Airflow UI after your job has
        finished.
    :param awslogs_fetch_interval: the interval that the ECS task log fetcher should wait
        in between each Cloudwatch logs fetches.
        If deferrable is set to True, that parameter is ignored and waiter_delay is used instead.
    :param container_name: The name of the container to fetch logs from. If not set, the first container is used.
    :param quota_retry: Config if and how to retry the launch of a new ECS task, to handle
        transient errors.
    :param reattach: If set to True, will check if the task previously launched by the task_instance
        is already running. If so, the operator will attach to it instead of starting a new task.
        This is to avoid relaunching a new task when the connection drops between Airflow and ECS while
        the task is running (when the Airflow worker is restarted for example).
    :param number_logs_exception: Number of lines from the last Cloudwatch logs to return in the
        AirflowException if an ECS task is stopped (to receive Airflow alerts with the logs of what
        failed in the code running in ECS).
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    :param waiter_delay: The amount of time in seconds to wait between attempts,
        if not set then the default waiter value will be used.
    :param waiter_max_attempts: The maximum number of attempts to be made,
        if not set then the default waiter value will be used.
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param do_xcom_push: If True, the operator will push the ECS task ARN to XCom with key 'ecs_task_arn'.
        Additionally, if logs are fetched, the last log message will be pushed to XCom with the key 'return_value'. (default: False)
    """

    ui_color = "#f0ede4"
    template_fields: Sequence[str] = (
        "task_definition",
        "cluster",
        "overrides",
        "launch_type",
        "capacity_provider_strategy",
        "volume_configurations",
        "group",
        "placement_constraints",
        "placement_strategy",
        "platform_version",
        "network_configuration",
        "tags",
        "awslogs_group",
        "awslogs_region",
        "awslogs_stream_prefix",
        "awslogs_fetch_interval",
        "container_name",
        "propagate_tags",
        "reattach",
        "number_logs_exception",
        "wait_for_completion",
        "deferrable",
    )
    template_fields_renderers = {
        "overrides": "json",
        "network_configuration": "json",
        "tags": "json",
    }

    def __init__(
        self,
        *,
        task_definition: str,
        cluster: str,
        overrides: dict,
        launch_type: str = "EC2",
        capacity_provider_strategy: list | None = None,
        volume_configurations: list | None = None,
        group: str | None = None,
        placement_constraints: list | None = None,
        placement_strategy: list | None = None,
        platform_version: str | None = None,
        network_configuration: dict | None = None,
        tags: dict | None = None,
        awslogs_group: str | None = None,
        awslogs_region: str | None = None,
        awslogs_stream_prefix: str | None = None,
        awslogs_fetch_interval: timedelta = timedelta(seconds=30),
        container_name: str | None = None,
        propagate_tags: str | None = None,
        quota_retry: dict | None = None,
        reattach: bool = False,
        number_logs_exception: int = 10,
        wait_for_completion: bool = True,
        waiter_delay: int = 6,
        waiter_max_attempts: int = 1000000,
        # Set the default waiter duration to 70 days (attempts*delay)
        # Airflow execution_timeout handles task timeout
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.task_definition = task_definition
        self.cluster = cluster
        self.overrides = overrides
        self.launch_type = launch_type
        self.capacity_provider_strategy = capacity_provider_strategy
        self.volume_configurations = volume_configurations
        self.group = group
        self.placement_constraints = placement_constraints
        self.placement_strategy = placement_strategy
        self.platform_version = platform_version
        self.network_configuration = network_configuration

        self.tags = tags
        self.awslogs_group = awslogs_group
        self.awslogs_stream_prefix = awslogs_stream_prefix
        self.awslogs_region = awslogs_region
        self.awslogs_fetch_interval = awslogs_fetch_interval
        self.propagate_tags = propagate_tags
        self.reattach = reattach
        self.number_logs_exception = number_logs_exception

        if self.awslogs_region is None:
            self.awslogs_region = self.region_name

        self.arn: str | None = None
        self.container_name: str | None = container_name
        self._started_by: str | None = None

        self.retry_args = quota_retry
        self.task_log_fetcher: AwsTaskLogFetcher | None = None
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

        if self._aws_logs_enabled() and not self.wait_for_completion:
            self.log.warning(
                "Trying to get logs without waiting for the task to complete is undefined behavior."
            )

    @staticmethod
    def _get_ecs_task_id(task_arn: str | None) -> str | None:
        if task_arn is None:
            return None
        return task_arn.split("/")[-1]

    def execute(self, context):
        self.log.info(
            "Running ECS Task - Task definition: %s - on cluster %s", self.task_definition, self.cluster
        )
        self.log.info("EcsOperator overrides: %s", self.overrides)

        if self.reattach:
            # Generate deterministic UUID which refers to unique TaskInstanceKey
            ti: TaskInstance = context["ti"]
            self._started_by = generate_uuid(*map(str, [ti.dag_id, ti.task_id, ti.run_id, ti.map_index]))
            self.log.info("Try to find run with startedBy=%r", self._started_by)
            self._try_reattach_task(started_by=self._started_by)

        if not self.arn:
            # start the task except if we reattached to an existing one just before.
            self._start_task()

        if self.do_xcom_push:
            context["ti"].xcom_push(key="ecs_task_arn", value=self.arn)

        if self.deferrable:
            self.defer(
                trigger=TaskDoneTrigger(
                    cluster=self.cluster,
                    task_arn=self.arn,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region=self.region_name,
                    log_group=self.awslogs_group,
                    log_stream=self._get_logs_stream_name(),
                ),
                method_name="execute_complete",
                # timeout is set to ensure that if a trigger dies, the timeout does not restart
                # 60 seconds is added to allow the trigger to exit gracefully (i.e. yield TriggerEvent)
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay + 60),
            )
            # self.defer raises a special exception, so execution stops here in this case.

        if not self.wait_for_completion:
            return

        if self._aws_logs_enabled():
            self.log.info("Starting ECS Task Log Fetcher")
            self.task_log_fetcher = self._get_task_log_fetcher()
            self.task_log_fetcher.start()

            try:
                self._wait_for_task_ended()
            finally:
                self.task_log_fetcher.stop()
            self.task_log_fetcher.join()
        else:
            self._wait_for_task_ended()

        self._after_execution()

        if self.do_xcom_push and self.task_log_fetcher:
            return self.task_log_fetcher.get_last_log_message()
        return None

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str | None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error in task execution: {validated_event}")
        self.arn = validated_event["task_arn"]  # restore arn to its updated value, needed for next steps
        self.cluster = validated_event["cluster"]
        self._after_execution()
        if self._aws_logs_enabled():
            # same behavior as non-deferrable mode, return last line of logs of the task.
            logs_client = AwsLogsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name).conn
            one_log = logs_client.get_log_events(
                logGroupName=self.awslogs_group,
                logStreamName=self._get_logs_stream_name(),
                startFromHead=False,
                limit=1,
            )
            if len(one_log["events"]) > 0:
                return one_log["events"][0]["message"]
        return None

    def _after_execution(self):
        self._check_success_task()

    def _start_task(self):
        run_opts = {
            "cluster": self.cluster,
            "taskDefinition": self.task_definition,
            "overrides": self.overrides,
            "startedBy": self._started_by or self.owner,
        }

        if self.capacity_provider_strategy:
            run_opts["capacityProviderStrategy"] = self.capacity_provider_strategy
        elif self.launch_type:
            run_opts["launchType"] = self.launch_type
        if self.volume_configurations is not None:
            run_opts["volumeConfigurations"] = self.volume_configurations
        if self.platform_version is not None:
            run_opts["platformVersion"] = self.platform_version
        if self.group is not None:
            run_opts["group"] = self.group
        if self.placement_constraints is not None:
            run_opts["placementConstraints"] = self.placement_constraints
        if self.placement_strategy is not None:
            run_opts["placementStrategy"] = self.placement_strategy
        if self.network_configuration is not None:
            run_opts["networkConfiguration"] = self.network_configuration
        if self.tags is not None:
            run_opts["tags"] = [{"key": k, "value": v} for (k, v) in self.tags.items()]
        if self.propagate_tags is not None:
            run_opts["propagateTags"] = self.propagate_tags

        response = self.client.run_task(**run_opts)

        failures = response["failures"]
        if len(failures) > 0:
            raise EcsOperatorError(failures, response)
        self.log.info("ECS Task started: %s", response)

        self.arn = response["tasks"][0]["taskArn"]
        self.log.info("ECS task ID is: %s", self._get_ecs_task_id(self.arn))

        if not self.container_name and (self.awslogs_group and self.awslogs_stream_prefix):
            backoff_schedule = [10, 30]
            for delay in backoff_schedule:
                sleep(delay)
                response = self.client.describe_tasks(cluster=self.cluster, tasks=[self.arn])
                containers = response["tasks"][0].get("containers", [])
                if containers:
                    self.container_name = containers[0]["name"]
                if self.container_name:
                    break

            if not self.container_name:
                self.log.info("Could not find container name, required for the log stream after 2 tries")

    def _try_reattach_task(self, started_by: str):
        if not started_by:
            raise AirflowException("`started_by` should not be empty or None")
        list_tasks_resp = self.client.list_tasks(
            cluster=self.cluster, desiredStatus="RUNNING", startedBy=started_by
        )
        running_tasks = list_tasks_resp["taskArns"]
        if running_tasks:
            if len(running_tasks) > 1:
                self.log.warning("Found more then one previously launched tasks: %s", running_tasks)
            self.arn = running_tasks[0]
            self.log.info("Reattaching previously launched task: %s", self.arn)
        else:
            self.log.info("No active previously launched task found to reattach")

    def _wait_for_task_ended(self) -> None:
        if not self.client or not self.arn:
            return

        waiter = self.client.get_waiter("tasks_stopped")
        waiter.wait(
            cluster=self.cluster,
            tasks=[self.arn],
            WaiterConfig={
                "Delay": self.waiter_delay,
                "MaxAttempts": self.waiter_max_attempts,
            },
        )

    def _aws_logs_enabled(self):
        return self.awslogs_group and self.awslogs_stream_prefix

    def _get_logs_stream_name(self) -> str:
        if not self.container_name and self.awslogs_stream_prefix and "/" not in self.awslogs_stream_prefix:
            self.log.warning(
                "Container name could not be inferred and awslogs_stream_prefix '%s' does not contain '/'. "
                "This may cause issues when extracting logs from Cloudwatch.",
                self.awslogs_stream_prefix,
            )
        elif (
            self.awslogs_stream_prefix
            and self.container_name
            and not self.awslogs_stream_prefix.endswith(f"/{self.container_name}")
        ):
            return f"{self.awslogs_stream_prefix}/{self.container_name}/{self._get_ecs_task_id(self.arn)}"
        return f"{self.awslogs_stream_prefix}/{self._get_ecs_task_id(self.arn)}"

    def _get_task_log_fetcher(self) -> AwsTaskLogFetcher:
        if not self.awslogs_group:
            raise ValueError("must specify awslogs_group to fetch task logs")

        return AwsTaskLogFetcher(
            aws_conn_id=self.aws_conn_id,
            region_name=self.awslogs_region,
            log_group=self.awslogs_group,
            log_stream_name=self._get_logs_stream_name(),
            fetch_interval=self.awslogs_fetch_interval,
            logger=self.log,
        )

    def _check_success_task(self) -> None:
        if not self.client or not self.arn:
            return

        response = self.client.describe_tasks(cluster=self.cluster, tasks=[self.arn])
        self.log.info("ECS Task stopped, check status: %s", response)

        if len(response.get("failures", [])) > 0:
            raise AirflowException(response)

        for task in response["tasks"]:
            if task.get("stopCode", "") == "TaskFailedToStart":
                raise EcsTaskFailToStart(f"The task failed to start due to: {task.get('stoppedReason', '')}")

            # This is a `stoppedReason` that indicates a task has not
            # successfully finished, but there is no other indication of failure
            # in the response.
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/stopped-task-errors.html
            if re.match(r"Host EC2 \(instance .+?\) (stopped|terminated)\.", task.get("stoppedReason", "")):
                raise AirflowException(
                    f"The task was stopped because the host instance terminated:"
                    f" {task.get('stoppedReason', '')}"
                )
            containers = task["containers"]
            for container in containers:
                if container.get("lastStatus") == "STOPPED" and container.get("exitCode", 1) != 0:
                    if self.task_log_fetcher:
                        last_logs = "\n".join(
                            self.task_log_fetcher.get_last_log_messages(self.number_logs_exception)
                        )
                        raise AirflowException(
                            f"This task is not in success state - last {self.number_logs_exception} "
                            f"logs from Cloudwatch:\n{last_logs}"
                        )
                    raise AirflowException(f"This task is not in success state {task}")
                if container.get("lastStatus") == "PENDING":
                    raise AirflowException(f"This task is still pending {task}")
                if "error" in container.get("reason", "").lower():
                    raise AirflowException(
                        f"This containers encounter an error during launching: "
                        f"{container.get('reason', '').lower()}"
                    )

    def on_kill(self) -> None:
        if not self.client or not self.arn:
            return

        if self.task_log_fetcher:
            self.task_log_fetcher.stop()

        response = self.client.stop_task(
            cluster=self.cluster, task=self.arn, reason="Task killed by the user"
        )
        self.log.info(response)
