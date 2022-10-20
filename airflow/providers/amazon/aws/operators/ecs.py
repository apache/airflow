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
import sys
import warnings
from datetime import timedelta
from typing import TYPE_CHECKING, Sequence

import boto3

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, XCom
from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart

# TODO: Remove the following import when EcsProtocol and EcsTaskLogFetcher deprecations are removed.
from airflow.providers.amazon.aws.hooks import ecs
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.ecs import (
    EcsClusterStates,
    EcsHook,
    EcsTaskDefinitionStates,
    should_retry_eni,
)
from airflow.providers.amazon.aws.sensors.ecs import EcsClusterStateSensor, EcsTaskDefinitionStateSensor
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from airflow.utils.context import Context

DEFAULT_CONN_ID = 'aws_default'


class EcsBaseOperator(BaseOperator):
    """This is the base operator for all Elastic Container Service operators."""

    def __init__(self, *, aws_conn_id: str | None = DEFAULT_CONN_ID, region: str | None = None, **kwargs):
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EcsHook:
        """Create and return an EcsHook."""
        return EcsHook(aws_conn_id=self.aws_conn_id, region_name=self.region)

    @cached_property
    def client(self) -> boto3.client:
        """Create and return the EcsHook's client."""
        return self.hook.conn

    def execute(self, context: Context):
        """Must overwrite in child classes."""
        raise NotImplementedError('Please implement execute() in subclass')


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
    """

    template_fields: Sequence[str] = ('cluster_name', 'create_cluster_kwargs', 'wait_for_completion')

    def __init__(
        self,
        *,
        cluster_name: str,
        create_cluster_kwargs: dict | None = None,
        wait_for_completion: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.create_cluster_kwargs = create_cluster_kwargs or {}
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        self.log.info(
            'Creating cluster %s using the following values: %s',
            self.cluster_name,
            self.create_cluster_kwargs,
        )
        result = self.client.create_cluster(clusterName=self.cluster_name, **self.create_cluster_kwargs)

        if self.wait_for_completion:
            while not EcsClusterStateSensor(
                task_id='await_cluster',
                cluster_name=self.cluster_name,
            ).poke(context):
                # The sensor has a built-in delay and will try again until
                # the cluster is ready or has reached a failed state.
                pass

        return result['cluster']


class EcsDeleteClusterOperator(EcsBaseOperator):
    """
    Deletes an AWS ECS cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsDeleteClusterOperator`

    :param cluster_name: The short name or full Amazon Resource Name (ARN) of the cluster to delete.
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    """

    template_fields: Sequence[str] = ('cluster_name', 'wait_for_completion')

    def __init__(
        self,
        *,
        cluster_name: str,
        wait_for_completion: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        self.log.info('Deleting cluster %s.', self.cluster_name)
        result = self.client.delete_cluster(cluster=self.cluster_name)

        if self.wait_for_completion:
            while not EcsClusterStateSensor(
                task_id='await_cluster_delete',
                cluster_name=self.cluster_name,
                target_state=EcsClusterStates.INACTIVE,
                failure_states={EcsClusterStates.FAILED},
            ).poke(context):
                # The sensor has a built-in delay and will try again until
                # the cluster is deleted or reaches a failed state.
                pass

        return result['cluster']


class EcsDeregisterTaskDefinitionOperator(EcsBaseOperator):
    """
    Deregister a task definition on AWS ECS.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsDeregisterTaskDefinitionOperator`

    :param task_definition: The family and revision (family:revision) or full Amazon Resource Name (ARN)
        of the task definition to deregister. If you use a family name, you must specify a revision.
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    """

    template_fields: Sequence[str] = ('task_definition', 'wait_for_completion')

    def __init__(self, *, task_definition: str, wait_for_completion: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.task_definition = task_definition
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        self.log.info('Deregistering task definition %s.', self.task_definition)
        result = self.client.deregister_task_definition(taskDefinition=self.task_definition)

        if self.wait_for_completion:
            while not EcsTaskDefinitionStateSensor(
                task_id='await_deregister_task_definition',
                task_definition=self.task_definition,
                target_state=EcsTaskDefinitionStates.INACTIVE,
            ).poke(context):
                # The sensor has a built-in delay and will try again until the
                # task definition is deregistered or reaches a failed state.
                pass

        return result['taskDefinition']['taskDefinitionArn']


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
    :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
    """

    template_fields: Sequence[str] = (
        'family',
        'container_definitions',
        'register_task_kwargs',
        'wait_for_completion',
    )

    def __init__(
        self,
        *,
        family: str,
        container_definitions: list[dict],
        register_task_kwargs: dict | None = None,
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.family = family
        self.container_definitions = container_definitions
        self.register_task_kwargs = register_task_kwargs or {}
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        self.log.info(
            'Registering task definition %s using the following values: %s',
            self.family,
            self.register_task_kwargs,
        )
        self.log.info('Using container definition %s', self.container_definitions)
        response = self.client.register_task_definition(
            family=self.family,
            containerDefinitions=self.container_definitions,
            **self.register_task_kwargs,
        )
        task_arn = response['taskDefinition']['taskDefinitionArn']

        if self.wait_for_completion:
            while not EcsTaskDefinitionStateSensor(
                task_id='await_register_task_definition', task_definition=task_arn
            ).poke(context):
                # The sensor has a built-in delay and will try again until
                # the task definition is registered or reaches a failed state.
                pass

        context['ti'].xcom_push(key='task_definition_arn', value=task_arn)
        return task_arn


class EcsRunTaskOperator(EcsBaseOperator):
    """
    Execute a task on AWS ECS (Elastic Container Service)

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
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :param launch_type: the launch type on which to run your task ('EC2', 'EXTERNAL', or 'FARGATE')
    :param capacity_provider_strategy: the capacity provider strategy to use for the task.
        When capacity_provider_strategy is specified, the launch_type parameter is omitted.
        If no capacity_provider_strategy or launch_type is specified,
        the default capacity provider strategy for the cluster is used.
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
        If None, this is the same as the `region_name` parameter. If that is also None,
        this is the default AWS region based on your connection settings.
    :param awslogs_stream_prefix: the stream prefix that is used for the CloudWatch logs.
        This is usually based on some custom name combined with the name of the container.
        Only required if you want logs to be shown in the Airflow UI after your job has
        finished.
    :param awslogs_fetch_interval: the interval that the ECS task log fetcher should wait
        in between each Cloudwatch logs fetches.
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
    """

    ui_color = '#f0ede4'
    template_fields: Sequence[str] = (
        'task_definition',
        'cluster',
        'overrides',
        'launch_type',
        'capacity_provider_strategy',
        'group',
        'placement_constraints',
        'placement_strategy',
        'platform_version',
        'network_configuration',
        'tags',
        'awslogs_group',
        'awslogs_region',
        'awslogs_stream_prefix',
        'awslogs_fetch_interval',
        'propagate_tags',
        'reattach',
        'number_logs_exception',
        'wait_for_completion',
    )
    template_fields_renderers = {
        "overrides": "json",
        "network_configuration": "json",
        "tags": "json",
    }
    REATTACH_XCOM_KEY = "ecs_task_arn"
    REATTACH_XCOM_TASK_ID_TEMPLATE = "{task_id}_task_arn"

    def __init__(
        self,
        *,
        task_definition: str,
        cluster: str,
        overrides: dict,
        launch_type: str = 'EC2',
        capacity_provider_strategy: list | None = None,
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
        propagate_tags: str | None = None,
        quota_retry: dict | None = None,
        reattach: bool = False,
        number_logs_exception: int = 10,
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.task_definition = task_definition
        self.cluster = cluster
        self.overrides = overrides
        self.launch_type = launch_type
        self.capacity_provider_strategy = capacity_provider_strategy
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
            self.awslogs_region = self.region

        self.arn: str | None = None
        self.retry_args = quota_retry
        self.task_log_fetcher: EcsTaskLogFetcher | None = None
        self.wait_for_completion = wait_for_completion

    @provide_session
    def execute(self, context, session=None):
        self.log.info(
            'Running ECS Task - Task definition: %s - on cluster %s', self.task_definition, self.cluster
        )
        self.log.info('EcsOperator overrides: %s', self.overrides)

        if self.reattach:
            self._try_reattach_task(context)

        self._start_wait_check_task(context)

        self.log.info('ECS Task has been successfully executed')

        if self.reattach:
            # Clear the XCom value storing the ECS task ARN if the task has completed
            # as we can't reattach it anymore
            self._xcom_del(session, self.REATTACH_XCOM_TASK_ID_TEMPLATE.format(task_id=self.task_id))

        if self.do_xcom_push and self.task_log_fetcher:
            return self.task_log_fetcher.get_last_log_message()

        return None

    @AwsBaseHook.retry(should_retry_eni)
    def _start_wait_check_task(self, context):

        if not self.arn:
            self._start_task(context)

        if self._aws_logs_enabled():
            self.log.info('Starting ECS Task Log Fetcher')
            self.task_log_fetcher = self._get_task_log_fetcher()
            self.task_log_fetcher.start()

            try:
                if self.wait_for_completion:
                    self._wait_for_task_ended()
            finally:
                self.task_log_fetcher.stop()

            self.task_log_fetcher.join()
        else:
            if self.wait_for_completion:
                self._wait_for_task_ended()

        self._check_success_task()

    def _xcom_del(self, session, task_id):
        session.query(XCom).filter(XCom.dag_id == self.dag_id, XCom.task_id == task_id).delete()

    def _start_task(self, context):
        run_opts = {
            'cluster': self.cluster,
            'taskDefinition': self.task_definition,
            'overrides': self.overrides,
            'startedBy': self.owner,
        }

        if self.capacity_provider_strategy:
            run_opts['capacityProviderStrategy'] = self.capacity_provider_strategy
        elif self.launch_type:
            run_opts['launchType'] = self.launch_type
        if self.platform_version is not None:
            run_opts['platformVersion'] = self.platform_version
        if self.group is not None:
            run_opts['group'] = self.group
        if self.placement_constraints is not None:
            run_opts['placementConstraints'] = self.placement_constraints
        if self.placement_strategy is not None:
            run_opts['placementStrategy'] = self.placement_strategy
        if self.network_configuration is not None:
            run_opts['networkConfiguration'] = self.network_configuration
        if self.tags is not None:
            run_opts['tags'] = [{'key': k, 'value': v} for (k, v) in self.tags.items()]
        if self.propagate_tags is not None:
            run_opts['propagateTags'] = self.propagate_tags

        response = self.client.run_task(**run_opts)

        failures = response['failures']
        if len(failures) > 0:
            raise EcsOperatorError(failures, response)
        self.log.info('ECS Task started: %s', response)

        self.arn = response['tasks'][0]['taskArn']
        self.ecs_task_id = self.arn.split("/")[-1]
        self.log.info("ECS task ID is: %s", self.ecs_task_id)

        if self.reattach:
            # Save the task ARN in XCom to be able to reattach it if needed
            self.xcom_push(context, key=self.REATTACH_XCOM_KEY, value=self.arn)

    def _try_reattach_task(self, context):
        task_def_resp = self.client.describe_task_definition(taskDefinition=self.task_definition)
        ecs_task_family = task_def_resp['taskDefinition']['family']

        list_tasks_resp = self.client.list_tasks(
            cluster=self.cluster, desiredStatus='RUNNING', family=ecs_task_family
        )
        running_tasks = list_tasks_resp['taskArns']

        # Check if the ECS task previously launched is already running
        previous_task_arn = self.xcom_pull(
            context,
            task_ids=self.REATTACH_XCOM_TASK_ID_TEMPLATE.format(task_id=self.task_id),
            key=self.REATTACH_XCOM_KEY,
        )
        if previous_task_arn in running_tasks:
            self.arn = previous_task_arn
            self.ecs_task_id = self.arn.split("/")[-1]
            self.log.info("Reattaching previously launched task: %s", self.arn)
        else:
            self.log.info("No active previously launched task found to reattach")

    def _wait_for_task_ended(self) -> None:
        if not self.client or not self.arn:
            return

        waiter = self.client.get_waiter('tasks_stopped')
        waiter.config.max_attempts = sys.maxsize  # timeout is managed by airflow
        waiter.wait(cluster=self.cluster, tasks=[self.arn])

        return

    def _aws_logs_enabled(self):
        return self.awslogs_group and self.awslogs_stream_prefix

    # TODO: When the deprecation wrapper below is removed, please fix the following return type hint.
    def _get_task_log_fetcher(self) -> ecs.EcsTaskLogFetcher:
        if not self.awslogs_group:
            raise ValueError("must specify awslogs_group to fetch task logs")
        log_stream_name = f"{self.awslogs_stream_prefix}/{self.ecs_task_id}"

        return ecs.EcsTaskLogFetcher(
            aws_conn_id=self.aws_conn_id,
            region_name=self.awslogs_region,
            log_group=self.awslogs_group,
            log_stream_name=log_stream_name,
            fetch_interval=self.awslogs_fetch_interval,
            logger=self.log,
        )

    def _check_success_task(self) -> None:
        if not self.client or not self.arn:
            return

        response = self.client.describe_tasks(cluster=self.cluster, tasks=[self.arn])
        self.log.info('ECS Task stopped, check status: %s', response)

        if len(response.get('failures', [])) > 0:
            raise AirflowException(response)

        for task in response['tasks']:

            if task.get('stopCode', '') == 'TaskFailedToStart':
                # Reset task arn here otherwise the retry run will not start
                # a new task but keep polling the old dead one
                # I'm not resetting it for other exceptions here because
                # EcsTaskFailToStart is the only exception that's being retried at the moment
                self.arn = None
                raise EcsTaskFailToStart(f"The task failed to start due to: {task.get('stoppedReason', '')}")

            # This is a `stoppedReason` that indicates a task has not
            # successfully finished, but there is no other indication of failure
            # in the response.
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/stopped-task-errors.html
            if re.match(r'Host EC2 \(instance .+?\) (stopped|terminated)\.', task.get('stoppedReason', '')):
                raise AirflowException(
                    f"The task was stopped because the host instance terminated:"
                    f" {task.get('stoppedReason', '')}"
                )
            containers = task['containers']
            for container in containers:
                if container.get('lastStatus') == 'STOPPED' and container.get('exitCode', 1) != 0:
                    if self.task_log_fetcher:
                        last_logs = "\n".join(
                            self.task_log_fetcher.get_last_log_messages(self.number_logs_exception)
                        )
                        raise AirflowException(
                            f"This task is not in success state - last {self.number_logs_exception} "
                            f"logs from Cloudwatch:\n{last_logs}"
                        )
                    else:
                        raise AirflowException(f'This task is not in success state {task}')
                elif container.get('lastStatus') == 'PENDING':
                    raise AirflowException(f'This task is still pending {task}')
                elif 'error' in container.get('reason', '').lower():
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
            cluster=self.cluster, task=self.arn, reason='Task killed by the user'
        )
        self.log.info(response)


class EcsOperator(EcsRunTaskOperator):
    """
    This operator is deprecated.
    Please use :class:`airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This operator is deprecated. "
            "Please use `airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class EcsTaskLogFetcher(ecs.EcsTaskLogFetcher):
    """
    This class is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.ecs.EcsTaskLogFetcher`.
    """

    # TODO: Note to deprecator, Be sure to fix the use of `ecs.EcsTaskLogFetcher`
    #       in the Operators above when you remove this wrapper class.
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This class is deprecated. "
            "Please use `airflow.providers.amazon.aws.hooks.ecs.EcsTaskLogFetcher`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class EcsProtocol(ecs.EcsProtocol):
    """
    This class is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.ecs.EcsProtocol`.
    """

    # TODO: Note to deprecator, Be sure to fix the use of `ecs.EcsProtocol`
    #       in the Operators above when you remove this wrapper class.
    def __init__(self):
        warnings.warn(
            "This class is deprecated.  Please use `airflow.providers.amazon.aws.hooks.ecs.EcsProtocol`.",
            DeprecationWarning,
            stacklevel=2,
        )
