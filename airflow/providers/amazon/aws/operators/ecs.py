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
import re
import sys
import time
import warnings
from collections import deque
from datetime import datetime, timedelta
from logging import Logger
from threading import Event, Thread
from typing import Dict, Generator, Optional, Sequence

from botocore.exceptions import ClientError, ConnectionClosedError
from botocore.waiter import Waiter

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, XCom
from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.typing_compat import Protocol, runtime_checkable
from airflow.utils.session import provide_session


def should_retry(exception: Exception):
    """Check if exception is related to ECS resource quota (CPU, MEM)."""
    if isinstance(exception, EcsOperatorError):
        return any(
            quota_reason in failure['reason']
            for quota_reason in ['RESOURCE:MEMORY', 'RESOURCE:CPU']
            for failure in exception.failures
        )
    return False


def should_retry_eni(exception: Exception):
    """Check if exception is related to ENI (Elastic Network Interfaces)."""
    if isinstance(exception, EcsTaskFailToStart):
        return any(
            eni_reason in exception.message
            for eni_reason in ['network interface provisioning', 'ResourceInitializationError']
        )
    return False


@runtime_checkable
class EcsProtocol(Protocol):
    """
    A structured Protocol for ``boto3.client('ecs')``. This is used for type hints on
    :py:meth:`.EcsOperator.client`.

    .. seealso::

        - https://mypy.readthedocs.io/en/latest/protocols.html
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html
    """

    def run_task(self, **kwargs) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task"""  # noqa: E501
        ...

    def get_waiter(self, x: str) -> Waiter:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.get_waiter"""  # noqa: E501
        ...

    def describe_tasks(self, cluster: str, tasks) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_tasks"""  # noqa: E501
        ...

    def stop_task(self, cluster, task, reason: str) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.stop_task"""  # noqa: E501
        ...

    def describe_task_definition(self, taskDefinition: str) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_task_definition"""  # noqa: E501
        ...

    def list_tasks(self, cluster: str, launchType: str, desiredStatus: str, family: str) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.list_tasks"""  # noqa: E501
        ...


class EcsTaskLogFetcher(Thread):
    """
    Fetches Cloudwatch log events with specific interval as a thread
    and sends the log events to the info channel of the provided logger.
    """

    def __init__(
        self,
        *,
        aws_conn_id: Optional[str] = 'aws_default',
        region_name: Optional[str] = None,
        log_group: str,
        log_stream_name: str,
        fetch_interval: timedelta,
        logger: Logger,
    ):
        super().__init__()
        self._event = Event()

        self.fetch_interval = fetch_interval

        self.logger = logger
        self.log_group = log_group
        self.log_stream_name = log_stream_name

        self.hook = AwsLogsHook(aws_conn_id=aws_conn_id, region_name=region_name)

    def run(self) -> None:
        logs_to_skip = 0
        while not self.is_stopped():
            time.sleep(self.fetch_interval.total_seconds())
            log_events = self._get_log_events(logs_to_skip)
            for log_event in log_events:
                self.logger.info(self._event_to_str(log_event))
                logs_to_skip += 1

    def _get_log_events(self, skip: int = 0) -> Generator:
        try:
            yield from self.hook.get_log_events(self.log_group, self.log_stream_name, skip=skip)
        except ClientError as error:
            if error.response['Error']['Code'] != 'ResourceNotFoundException':
                self.logger.warning('Error on retrieving Cloudwatch log events', error)

            yield from ()
        except ConnectionClosedError as error:
            self.logger.warning('ConnectionClosedError on retrieving Cloudwatch log events', error)
            yield from ()

    def _event_to_str(self, event: dict) -> str:
        event_dt = datetime.utcfromtimestamp(event['timestamp'] / 1000.0)
        formatted_event_dt = event_dt.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        message = event['message']
        return f'[{formatted_event_dt}] {message}'

    def get_last_log_messages(self, number_messages) -> list:
        return [log['message'] for log in deque(self._get_log_events(), maxlen=number_messages)]

    def get_last_log_message(self) -> Optional[str]:
        try:
            return self.get_last_log_messages(1)[0]
        except IndexError:
            return None

    def is_stopped(self) -> bool:
        return self._event.is_set()

    def stop(self):
        self._event.set()


class EcsOperator(BaseOperator):
    """
    Execute a task on AWS ECS (Elastic Container Service)

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EcsOperator`

    :param task_definition: the task definition name on Elastic Container Service
    :param cluster: the cluster name on Elastic Container Service
    :param overrides: the same parameter that boto3 will receive (templated):
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
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
    """

    ui_color = '#f0ede4'
    template_fields: Sequence[str] = (
        'cluster',
        'task_definition',
        'overrides',
        'network_configuration',
    )
    template_fields_renderers = {
        "overrides": "json",
        "network_configuration": "json",
        "tags": "json",
        "quota_retry": "json",
    }
    REATTACH_XCOM_KEY = "ecs_task_arn"
    REATTACH_XCOM_TASK_ID_TEMPLATE = "{task_id}_task_arn"

    def __init__(
        self,
        *,
        task_definition: str,
        cluster: str,
        overrides: dict,
        aws_conn_id: Optional[str] = None,
        region_name: Optional[str] = None,
        launch_type: str = 'EC2',
        capacity_provider_strategy: Optional[list] = None,
        group: Optional[str] = None,
        placement_constraints: Optional[list] = None,
        placement_strategy: Optional[list] = None,
        platform_version: Optional[str] = None,
        network_configuration: Optional[dict] = None,
        tags: Optional[dict] = None,
        awslogs_group: Optional[str] = None,
        awslogs_region: Optional[str] = None,
        awslogs_stream_prefix: Optional[str] = None,
        awslogs_fetch_interval: timedelta = timedelta(seconds=30),
        propagate_tags: Optional[str] = None,
        quota_retry: Optional[dict] = None,
        reattach: bool = False,
        number_logs_exception: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
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
            self.awslogs_region = region_name

        self.hook: Optional[AwsBaseHook] = None
        self.client: Optional[EcsProtocol] = None
        self.arn: Optional[str] = None
        self.retry_args = quota_retry
        self.task_log_fetcher: Optional[EcsTaskLogFetcher] = None

    @provide_session
    def execute(self, context, session=None):
        self.log.info(
            'Running ECS Task - Task definition: %s - on cluster %s', self.task_definition, self.cluster
        )
        self.log.info('EcsOperator overrides: %s', self.overrides)

        self.client = self.get_hook().get_conn()

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
                self._wait_for_task_ended()
            finally:
                self.task_log_fetcher.stop()

            self.task_log_fetcher.join()
        else:
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
            self._xcom_set(
                context,
                key=self.REATTACH_XCOM_KEY,
                value=self.arn,
                task_id=self.REATTACH_XCOM_TASK_ID_TEMPLATE.format(task_id=self.task_id),
            )

    def _xcom_set(self, context, key, value, task_id):
        XCom.set(
            key=key,
            value=value,
            task_id=task_id,
            dag_id=self.dag_id,
            run_id=context["run_id"],
        )

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

    def _get_task_log_fetcher(self) -> EcsTaskLogFetcher:
        if not self.awslogs_group:
            raise ValueError("must specify awslogs_group to fetch task logs")
        log_stream_name = f"{self.awslogs_stream_prefix}/{self.ecs_task_id}"

        return EcsTaskLogFetcher(
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

    def get_hook(self) -> AwsBaseHook:
        """Create and return an AwsHook."""
        if self.hook:
            return self.hook

        self.hook = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type='ecs', region_name=self.region_name)
        return self.hook

    def on_kill(self) -> None:
        if not self.client or not self.arn:
            return

        if self.task_log_fetcher:
            self.task_log_fetcher.stop()

        response = self.client.stop_task(
            cluster=self.cluster, task=self.arn, reason='Task killed by the user'
        )
        self.log.info(response)


class ECSOperator(EcsOperator):
    """
    This operator is deprecated.
    Please use :class:`airflow.providers.amazon.aws.operators.ecs.EcsOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This operator is deprecated. "
            "Please use `airflow.providers.amazon.aws.operators.ecs.EcsOperator`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class ECSTaskLogFetcher(EcsTaskLogFetcher):
    """
    This class is deprecated.
    Please use :class:`airflow.providers.amazon.aws.operators.ecs.EcsTaskLogFetcher`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This class is deprecated. "
            "Please use `airflow.providers.amazon.aws.operators.ecs.EcsTaskLogFetcher`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class ECSProtocol(EcsProtocol):
    """
    This class is deprecated.
    Please use :class:`airflow.providers.amazon.aws.operators.ecs.EcsProtocol`.
    """

    def __init__(self):
        warnings.warn(
            "This class is deprecated. "
            "Please use `airflow.providers.amazon.aws.operators.ecs.EcsProtocol`.",
            DeprecationWarning,
            stacklevel=2,
        )
