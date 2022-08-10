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
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from logging import Logger
from threading import Event, Thread
from typing import Dict, Generator, Optional

from botocore.exceptions import ClientError, ConnectionClosedError
from botocore.waiter import Waiter

from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.typing_compat import Protocol, runtime_checkable


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


class EcsClusterStates(Enum):
    """Contains the possible State values of an ECS Cluster."""

    ACTIVE = 'ACTIVE'
    PROVISIONING = 'PROVISIONING'
    DEPROVISIONING = 'DEPROVISIONING'
    FAILED = 'FAILED'
    INACTIVE = 'INACTIVE'


class EcsTaskDefinitionStates(Enum):
    """Contains the possible State values of an ECS Task Definition."""

    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


class EcsTaskStates(Enum):
    """Contains the possible State values of an ECS Task."""

    PROVISIONING = 'PROVISIONING'
    PENDING = 'PENDING'
    ACTIVATING = 'ACTIVATING'
    RUNNING = 'RUNNING'
    DEACTIVATING = 'DEACTIVATING'
    STOPPING = 'STOPPING'
    DEPROVISIONING = 'DEPROVISIONING'
    STOPPED = 'STOPPED'
    NONE = 'NONE'


class EcsHook(AwsGenericHook):
    """
    Interact with AWS Elastic Container Service, using the boto3 library
    Hook attribute `conn` has all methods that listed in documentation

    .. seealso::
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html
        - https://docs.aws.amazon.com/AmazonECS/latest/APIReference/Welcome.html

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
        are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs['client_type'] = 'ecs'
        super().__init__(*args, **kwargs)

    def get_cluster_state(self, cluster_name: str) -> str:
        return self.conn.describe_clusters(clusters=[cluster_name])['clusters'][0]['status']

    def get_task_definition_state(self, task_definition: str) -> str:
        return self.conn.describe_task_definition(taskDefinition=task_definition)['taskDefinition']['status']

    def get_task_state(self, cluster, task) -> str:
        return self.conn.describe_tasks(cluster=cluster, tasks=[task])['tasks'][0]['lastStatus']


class EcsTaskLogFetcher(Thread):
    """
    Fetches Cloudwatch log events with specific interval as a thread
    and sends the log events to the info channel of the provided logger.
    """

    def __init__(
        self,
        *,
        log_group: str,
        log_stream_name: str,
        fetch_interval: timedelta,
        logger: Logger,
        aws_conn_id: Optional[str] = 'aws_default',
        region_name: Optional[str] = None,
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
