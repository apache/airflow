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

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from airflow.providers.amazon.aws.exceptions import EcsOperatorError
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.utils import _StringCompareEnum

if TYPE_CHECKING:
    from botocore.waiter import Waiter


def should_retry(exception: Exception):
    """Check if exception is related to ECS resource quota (CPU, MEM)."""
    if isinstance(exception, EcsOperatorError):
        return any(
            quota_reason in failure["reason"]
            for quota_reason in ["RESOURCE:MEMORY", "RESOURCE:CPU"]
            for failure in exception.failures
        )
    return False


class EcsClusterStates(_StringCompareEnum):
    """Contains the possible State values of an ECS Cluster."""

    ACTIVE = "ACTIVE"
    PROVISIONING = "PROVISIONING"
    DEPROVISIONING = "DEPROVISIONING"
    FAILED = "FAILED"
    INACTIVE = "INACTIVE"


class EcsTaskDefinitionStates(_StringCompareEnum):
    """Contains the possible State values of an ECS Task Definition."""

    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DELETE_IN_PROGRESS = "DELETE_IN_PROGRESS"


class EcsTaskStates(_StringCompareEnum):
    """Contains the possible State values of an ECS Task."""

    PROVISIONING = "PROVISIONING"
    PENDING = "PENDING"
    ACTIVATING = "ACTIVATING"
    RUNNING = "RUNNING"
    DEACTIVATING = "DEACTIVATING"
    STOPPING = "STOPPING"
    DEPROVISIONING = "DEPROVISIONING"
    STOPPED = "STOPPED"
    NONE = "NONE"


class EcsHook(AwsGenericHook):
    """
    Interact with Amazon Elastic Container Service (ECS).

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("ecs") <ECS.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - `Amazon Elastic Container Service \
        <https://docs.aws.amazon.com/AmazonECS/latest/APIReference/Welcome.html>`__
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "ecs"
        super().__init__(*args, **kwargs)

    def get_cluster_state(self, cluster_name: str) -> str:
        """
        Get ECS Cluster state.

        .. seealso::
            - :external+boto3:py:meth:`ECS.Client.describe_clusters`

        :param cluster_name: ECS Cluster name or full cluster Amazon Resource Name (ARN) entry.
        """
        return self.conn.describe_clusters(clusters=[cluster_name])["clusters"][0]["status"]

    def get_task_definition_state(self, task_definition: str) -> str:
        """
        Get ECS Task Definition state.

        .. seealso::
            - :external+boto3:py:meth:`ECS.Client.describe_task_definition`

        :param task_definition: The family for the latest ACTIVE revision,
            family and revision ( family:revision ) for a specific revision in the family,
            or full Amazon Resource Name (ARN) of the task definition to describe.
        """
        return self.conn.describe_task_definition(taskDefinition=task_definition)["taskDefinition"]["status"]

    def get_task_state(self, cluster, task) -> str:
        """
        Get ECS Task state.

        .. seealso::
            - :external+boto3:py:meth:`ECS.Client.describe_tasks`

        :param cluster: The short name or full Amazon Resource Name (ARN)
            of the cluster that hosts the task or tasks to describe.
        :param task: Task ID or full ARN entry.
        """
        return self.conn.describe_tasks(cluster=cluster, tasks=[task])["tasks"][0]["lastStatus"]


@runtime_checkable
class EcsProtocol(Protocol):
    """
    A structured Protocol for ``boto3.client('ecs')``.

    This is used for type hints on :py:meth:`.EcsOperator.client`.

    .. seealso::

        - https://mypy.readthedocs.io/en/latest/protocols.html
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html
    """

    def run_task(self, **kwargs) -> dict:
        """
        Run a task.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
        """
        ...

    def get_waiter(self, x: str) -> Waiter:
        """
        Get a waiter.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.get_waiter
        """
        ...

    def describe_tasks(self, cluster: str, tasks) -> dict:
        """
        Describe tasks.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_tasks
        """
        ...

    def stop_task(self, cluster, task, reason: str) -> dict:
        """
        Stop a task.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.stop_task
        """
        ...

    def describe_task_definition(self, taskDefinition: str) -> dict:
        """
        Describe a task definition.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_task_definition
        """
        ...

    def list_tasks(self, cluster: str, launchType: str, desiredStatus: str, family: str) -> dict:
        """
        List tasks.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.list_tasks
        """
        ...
