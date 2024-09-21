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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.ecs import (
    EcsClusterStates,
    EcsHook,
    EcsTaskDefinitionStates,
    EcsTaskStates,
)
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    import boto3

    from airflow.utils.context import Context


def _check_failed(current_state, target_state, failure_states) -> None:
    if (current_state != target_state) and (current_state in failure_states):
        raise AirflowException(
            f"Terminal state reached. Current state: {current_state}, Expected state: {target_state}"
        )


class EcsBaseSensor(AwsBaseSensor[EcsHook]):
    """Contains general sensor behavior for Elastic Container Service."""

    aws_hook_class = EcsHook

    @cached_property
    def client(self) -> boto3.client:
        """Create and return an EcsHook client."""
        return self.hook.conn


class EcsClusterStateSensor(EcsBaseSensor):
    """
    Poll the cluster state until it reaches a terminal state; raises AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/sensor:EcsClusterStateSensor`

    :param cluster_name: The name of your cluster.
    :param target_state: Success state to watch for. (Default: "ACTIVE")
    :param failure_states: Fail if any of these states are reached before the
         Success State. (Default: "FAILED" or "INACTIVE")
    """

    template_fields: Sequence[str] = aws_template_fields("cluster_name", "target_state", "failure_states")

    def __init__(
        self,
        *,
        cluster_name: str,
        target_state: EcsClusterStates | None = EcsClusterStates.ACTIVE,
        failure_states: set[EcsClusterStates] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.target_state = target_state
        self.failure_states = failure_states or {EcsClusterStates.FAILED, EcsClusterStates.INACTIVE}

    def poke(self, context: Context):
        cluster_state = EcsClusterStates(self.hook.get_cluster_state(cluster_name=self.cluster_name))

        self.log.info("Cluster state: %s, waiting for: %s", cluster_state, self.target_state)
        _check_failed(cluster_state, self.target_state, self.failure_states)

        return cluster_state == self.target_state


class EcsTaskDefinitionStateSensor(EcsBaseSensor):
    """
    Poll task definition until it reaches a terminal state; raise AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/sensor:EcsTaskDefinitionStateSensor`

    :param task_definition: The family for the latest ACTIVE revision, family and
         revision (family:revision ) for a specific revision in the family, or full
         Amazon Resource Name (ARN) of the task definition.
    :param target_state: Success state to watch for. (Default: "ACTIVE")
    """

    template_fields: Sequence[str] = aws_template_fields("task_definition", "target_state", "failure_states")

    def __init__(
        self,
        *,
        task_definition: str,
        target_state: EcsTaskDefinitionStates | None = EcsTaskDefinitionStates.ACTIVE,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.task_definition = task_definition
        self.target_state = target_state
        # There are only two possible states, so set failure_state to whatever is not the target_state
        self.failure_states = {
            (
                EcsTaskDefinitionStates.INACTIVE
                if target_state == EcsTaskDefinitionStates.ACTIVE
                else EcsTaskDefinitionStates.ACTIVE
            )
        }

    def poke(self, context: Context):
        task_definition_state = EcsTaskDefinitionStates(
            self.hook.get_task_definition_state(task_definition=self.task_definition)
        )

        self.log.info("Task Definition state: %s, waiting for: %s", task_definition_state, self.target_state)
        _check_failed(task_definition_state, self.target_state, [self.failure_states])
        return task_definition_state == self.target_state


class EcsTaskStateSensor(EcsBaseSensor):
    """
    Poll the task state until it reaches a terminal state; raises AirflowException with the failure reason.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/sensor:EcsTaskStateSensor`

    :param cluster: The short name or full Amazon Resource Name (ARN) of the cluster that hosts the task.
    :param task: The task ID or full ARN of the task to poll.
    :param target_state: Success state to watch for. (Default: "ACTIVE")
    :param failure_states: Fail if any of these states are reached before
         the Success State. (Default: "STOPPED")
    """

    template_fields: Sequence[str] = aws_template_fields("cluster", "task", "target_state", "failure_states")

    def __init__(
        self,
        *,
        cluster: str,
        task: str,
        target_state: EcsTaskStates | None = EcsTaskStates.RUNNING,
        failure_states: set[EcsTaskStates] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster = cluster
        self.task = task
        self.target_state = target_state
        self.failure_states = failure_states or {EcsTaskStates.STOPPED}

    def poke(self, context: Context):
        task_state = EcsTaskStates(self.hook.get_task_state(cluster=self.cluster, task=self.task))

        self.log.info("Task state: %s, waiting for: %s", task_state, self.target_state)
        _check_failed(task_state, self.target_state, self.failure_states)
        return task_state == self.target_state
