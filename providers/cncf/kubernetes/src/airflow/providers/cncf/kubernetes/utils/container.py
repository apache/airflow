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
"""Helper functions for inspecting and interacting with containers in a Kubernetes Pod."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kubernetes.client.models.v1_container_status import V1ContainerStatus
    from kubernetes.client.models.v1_pod import V1Pod
    from kubernetes_asyncio.client.models import (
        V1ContainerStatus as AsyncV1ContainerStatus,
        V1Pod as AsyncV1Pod,
    )


def get_container_status(
    pod: V1Pod | AsyncV1Pod, container_name: str
) -> V1ContainerStatus | AsyncV1ContainerStatus | None:
    """Retrieve container status."""
    container_statuses: list[V1ContainerStatus | AsyncV1ContainerStatus] = []
    if pod and pod.status:
        if pod.status.container_statuses:
            container_statuses.extend(pod.status.container_statuses)
        if pod.status.init_container_statuses:
            container_statuses.extend(pod.status.init_container_statuses)

    if container_statuses:
        # In general the variable container_statuses can store multiple items matching different containers.
        # The following generator expression yields all items that have name equal to the container_name.
        # The function next() here calls the generator to get only the first value. If there's nothing found
        # then None is returned.
        return next((x for x in container_statuses if x.name == container_name), None)
    return None


def container_is_running(pod: V1Pod | AsyncV1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is running.

    If that container is present and running, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status or not container_status.state:
        return False
    return container_status.state.running is not None


def container_is_completed(pod: V1Pod | AsyncV1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is completed.

    If that container is present and completed, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status or not container_status.state:
        return False
    return container_status.state.terminated is not None


def container_is_succeeded(pod: V1Pod | AsyncV1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is completed and succeeded.

    If that container is present and completed and succeeded, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status or not container_status.state or container_status.state.terminated is None:
        return False
    return container_status.state.terminated.exit_code == 0


def container_is_wait(pod: V1Pod | AsyncV1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is waiting.

    If that container is present and waiting, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status or not container_status.state:
        return False

    return container_status.state.waiting is not None


def container_is_terminated(pod: V1Pod | AsyncV1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is terminated.

    If that container is present and terminated, returns True.  Returns False otherwise.
    """
    container_statuses = pod.status.container_statuses if pod and pod.status else None
    if not container_statuses:
        return False
    container_status = next((x for x in container_statuses if x.name == container_name), None)
    if not container_status or not container_status.state:
        return False
    return container_status.state.terminated is not None


def get_container_termination_message(pod: V1Pod | AsyncV1Pod, container_name: str) -> str | None:
    with suppress(AttributeError, TypeError):
        if not pod or not pod.status:
            return None
        container_status = next(
            (status for status in pod.status.container_statuses or [] if status.name == container_name), None
        )
        if container_status and container_status.state and container_status.state.terminated:
            return container_status.state.terminated.message
    return None
