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


def get_container_status(pod: V1Pod, container_name: str) -> V1ContainerStatus | None:
    """Retrieve container status."""
    if pod and pod.status:
        container_statuses = []
        if pod.status.container_statuses:
            container_statuses.extend(pod.status.container_statuses)
        if pod.status.init_container_statuses:
            container_statuses.extend(pod.status.init_container_statuses)

    else:
        container_statuses = None

    if container_statuses:
        # In general the variable container_statuses can store multiple items matching different containers.
        # The following generator expression yields all items that have name equal to the container_name.
        # The function next() here calls the generator to get only the first value. If there's nothing found
        # then None is returned.
        return next((x for x in container_statuses if x.name == container_name), None)
    return None


def container_is_running(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is running.

    If that container is present and running, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False
    return container_status.state.running is not None


def container_is_completed(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is completed.

    If that container is present and completed, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False
    return container_status.state.terminated is not None


def container_is_succeeded(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is completed and succeeded.

    If that container is present and completed and succeeded, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status or container_status.state.terminated is None:
        return False
    return container_status.state.terminated.exit_code == 0


def container_is_wait(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is waiting.

    If that container is present and waiting, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False

    return container_status.state.waiting is not None


def container_is_terminated(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is terminated.

    If that container is present and terminated, returns True.  Returns False otherwise.
    """
    container_statuses = pod.status.container_statuses if pod and pod.status else None
    if not container_statuses:
        return False
    container_status = next((x for x in container_statuses if x.name == container_name), None)
    if not container_status:
        return False
    return container_status.state.terminated is not None


def get_container_termination_message(pod: V1Pod, container_name: str):
    with suppress(AttributeError, TypeError):
        container_statuses = pod.status.container_statuses
        container_status = next((x for x in container_statuses if x.name == container_name), None)
        return container_status.state.terminated.message if container_status else None
