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
"""Utilities for checking Kubernetes resource quotas."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kubernetes.client.exceptions import ApiException

from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api
    from kubernetes.client.models import V1Pod

logger = logging.getLogger(__name__)


class PodResourceQuotaExceededException(AirflowException):
    """Raised when creating a pod would exceed the namespace resource quota."""


def parse_resource_quantity(quantity: str | None) -> float:
    """
    Parse Kubernetes resource quantity string to a numeric value.

    Supports suffixes like Ki, Mi, Gi, Ti, Pi, Ei for binary units
    and k, M, G, T, P, E for decimal units, as well as millicpu 'm' suffix.

    :param quantity: Kubernetes resource quantity string (e.g., "100m", "1Gi", "500Mi")
    :return: Numeric value in base units (cores for CPU, bytes for memory)
    """
    if not quantity:
        return 0.0

    quantity = str(quantity).strip()
    if not quantity:
        return 0.0

    # Binary suffixes (base 1024)
    binary_suffixes = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "Pi": 1024**5,
        "Ei": 1024**6,
    }

    # Decimal suffixes (base 1000)
    decimal_suffixes = {
        "k": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
        "P": 1000**5,
        "E": 1000**6,
    }

    # Check for millicpu (m suffix)
    if quantity.endswith("m"):
        try:
            return float(quantity[:-1]) / 1000
        except ValueError:
            return 0.0

    # Check for binary suffixes
    for suffix, multiplier in binary_suffixes.items():
        if quantity.endswith(suffix):
            try:
                return float(quantity[: -len(suffix)]) * multiplier
            except ValueError:
                return 0.0

    # Check for decimal suffixes
    for suffix, multiplier in decimal_suffixes.items():
        if quantity.endswith(suffix):
            try:
                return float(quantity[: -len(suffix)]) * multiplier
            except ValueError:
                return 0.0

    # No suffix, just a number
    try:
        return float(quantity)
    except ValueError:
        return 0.0


def get_pod_resource_requests(pod: V1Pod) -> dict[str, float]:
    """
    Extract total resource requests from a pod specification.

    :param pod: V1Pod object
    :return: Dictionary with 'cpu' and 'memory' keys containing total requests
    """
    total_cpu = 0.0
    total_memory = 0.0

    if not pod.spec or not pod.spec.containers:
        return {"cpu": total_cpu, "memory": total_memory}

    for container in pod.spec.containers:
        if container.resources and container.resources.requests:
            cpu_request = container.resources.requests.get("cpu")
            memory_request = container.resources.requests.get("memory")

            if cpu_request:
                total_cpu += parse_resource_quantity(cpu_request)
            if memory_request:
                total_memory += parse_resource_quantity(memory_request)

    # Also check init containers
    if pod.spec.init_containers:
        for container in pod.spec.init_containers:
            if container.resources and container.resources.requests:
                cpu_request = container.resources.requests.get("cpu")
                memory_request = container.resources.requests.get("memory")

                if cpu_request:
                    total_cpu += parse_resource_quantity(cpu_request)
                if memory_request:
                    total_memory += parse_resource_quantity(memory_request)

    return {"cpu": total_cpu, "memory": total_memory}


def get_namespace_quota_status(
    client: CoreV1Api, namespace: str
) -> tuple[dict[str, float], dict[str, float]] | None:
    """
    Get the current quota usage and limits for a namespace.

    :param client: Kubernetes CoreV1Api client
    :param namespace: Namespace name
    :return: Tuple of (used_resources, hard_limits) dictionaries or None if no quota
    """
    try:
        quota_list = client.list_namespaced_resource_quota(namespace=namespace)
    except ApiException as e:
        if e.status == 403:
            logger.warning(
                "Insufficient permissions to check resource quotas in namespace %s. "
                "Skipping quota validation.",
                namespace,
            )
            return None
        if e.status == 404:
            logger.debug("Namespace %s not found for quota check", namespace)
            return None
        logger.warning("Error checking resource quotas: %s", e)
        return None

    if not quota_list.items:
        # No resource quotas defined for this namespace
        return None

    # Aggregate all quotas in the namespace
    total_used = {"cpu": 0.0, "memory": 0.0}
    total_hard = {"cpu": 0.0, "memory": 0.0}

    for quota in quota_list.items:
        if quota.status and quota.status.used:
            cpu_used = quota.status.used.get("requests.cpu") or quota.status.used.get("cpu")
            memory_used = quota.status.used.get("requests.memory") or quota.status.used.get("memory")

            if cpu_used:
                total_used["cpu"] += parse_resource_quantity(cpu_used)
            if memory_used:
                total_used["memory"] += parse_resource_quantity(memory_used)

        if quota.spec and quota.spec.hard:
            cpu_hard = quota.spec.hard.get("requests.cpu") or quota.spec.hard.get("cpu")
            memory_hard = quota.spec.hard.get("requests.memory") or quota.spec.hard.get("memory")

            if cpu_hard:
                total_hard["cpu"] += parse_resource_quantity(cpu_hard)
            if memory_hard:
                total_hard["memory"] += parse_resource_quantity(memory_hard)

    # Only return quota info if limits are actually defined
    if total_hard["cpu"] > 0 or total_hard["memory"] > 0:
        return total_used, total_hard

    return None


def check_pod_quota_compliance(client: CoreV1Api, pod: V1Pod, namespace: str) -> None:
    """
    Check if creating a pod would exceed namespace resource quotas.

    :param client: Kubernetes CoreV1Api client
    :param pod: V1Pod object to check
    :param namespace: Namespace where pod will be created
    :raises PodResourceQuotaExceededException: If pod would exceed quota
    """
    quota_info = get_namespace_quota_status(client, namespace)

    if not quota_info:
        # No quotas defined or couldn't check, allow pod creation
        return

    used_resources, hard_limits = quota_info
    pod_requests = get_pod_resource_requests(pod)

    violations = []

    # Check CPU quota
    if hard_limits["cpu"] > 0:
        new_cpu_usage = used_resources["cpu"] + pod_requests["cpu"]
        if new_cpu_usage > hard_limits["cpu"]:
            violations.append(
                f"CPU: would use {new_cpu_usage:.3f} cores "
                f"(current: {used_resources['cpu']:.3f}, limit: {hard_limits['cpu']:.3f}, "
                f"pod requests: {pod_requests['cpu']:.3f})"
            )

    # Check memory quota
    if hard_limits["memory"] > 0:
        new_memory_usage = used_resources["memory"] + pod_requests["memory"]
        if new_memory_usage > hard_limits["memory"]:
            violations.append(
                f"Memory: would use {new_memory_usage / (1024**3):.3f} GiB "
                f"(current: {used_resources['memory'] / (1024**3):.3f} GiB, "
                f"limit: {hard_limits['memory'] / (1024**3):.3f} GiB, "
                f"pod requests: {pod_requests['memory'] / (1024**3):.3f} GiB)"
            )

    if violations:
        error_msg = (
            f"Cannot create pod '{pod.metadata.name}' in namespace '{namespace}'. "
            f"Would exceed resource quota:\n" + "\n".join(f"  - {v}" for v in violations)
        )
        raise PodResourceQuotaExceededException(error_msg)
