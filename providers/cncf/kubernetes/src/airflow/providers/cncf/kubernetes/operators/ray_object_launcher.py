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
"""Launcher for Ray Job Custom Resource Definition."""

from __future__ import annotations

import copy
import time
from datetime import datetime as dt
from typing import TYPE_CHECKING, Any

import tenacity
from kubernetes.client.rest import ApiException

from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api, CustomObjectsApi


def should_retry_start_ray_job(exception: BaseException) -> bool:
    """Check if an Exception indicates a transient error and warrants retrying."""
    if isinstance(exception, ApiException):
        return str(exception.status) == "409"
    return False


class RayJobStatus:
    """Ray Job status constants."""

    NEW = ""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class RayJobDeploymentStatus:
    """Ray Job deployment status constants."""

    NEW = ""
    INITIALIZING = "Initializing"
    RUNNING = "Running"
    COMPLETE = "Complete"
    FAILED = "Failed"
    VALIDATION_FAILED = "ValidationFailed"
    SUSPENDING = "Suspending"
    SUSPENDED = "Suspended"
    RETRYING = "Retrying"
    WAITING = "Waiting"


class RayObjectLauncher(LoggingMixin):
    """
    Launches and manages Ray Job CRDs on Kubernetes.

    :param name: Name of the RayJob
    :param namespace: Kubernetes namespace
    :param kube_client: Kubernetes CoreV1Api client
    :param custom_obj_api: Kubernetes CustomObjectsApi client
    :param template_body: RayJob specification dict
    """

    def __init__(
        self,
        name: str | None,
        namespace: str | None,
        kube_client: CoreV1Api,
        custom_obj_api: CustomObjectsApi,
        template_body: dict | None = None,
    ):
        super().__init__()
        self.name = name
        self.namespace = namespace
        self.template_body = template_body
        self.body: dict = self._get_body()
        self.kind = self.body.get("kind", "RayJob")
        self.plural = f"{self.kind.lower()}s"

        api_version = self.body.get("apiVersion", "ray.io/v1")
        if "/" in api_version:
            parts = api_version.split("/", 1)
            self.api_group, self.api_version = parts[0], parts[1]
        else:
            self.api_group = "ray.io"
            self.api_version = api_version

        if not self.api_group or not self.api_version:
            raise AirflowException(f"Invalid apiVersion format: {api_version}")

        self._client = kube_client
        self.custom_obj_api = custom_obj_api
        self.ray_job_spec: dict = {}
        self.ray_job_name: str | None = name

    def _get_body(self) -> dict:
        """Process and return the RayJob body specification."""
        if not self.template_body:
            raise AirflowException("template_body is required for RayObjectLauncher")

        body = copy.deepcopy(self.template_body)

        if "metadata" not in body:
            body["metadata"] = {}

        if self.name:
            sanitized_name = str(self.name).replace("_", "-").lower()
            sanitized_name = "".join(c for c in sanitized_name if c.isalnum() or c in "-.")
            sanitized_name = sanitized_name.strip("-.")
            if not sanitized_name:
                raise AirflowException("Generated job name is empty after sanitization")
            body["metadata"]["name"] = sanitized_name
        else:
            raise AirflowException("Job name cannot be None or empty")

        if self.namespace:
            body["metadata"]["namespace"] = self.namespace
        else:
            body["metadata"]["namespace"] = "default"

        if "labels" not in body["metadata"]:
            body["metadata"]["labels"] = {}

        body["metadata"]["labels"]["ray.io/job-name"] = body["metadata"]["name"]

        return body

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_ray_job),
    )
    def start_ray_job(self, startup_timeout: int = 600):
        """
        Launch the Ray job and wait for the Ray cluster to be ready.

        :param startup_timeout: Timeout for Ray cluster startup
        :return: Ray job specification dict
        """
        self.log.debug("Ray Job Creation Request Submitted")
        self.ray_job_spec = self.custom_obj_api.create_namespaced_custom_object(
            group=self.api_group,
            version=self.api_version,
            namespace=self.namespace,
            plural=self.plural,
            body=self.body,
        )

        self.ray_job_name = self.ray_job_spec["metadata"]["name"]

        curr_time = dt.now()
        while self._ray_job_not_running(self.ray_job_spec):
            self.log.warning(
                "Ray job submitted but not yet started. job_id: %s",
                self.ray_job_spec["metadata"]["name"],
            )
            delta = dt.now() - curr_time
            if delta.total_seconds() >= startup_timeout:
                job_status_info = self.get_ray_job_status()
                status = job_status_info.get("status", {})
                raise AirflowException(
                    f"Job took too long to start. "
                    f"Job status: {status.get('jobStatus', 'NEW')}, "
                    f"Deployment status: {status.get('jobDeploymentStatus', 'NEW')}"
                )
            time.sleep(10)

        return self.ray_job_spec

    def _ray_job_not_running(self, ray_job_spec: dict) -> bool:
        """Check if Ray job is not yet running."""
        ray_job_info = self.custom_obj_api.get_namespaced_custom_object_status(
            group=self.api_group,
            version=self.api_version,
            namespace=self.namespace,
            name=ray_job_spec["metadata"]["name"],
            plural=self.plural,
        )

        job_status = ray_job_info.get("status", {}).get("jobStatus", RayJobStatus.NEW)
        deployment_status = ray_job_info.get("status", {}).get(
            "jobDeploymentStatus", RayJobDeploymentStatus.NEW
        )

        if job_status == RayJobStatus.FAILED or deployment_status in [
            RayJobDeploymentStatus.FAILED,
            RayJobDeploymentStatus.VALIDATION_FAILED,
        ]:
            error_msg = ray_job_info.get("status", {}).get("message", "Unknown error")
            reason = ray_job_info.get("status", {}).get("reason", "Unknown reason")
            raise AirflowException(f"Ray Job Failed. Reason: {reason}, Error: {error_msg}")

        if deployment_status in [RayJobDeploymentStatus.RUNNING, RayJobDeploymentStatus.COMPLETE]:
            return False

        return deployment_status in [RayJobDeploymentStatus.NEW, RayJobDeploymentStatus.INITIALIZING]

    def delete_ray_job(self, ray_job_name: str | None = None):
        """Delete Ray job."""
        ray_job_name = ray_job_name or self.ray_job_name
        if not ray_job_name:
            self.log.warning("Ray job name is empty or None, skipping deletion")
            return

        try:
            self.custom_obj_api.delete_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                name=ray_job_name,
            )
            self.log.info("Successfully deleted Ray job: %s", ray_job_name)
        except ApiException as e:
            if str(e.status) != "404":
                raise
            self.log.info("Ray job already deleted: %s", ray_job_name)

    def get_ray_job_status(self, ray_job_name: str | None = None) -> dict[str, Any]:
        """Get the status of a Ray job."""
        ray_job_name = ray_job_name or self.ray_job_name
        if not ray_job_name:
            raise AirflowException("Ray job name is required")

        try:
            return self.custom_obj_api.get_namespaced_custom_object_status(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                name=ray_job_name,
                plural=self.plural,
            )
        except ApiException as e:
            if e.status == 404:
                raise AirflowException(f"Ray job not found: {ray_job_name}")
            raise

    def is_job_terminal(self, ray_job_name: str | None = None) -> bool:
        """Check if the Ray job is in a terminal state."""
        ray_job_info = self.get_ray_job_status(ray_job_name)
        status = ray_job_info.get("status", {})
        job_status = status.get("jobStatus", "")
        deployment_status = status.get("jobDeploymentStatus", "")

        terminal_job_statuses = [RayJobStatus.STOPPED, RayJobStatus.SUCCEEDED, RayJobStatus.FAILED]
        terminal_deployment_statuses = [
            RayJobDeploymentStatus.COMPLETE,
            RayJobDeploymentStatus.FAILED,
            RayJobDeploymentStatus.VALIDATION_FAILED,
        ]

        return job_status in terminal_job_statuses or deployment_status in terminal_deployment_statuses
