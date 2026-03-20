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
"""Operator for managing Ray workloads on Kubernetes via KubeRay."""

from __future__ import annotations

import copy
import time
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook, _load_body_to_dict
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_unique_suffix
from airflow.providers.cncf.kubernetes.pod_generator import MAX_LABEL_LEN
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.providers.cncf.kubernetes.utils.ray_job_status import (
    RayJobDeploymentStatus,
    is_ray_job_successful,
    is_ray_job_terminal,
)
from airflow.providers.common.compat.sdk import AirflowException, BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

RAY_API_GROUP = "ray.io"
RAY_API_VERSION = "v1"
RAY_PLURAL = "rayjobs"


class RayKubernetesOperator(BaseOperator):
    """
    Create a RayJob object in a Kubernetes cluster via the KubeRay operator.

    Submits a RayJob CRD and optionally waits for completion. Uses ``KubernetesHook``
    for all CRD operations (create, get, delete).

    .. seealso::
        For more detail about RayJob have a look at the reference:
        https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html

    :param application_file: filepath to a RayJob YAML/JSON definition
    :param template_spec: RayJob specification as a Python dict
    :param namespace: kubernetes namespace for the RayJob
    :param name: name of the RayJob (optional, extracted from spec if not provided)
    :param wait_for_completion: whether to wait for the job to finish (default: True)
    :param get_logs: stream logs from the Ray head pod
    :param startup_timeout_seconds: timeout for Ray cluster startup
    :param job_timeout_seconds: timeout for the Ray job to complete
    :param poll_interval: seconds between status polls
    :param reattach_on_restart: reattach to existing RayJob on scheduler restart
    :param delete_on_termination: delete the RayJob CRD when done
    :param kubernetes_conn_id: Kubernetes connection ID
    :param random_name_suffix: add a random suffix to the job name
    """

    template_fields = [
        "application_file",
        "template_spec",
        "namespace",
        "kubernetes_conn_id",
    ]
    template_fields_renderers = {"application_file": "yaml", "template_spec": "py"}
    template_ext = ("yaml", "yml", "json")
    ui_color = "#028edd"

    BASE_CONTAINER_NAME = "ray-head"

    def __init__(
        self,
        *,
        application_file: str | None = None,
        template_spec: dict | None = None,
        namespace: str = "default",
        name: str | None = None,
        wait_for_completion: bool = True,
        get_logs: bool = True,
        startup_timeout_seconds: int = 600,
        job_timeout_seconds: int = 7200,
        poll_interval: int = 10,
        reattach_on_restart: bool = True,
        delete_on_termination: bool = True,
        kubernetes_conn_id: str = "kubernetes_default",
        in_cluster: bool | None = None,
        config_file: str | None = None,
        cluster_context: str | None = None,
        random_name_suffix: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.application_file = application_file
        self.template_spec = template_spec
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.in_cluster = in_cluster
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.wait_for_completion = wait_for_completion
        self.startup_timeout_seconds = startup_timeout_seconds
        self.job_timeout_seconds = job_timeout_seconds
        self.poll_interval = poll_interval
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.get_logs = get_logs
        self.random_name_suffix = random_name_suffix
        self.ray_job_name: str | None = None

    @cached_property
    def hook(self) -> KubernetesHook:
        """Get KubernetesHook instance."""
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    @cached_property
    def pod_manager(self) -> PodManager:
        """Get PodManager for log streaming."""
        return PodManager(kube_client=self.hook.core_v1_client)

    # --- Template & name handling ---

    def _parse_template(self) -> dict:
        """Parse and validate the RayJob specification."""
        if self.application_file:
            try:
                filepath = Path(self.application_file.rstrip()).resolve(strict=True)
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                application_file_body = self.application_file
            else:
                application_file_body = filepath.read_text()

            template_body = _load_body_to_dict(application_file_body)
            if not isinstance(template_body, dict):
                raise TypeError(
                    f"application_file body can't be transformed into dictionary:\n{application_file_body}"
                )
        elif self.template_spec:
            template_body = copy.deepcopy(self.template_spec)
        else:
            raise AirflowException("Either application_file or template_spec must be provided")

        template_body.setdefault("apiVersion", "ray.io/v1")
        template_body.setdefault("kind", "RayJob")
        template_body.setdefault("metadata", {})
        template_body["metadata"].setdefault("name", self.task_id)
        template_body["metadata"].setdefault("namespace", self.namespace)
        return template_body

    def _create_job_name(self) -> str:
        """Generate a unique, RFC 1123 compliant job name."""
        name = self.name or self.task_id
        sanitized = pod_generator.make_safe_label_value(str(name).replace("_", "-"))
        if self.random_name_suffix:
            return add_unique_suffix(name=sanitized, max_len=MAX_LABEL_LEN)
        return sanitized[:MAX_LABEL_LEN]

    @staticmethod
    def _get_ti_pod_labels(context: Context | None = None) -> dict[str, str]:
        """Generate labels for reattachment after scheduler restart."""
        if not context:
            return {}

        context_dict = cast("dict", context)
        ti = context_dict["ti"]

        labels = {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": context_dict["run_id"],
            "ray_kubernetes_operator": "True",
            "try_number": str(ti.try_number),
        }

        map_index = getattr(ti, "map_index", -1)
        if map_index is not None and map_index >= 0:
            labels["map_index"] = str(map_index)

        return {
            pod_generator.make_safe_label_value(k.replace("_", "-")): pod_generator.make_safe_label_value(
                str(v)
            )
            for k, v in labels.items()
        }

    def _build_ray_job_body(self, context: Context) -> dict:
        """Build the RayJob CRD body with labels for reattachment."""
        body = copy.deepcopy(self._parse_template())
        body["metadata"]["name"] = self.ray_job_name
        body["metadata"].setdefault("labels", {})
        body["metadata"]["labels"].update(self._get_ti_pod_labels(context))
        return body

    # --- CRD operations (all via KubernetesHook) ---

    def _submit_ray_job(self, context: Context) -> None:
        """Create the RayJob CRD via KubernetesHook."""
        body = self._build_ray_job_body(context)
        self.log.info("Creating RayJob: %s", self.ray_job_name)
        self.hook.create_custom_object(
            group=RAY_API_GROUP,
            version=RAY_API_VERSION,
            plural=RAY_PLURAL,
            body=body,
            namespace=self.namespace,
        )

    def _get_ray_job_status(self) -> tuple[str, str]:
        """Get current job and deployment status from the RayJob CRD."""
        obj = self.hook.get_custom_object(
            group=RAY_API_GROUP,
            version=RAY_API_VERSION,
            plural=RAY_PLURAL,
            name=self.ray_job_name,
            namespace=self.namespace,
        )
        status = obj.get("status", {})
        return status.get("jobStatus", ""), status.get("jobDeploymentStatus", "")

    def _delete_ray_job(self) -> None:
        """Delete the RayJob CRD via KubernetesHook."""
        if not self.ray_job_name:
            return
        try:
            self.hook.delete_custom_object(
                group=RAY_API_GROUP,
                version=RAY_API_VERSION,
                plural=RAY_PLURAL,
                name=self.ray_job_name,
                namespace=self.namespace,
            )
            self.log.info("Deleted RayJob: %s", self.ray_job_name)
        except Exception:
            self.log.warning("Failed to delete RayJob %s (may already be deleted)", self.ray_job_name)

    # --- Reattach ---

    def _find_existing_ray_job(self, context: Context) -> str | None:
        """Find an existing non-terminal RayJob for reattachment."""
        try:
            labels = self._get_ti_pod_labels(context)
            if not labels:
                return None

            label_selector = ",".join(f"{k}={v}" for k, v in labels.items())
            ray_jobs = self.hook.list_custom_objects(
                group=RAY_API_GROUP,
                version=RAY_API_VERSION,
                plural=RAY_PLURAL,
                namespace=self.namespace,
                label_selector=label_selector,
            )

            for item in ray_jobs.get("items", []):
                job_name = item["metadata"]["name"]
                job_status = item.get("status", {}).get("jobStatus", "")
                dep_status = item.get("status", {}).get("jobDeploymentStatus", "")

                if is_ray_job_terminal(job_status, dep_status):
                    continue

                self.log.info("Reattaching to existing RayJob: %s", job_name)
                return job_name
        except Exception as e:
            self.log.warning("Error searching for existing RayJob: %s", e)
        return None

    # --- Monitoring ---

    def _wait_for_terminal_state(self) -> str:
        """Poll the RayJob status until terminal or timeout."""
        start_time = time.monotonic()
        cluster_ready = False
        last_log_time = None
        head_pod_cache: dict[str, Any] = {}

        while True:
            elapsed = time.monotonic() - start_time

            try:
                job_status, dep_status = self._get_ray_job_status()
            except Exception:
                self.log.exception("Failed to get RayJob status for %s", self.ray_job_name)
                raise

            self.log.info(
                "RayJob %s — job: %s, deployment: %s (%.0fs elapsed)",
                self.ray_job_name,
                job_status or "NEW",
                dep_status or "NEW",
                elapsed,
            )

            if is_ray_job_terminal(job_status, dep_status):
                if is_ray_job_successful(job_status, dep_status):
                    return "SUCCEEDED"
                raise AirflowException(f"RayJob failed — job: {job_status}, deployment: {dep_status}")

            # Stream logs when running
            if (
                dep_status in (RayJobDeploymentStatus.RUNNING, RayJobDeploymentStatus.COMPLETE)
                and self.get_logs
            ):
                last_log_time = self._stream_logs(last_log_time, head_pod_cache)

            # Startup timeout
            if not cluster_ready:
                if dep_status and dep_status != RayJobDeploymentStatus.INITIALIZING:
                    cluster_ready = True
                elif elapsed > self.startup_timeout_seconds:
                    raise AirflowException(
                        f"Ray cluster startup timed out after {elapsed:.0f}s "
                        f"(deployment: {dep_status or 'NEW'})"
                    )

            # Job timeout
            if elapsed > self.job_timeout_seconds:
                raise AirflowException(
                    f"RayJob timed out after {elapsed:.0f}s "
                    f"(job: {job_status or 'NEW'}, deployment: {dep_status or 'NEW'})"
                )

            time.sleep(self.poll_interval)

    def _stream_logs(self, last_log_time: Any, head_pod_cache: dict) -> Any:
        """Stream logs from the Ray head pod."""
        try:
            obj = self.hook.get_custom_object(
                group=RAY_API_GROUP,
                version=RAY_API_VERSION,
                plural=RAY_PLURAL,
                name=self.ray_job_name,
                namespace=self.namespace,
            )
            cluster_name = obj.get("status", {}).get("rayClusterName")
            if not cluster_name:
                return last_log_time

            head_pod = head_pod_cache.get(cluster_name)
            if head_pod and hasattr(head_pod, "status") and head_pod.status.phase != "Running":
                head_pod_cache.pop(cluster_name, None)
                head_pod = None

            if not head_pod:
                label_selector = f"ray.io/node-type=head,ray.io/cluster={cluster_name}"
                pods = self.hook.core_v1_client.list_namespaced_pod(
                    self.namespace, label_selector=label_selector
                ).items
                running = [p for p in pods if p.status.phase == "Running"]
                if running:
                    head_pod = running[0]
                    head_pod_cache[cluster_name] = head_pod

            if head_pod:
                try:
                    result = self.pod_manager.fetch_container_logs(
                        pod=head_pod,
                        container_name=self.BASE_CONTAINER_NAME,
                        follow=False,
                        since_time=last_log_time,
                    )
                    return result.last_log_time
                except Exception:
                    head_pod_cache.pop(cluster_name, None)
        except Exception as e:
            self.log.debug("Error streaming logs: %s", e)
        return last_log_time

    # --- Execution ---

    def execute(self, context: Context) -> str | None:
        """Execute the RayJob on Kubernetes."""
        # Try reattaching before generating a new name
        if self.reattach_on_restart:
            existing = self._find_existing_ray_job(context)
            if existing:
                self.ray_job_name = existing
            else:
                self.ray_job_name = self._create_job_name()
                self._submit_ray_job(context)
        else:
            self.ray_job_name = self._create_job_name()
            self._submit_ray_job(context)

        try:
            if self.wait_for_completion:
                result = self._wait_for_terminal_state()
                if self.delete_on_termination:
                    self._delete_ray_job()
                return result
            return None
        except Exception:
            if self.delete_on_termination:
                self._delete_ray_job()
            raise

    def on_kill(self) -> None:
        """Handle task kill signal."""
        if self.delete_on_termination:
            self._delete_ray_job()
