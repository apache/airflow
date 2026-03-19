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
from airflow.providers.cncf.kubernetes.operators.ray_object_launcher import RayObjectLauncher
from airflow.providers.cncf.kubernetes.pod_generator import MAX_LABEL_LEN
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.providers.common.compat.sdk import AirflowException, BaseOperator
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    import jinja2
    from kubernetes.client import CoreV1Api, CustomObjectsApi

    from airflow.providers.common.compat.sdk import Context


class RayKubernetesOperator(BaseOperator):
    """
    Create a RayJob object in a Kubernetes cluster via the KubeRay operator.

    .. seealso::
        For more detail about Ray Job Object have a look at the reference:
        https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html

    :param application_file: filepath to kubernetes custom_resource_definition of RayJob (YAML/JSON file)
    :param template_spec: kubernetes RayJob specification as a Python dict
    :param namespace: kubernetes namespace to put RayJob
    :param name: name of the RayJob (optional, will be extracted from YAML if not provided)
    :param get_logs: get the stdout of the Ray head pod as logs of the tasks
    :param startup_timeout_seconds: timeout in seconds to startup the Ray cluster
    :param job_timeout_seconds: timeout in seconds for the Ray job to complete
    :param poll_interval: interval in seconds to poll the Ray job status
    :param reattach_on_restart: if the scheduler dies while the job is running, reattach and monitor
    :param delete_on_termination: What to do when the job reaches its final state, or the execution
        is interrupted. If True (default), delete the RayJob; if False, leave the RayJob.
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :param random_name_suffix: If True, adds a random suffix to the job name
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
        self.startup_timeout_seconds = startup_timeout_seconds
        self.job_timeout_seconds = job_timeout_seconds
        self.poll_interval = poll_interval
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.get_logs = get_logs
        self.random_name_suffix = random_name_suffix

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set,
    ) -> None:
        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    def _manage_template_specs(self) -> dict:
        """Process and validate the RayJob specification."""
        if self.application_file:
            try:
                filepath = Path(self.application_file.rstrip()).resolve(strict=True)
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                application_file_body = self.application_file
            else:
                application_file_body = filepath.read_text()

            template_body = _load_body_to_dict(application_file_body)
            if not isinstance(template_body, dict):
                msg = f"application_file body can't be transformed into dictionary:\n{application_file_body}"
                raise TypeError(msg)
        elif self.template_spec:
            template_body = self.template_spec
        else:
            raise AirflowException("Either application_file or template_spec must be provided")

        return self._validate_and_enhance_template(template_body)

    def _validate_and_enhance_template(self, template_body: dict) -> dict:
        """Validate and enhance the RayJob template."""
        if "apiVersion" not in template_body:
            template_body["apiVersion"] = "ray.io/v1"
        if "kind" not in template_body:
            template_body["kind"] = "RayJob"
        if "metadata" not in template_body:
            template_body["metadata"] = {}
        if "name" not in template_body["metadata"]:
            template_body["metadata"]["name"] = self.task_id
        if "namespace" not in template_body["metadata"]:
            template_body["metadata"]["namespace"] = self.namespace
        return template_body

    def _create_job_name(self) -> str:
        """Generate a unique job name."""
        name = self.name or self.task_id

        if not name and (self.application_file or self.template_spec):
            try:
                template_specs = self._manage_template_specs()
                name = template_specs.get("metadata", {}).get("name") or self.task_id
            except Exception:
                name = self.task_id

        sanitized_name = pod_generator.make_safe_label_value(str(name).replace("_", "-"))

        if self.random_name_suffix:
            updated_name = add_unique_suffix(name=sanitized_name, max_len=MAX_LABEL_LEN)
        else:
            updated_name = sanitized_name[:MAX_LABEL_LEN]

        self.name = updated_name
        return updated_name

    @staticmethod
    def _get_ti_pod_labels(context: Context | None = None, include_try_number: bool = True) -> dict[str, str]:
        """
        Generate labels for the RayJob CRD to enable reattachment after crashes.

        :param include_try_number: add try number to labels
        :param context: task context provided by airflow DAG
        :return: dict of safe Kubernetes labels
        """
        if not context:
            return {}

        context_dict = cast("dict", context)
        ti = context_dict["ti"]
        run_id = context_dict["run_id"]

        labels = {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": run_id,
            "ray_kubernetes_operator": "True",
        }

        map_index = getattr(ti, "map_index", -1)
        if map_index is not None and map_index >= 0:
            labels["map_index"] = str(map_index)

        if include_try_number:
            labels["try_number"] = str(ti.try_number)

        safe_labels = {}
        for label_id, label in labels.items():
            safe_key = pod_generator.make_safe_label_value(str(label_id).replace("_", "-"))
            safe_value = pod_generator.make_safe_label_value(str(label))
            safe_labels[safe_key] = safe_value
        return safe_labels

    @cached_property
    def pod_manager(self) -> PodManager:
        """Get or create a PodManager instance."""
        return PodManager(kube_client=self.client)

    @property
    def _template_body(self) -> dict:
        """Get the templated body for RayObjectLauncher."""
        return self._manage_template_specs()

    def _find_existing_ray_job(self, context: Context) -> str | None:
        """Find existing RayJob CRD for reattachment."""
        try:
            labels = self._get_ti_pod_labels(context)
            if not labels:
                return None

            label_selector = ",".join(f"{k}={v}" for k, v in labels.items())
            template_body = self._template_body
            api_version = template_body.get("apiVersion", "ray.io/v1")
            api_group, version = api_version.split("/", 1) if "/" in api_version else ("ray.io", api_version)

            ray_jobs = self.custom_obj_api.list_namespaced_custom_object(
                group=api_group,
                version=version,
                namespace=self.namespace,
                plural="rayjobs",
                label_selector=label_selector,
            )

            if not ray_jobs.get("items"):
                return None

            existing_job = ray_jobs["items"][0]
            job_name = existing_job["metadata"]["name"]
            job_status = existing_job.get("status", {}).get("jobStatus", "")
            deployment_status = existing_job.get("status", {}).get("jobDeploymentStatus", "")

            self.log.info(
                "Found existing RayJob: %s (job=%s, deployment=%s)",
                job_name,
                job_status or "NEW",
                deployment_status or "NEW",
            )

            if job_status in ("SUCCEEDED", "FAILED", "STOPPED"):
                return None
            if deployment_status in ("Complete", "Failed", "ValidationFailed"):
                return None

            return job_name
        except Exception as e:
            self.log.warning("Error finding existing RayJob: %s", e)
            return None

    def _get_or_create_ray_job(self, context: Context) -> RayObjectLauncher:
        """Get existing or create new Ray job launcher."""
        existing_job_name = None

        if self.reattach_on_restart:
            existing_job_name = self._find_existing_ray_job(context)

        if existing_job_name:
            self.log.info("Reattaching to existing RayJob: %s", existing_job_name)
            reattach_template = {
                "apiVersion": "ray.io/v1",
                "kind": "RayJob",
                "metadata": {"name": existing_job_name, "namespace": self.namespace},
            }
            launcher = RayObjectLauncher(
                name=existing_job_name,
                namespace=self.namespace,
                kube_client=self.client,
                custom_obj_api=self.custom_obj_api,
                template_body=reattach_template,
            )
            launcher.ray_job_name = existing_job_name

            try:
                job_status_info = launcher.get_ray_job_status()
                job_status = job_status_info.get("status", {}).get("jobStatus", "")
                self.log.info(
                    "Reattached to RayJob %s with status: %s", existing_job_name, job_status or "NEW"
                )
                return launcher
            except Exception as e:
                self.log.warning(
                    "Failed to get status for RayJob %s: %s. Creating new job.", existing_job_name, e
                )

        labels = self._get_ti_pod_labels(context)
        template_body_copy = copy.deepcopy(self._template_body)

        if "metadata" not in template_body_copy:
            template_body_copy["metadata"] = {}
        if "labels" not in template_body_copy["metadata"]:
            template_body_copy["metadata"]["labels"] = {}
        if labels:
            template_body_copy["metadata"]["labels"].update(labels)

        launcher = RayObjectLauncher(
            name=self.name,
            namespace=self.namespace,
            kube_client=self.client,
            custom_obj_api=self.custom_obj_api,
            template_body=template_body_copy,
        )

        self.log.info("Creating new RayJob: %s", self.name)
        ray_job_spec = launcher.start_ray_job(startup_timeout=self.startup_timeout_seconds)
        launcher.ray_job_spec = ray_job_spec

        return launcher

    def _monitor_ray_job(self, launcher: RayObjectLauncher) -> str:
        """
        Monitor the Ray job until it completes or times out.

        :param launcher: The RayObjectLauncher instance
        :return: Final job status
        """
        start_time = time.monotonic()
        cluster_ready = False
        last_log_time = None
        head_pod_cache: dict[str, Any] = {}

        while True:
            elapsed = time.monotonic() - start_time

            try:
                job_status_info = launcher.get_ray_job_status()
                job_status = job_status_info.get("status", {}).get("jobStatus", "")
                deployment_status = job_status_info.get("status", {}).get("jobDeploymentStatus", "")
            except AirflowException as e:
                if "not found" in str(e).lower() and self.delete_on_termination:
                    self.log.info("Ray job not found (likely deleted after completion)")
                    return "SUCCEEDED"
                raise

            self.log.info(
                "Ray job status - Job: %s, Deployment: %s (elapsed: %.0fs)",
                job_status or "NEW",
                deployment_status or "NEW",
                elapsed,
            )

            # Terminal success
            if job_status == "SUCCEEDED":
                return "SUCCEEDED"

            if deployment_status == "Complete":
                if job_status == "SUCCEEDED":
                    return "SUCCEEDED"
                if job_status in ("FAILED", "STOPPED"):
                    error_msg = job_status_info.get("status", {}).get("message", "Job execution failed")
                    raise AirflowException(f"Ray job {job_status}: {error_msg}")

            # Terminal failure
            if job_status in ("FAILED", "STOPPED"):
                error_msg = job_status_info.get("status", {}).get("message", "Job execution failed")
                raise AirflowException(f"Ray job {job_status}: {error_msg}")

            if deployment_status in ("Failed", "ValidationFailed"):
                error_msg = job_status_info.get("status", {}).get("message", "Deployment failed")
                raise AirflowException(f"Ray job deployment {deployment_status}: {error_msg}")

            # Stream logs
            if deployment_status in ("Running", "Complete") and self.get_logs:
                last_log_time = self._stream_logs(job_status_info, last_log_time, head_pod_cache)

            # Startup timeout
            if not cluster_ready:
                if deployment_status not in ("", "Initializing"):
                    cluster_ready = True
                elif elapsed > self.startup_timeout_seconds:
                    raise AirflowException(
                        f"Ray cluster startup timed out after {elapsed:.0f}s. "
                        f"Deployment status: {deployment_status or 'NEW'}"
                    )

            # Job timeout
            if elapsed > self.job_timeout_seconds:
                raise AirflowException(
                    f"Ray job timed out after {elapsed:.0f}s. "
                    f"Job: {job_status or 'NEW'}, Deployment: {deployment_status or 'NEW'}"
                )

            time.sleep(self.poll_interval)

    def _stream_logs(self, job_status_info: dict, last_log_time: Any, head_pod_cache: dict) -> Any:
        """Stream logs from the Ray head pod."""
        try:
            ray_cluster_name = job_status_info.get("status", {}).get("rayClusterName")
            if not ray_cluster_name:
                return last_log_time

            head_pod = head_pod_cache.get(ray_cluster_name)

            if head_pod and hasattr(head_pod, "status") and head_pod.status.phase != "Running":
                del head_pod_cache[ray_cluster_name]
                head_pod = None

            if not head_pod:
                label_selector = f"ray.io/node-type=head,ray.io/cluster={ray_cluster_name}"
                pod_list = self.client.list_namespaced_pod(
                    self.namespace, label_selector=label_selector
                ).items
                running_pods = [p for p in pod_list if p.status.phase == "Running"]
                if running_pods:
                    head_pod = running_pods[0]
                    head_pod_cache[ray_cluster_name] = head_pod

            if head_pod:
                try:
                    pod_log_status = self.pod_manager.fetch_container_logs(
                        pod=head_pod,
                        container_name=self.BASE_CONTAINER_NAME,
                        follow=False,
                        since_time=last_log_time,
                    )
                    return pod_log_status.last_log_time
                except Exception as e:
                    self.log.debug("Could not fetch logs: %s", e)
                    head_pod_cache.pop(ray_cluster_name, None)
        except Exception as e:
            self.log.debug("Error fetching logs: %s", e)

        return last_log_time

    @cached_property
    def hook(self) -> KubernetesHook:
        """Get or create a KubernetesHook instance."""
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    @cached_property
    def client(self) -> CoreV1Api:
        """Get the Kubernetes CoreV1Api client."""
        return self.hook.core_v1_client

    @cached_property
    def custom_obj_api(self) -> CustomObjectsApi:
        """Get the Kubernetes CustomObjectsApi client."""
        return self.hook.custom_object_client

    def execute(self, context: Context):
        """Execute the RayJob on Kubernetes."""
        self.name = self._create_job_name()

        try:
            self.log.info("Processing RayJob: %s", self.name)
            self.launcher = self._get_or_create_ray_job(context)
            final_status = self._monitor_ray_job(self.launcher)

            if self.delete_on_termination:
                self.launcher.delete_ray_job()

            return final_status

        except Exception:
            if self.delete_on_termination and hasattr(self, "launcher"):
                try:
                    self.launcher.delete_ray_job()
                except Exception as delete_error:
                    self.log.warning("Failed to delete Ray job on failure: %s", delete_error)
            raise

    def on_kill(self) -> None:
        """Handle task kill signal."""
        if hasattr(self, "launcher"):
            self.log.debug("Deleting Ray job for task %s", self.task_id)
            self.launcher.delete_ray_job()

    def dry_run(self) -> None:
        """Print out the Ray job that would be created by this operator."""
        generated_name = self._create_job_name()
        template_body = self._template_body

        temp_launcher = RayObjectLauncher(
            name=generated_name,
            namespace=self.namespace,
            kube_client=self.client,
            custom_obj_api=self.custom_obj_api,
            template_body=template_body,
        )
        print(prune_dict(temp_launcher.body, mode="strict"))
