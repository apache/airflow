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

from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, cast, List
from datetime import datetime as dt
import tenacity
import time

from kubernetes.client import (
    CoreV1Api,
    CustomObjectsApi,
    models as k8s,
    V1PodStatus,
    V1ContainerStatus,
    V1ObjectMeta,
    V1ContainerState,
    V1ContainerStateWaiting,
    V1Pod,
    V1PodList,
)
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import (
    KubernetesHook,
    _load_body_to_dict,
)
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    add_unique_suffix,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import (
    KubernetesSpec,
)
from airflow.providers.cncf.kubernetes.pod_generator import MAX_LABEL_LEN, PodGenerator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.utils.helpers import prune_dict
from airflow.utils.context import Context

if TYPE_CHECKING:
    import jinja2


from airflow.utils.log.logging_mixin import LoggingMixin


def should_retry_start_ray_job(exception: BaseException) -> bool:
    """Check if an Exception indicates a transient error and warrants retrying."""
    if isinstance(exception, ApiException):
        return str(exception.status) == "409"
    return False


class CustomObjectStatus:
    """Status of the PODs."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class RayJobSpec:
    """Ray job spec."""

    def __init__(self, **entries):
        self.metadata: Dict[str, Any] | None = None
        self.__dict__.update(entries)

    if TYPE_CHECKING:

        @property
        def spec(self) -> Dict[str, Any]: ...


class RayCustomObjectLauncher(LoggingMixin):
    """Launches PODS."""

    def __init__(
        self,
        name: str | None,
        namespace: str | None,
        kube_client: CoreV1Api,
        custom_obj_api: CustomObjectsApi,
        template_body: Dict[str, Any],
    ):
        """
        Create custom object launcher(ray job crd).

        :param kube_client: kubernetes client.
        """
        super().__init__()
        self.name = name
        self.namespace = namespace
        self.template_body = template_body
        self.body: dict = self.get_body()
        self.kind = self.body["kind"]
        self.plural = f"{self.kind.lower()}s"
        if self.body.get("apiVersion"):
            self.api_group, self.api_version = self.body["apiVersion"].split("/")
        else:
            self.api_group = self.body["apiGroup"]
            self.api_version = self.body["version"]
        self._client = kube_client
        self.custom_obj_api = custom_obj_api
        self.ray_obj_spec: dict = {}
        self.pod_spec: k8s.V1Pod | None = None

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self._client)

    def get_body(self) -> Dict[str, Any]:
        body: RayJobSpec = RayJobSpec(**self.template_body["ray"])
        body.metadata = {"name": self.name, "namespace": self.namespace}
        if self.template_body.get("kubernetes"):
            k8s_spec: KubernetesSpec = KubernetesSpec(
                **self.template_body["kubernetes"]
            )
            body.spec["volumes"] = k8s_spec.volumes
            if k8s_spec.image_pull_secrets:
                body.spec["imagePullSecrets"] = k8s_spec.image_pull_secrets
            for item in ["submitterPodTemplate.spec", "rayClusterSpec.headGroupSpec.template.spec", "rayClusterSpec.workerGroupSpecs.template.spec"]:
                payload = body.spec
                for key in item.split("."):
                    if payload is not None:
                        payload = payload.get(key)
                if payload is None:
                    continue
                # Env List
                payload["env"] = k8s_spec.env_vars
                payload["envFrom"] = k8s_spec.env_from
                # Volumes
                payload["volumeMounts"] = k8s_spec.volume_mounts
                # Add affinity
                payload["affinity"] = k8s_spec.affinity
                payload["tolerations"] = k8s_spec.tolerations
                payload["nodeSelector"] = k8s_spec.node_selector
                # Labels
                payload["labels"] = body.spec["labels"]

        return body.__dict__

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_ray_job),
    )
    def start_ray_job(self, image=None, entrypoint=None, startup_timeout: int = 600):
        """
        Launch the pod synchronously and waits for completion.

        :param image: image name
        :param entrypoint: entrypoint command
        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :return:
        """
        try:
            if image:
                self.body["spec"]["image"] = image
            if entrypoint:
                self.body["spec"]["entrypoint"] = entrypoint
            self.log.debug("Ray Job Creation Request Submitted")
            self.ray_obj_spec = self.custom_obj_api.create_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                body=self.body,
            )
            self.log.debug("Ray Job Creation Response: %s", self.ray_obj_spec)

            curr_time = dt.now()
            while self.ray_job_not_running(self.ray_obj_spec):
                self.log.warning(
                    "Ray job submitted but not yet started. job_id: %s",
                    self.ray_obj_spec["metadata"]["name"],
                )
                self.check_pod_start_failure()
                delta = dt.now() - curr_time
                if delta.total_seconds() >= startup_timeout:
                    raise AirflowException("Job took too long to start")
                time.sleep(10)

            labels = {"job-name": self.ray_obj_spec["metadata"]["name"]}
            pods: V1PodList = self.pod_manager._client.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=",".join([f"{k}={v}" for k, v in labels.items()]),
            )
            pod_list = cast(list[V1Pod], pods.items)
            if len(pod_list) == 0:
                raise AirflowException("Job has no driver pod")
            if len(pod_list) != 1:
                raise AirflowException("Job has multiple driver pod")
            # Wait for the head pod to come alive
            self.pod_spec = pod_list[0]
        except Exception as e:
            self.log.exception("Exception when attempting to create ray job")
            raise e

        return self.pod_spec, self.ray_obj_spec

    def ray_job_not_running(self, ray_obj_spec):
        """Test if ray_obj_spec has not started."""
        ray_job_info = cast(
            Dict[str, Any],
            self.custom_obj_api.get_namespaced_custom_object_status(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                name=ray_obj_spec["metadata"]["name"],
                plural=self.plural,
            ),
        )
        job_state = ray_job_info.get("status", {}).get("jobStatus", "PENDING")
        if job_state == CustomObjectStatus.FAILED:
            err = (
                ray_job_info.get("status", {})
                .get("applicationState", {})
                .get("message", "N/A")
            )
            try:
                assert self.pod_spec
                self.pod_manager.fetch_container_logs(
                    pod=self.pod_spec, container_name="base"
                )
            except Exception:
                pass
            raise AirflowException(f"Ray Job Failed. Error stack: {err}")
        return job_state == CustomObjectStatus.PENDING

    def check_pod_start_failure(self):
        try:
            assert self.pod_spec is not None
            waiting_status = cast(
                V1ContainerStateWaiting,
                cast(
                    V1ContainerState,
                    cast(
                        List[V1ContainerStatus],
                        cast(
                            V1PodStatus, self.pod_manager.read_pod(self.pod_spec).status
                        ).container_statuses,
                    )[0].state,
                ).waiting,
            )
            waiting_reason = waiting_status.reason
            waiting_message = waiting_status.message
        except Exception:
            return
        if waiting_reason not in ("ContainerCreating", "PodInitializing"):
            raise AirflowException(
                f"Ray Job Failed. Status: {waiting_reason}, Error: {waiting_message}"
            )

    def delete_ray_job(self, ray_job_name=None):
        """Delete ray job."""
        ray_job_name = ray_job_name or self.ray_obj_spec.get("metadata", {}).get("name")
        if not ray_job_name:
            self.log.warning("Ray job not found: %s", ray_job_name)
            return
        try:
            self.custom_obj_api.delete_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                name=ray_job_name,
            )
        except ApiException as e:
            # If the pod is already deleted
            if str(e.status) != "404":
                raise


class RayKubernetesOperator(KubernetesPodOperator):
    template_fields = [
        "application_file",
        "namespace",
        "template_spec",
        "kubernetes_conn_id",
    ]
    template_fields_renderers = {"template_spec": "py"}
    template_ext = ("yaml", "yml", "json")
    ui_color = "#f4a460"

    BASE_CONTAINER_NAME = "base"

    def __init__(
        self,
        *,
        image: str | None = None,
        entrypoint: str | None = None,
        namespace: str = "default",
        name: str | None = None,
        application_file: str | None = None,
        template_spec=None,
        get_logs: bool = True,
        do_xcom_push: bool = False,
        success_run_history_limit: int = 1,
        startup_timeout_seconds=600,
        log_events_on_failure: bool = False,
        reattach_on_restart: bool = True,
        delete_on_termination: bool = True,
        kubernetes_conn_id: str = "kubernetes_default",
        random_name_suffix: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(name=name, **kwargs)
        self.image = image
        self.entrypoint = entrypoint
        self.application_file = application_file
        self.template_spec = template_spec
        self.kubernetes_conn_id = kubernetes_conn_id
        self.startup_timeout_seconds = startup_timeout_seconds
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.do_xcom_push = do_xcom_push
        self.namespace = namespace
        self.get_logs = get_logs
        self.log_events_on_failure = log_events_on_failure
        self.success_run_history_limit = success_run_history_limit
        self.random_name_suffix = random_name_suffix

        if self.base_container_name != self.BASE_CONTAINER_NAME:
            self.log.warning(
                "base_container_name is not supported and will be overridden to %s",
                self.BASE_CONTAINER_NAME,
            )
            self.base_container_name = self.BASE_CONTAINER_NAME

        if self.get_logs and self.container_logs != self.BASE_CONTAINER_NAME:
            self.log.warning(
                "container_logs is not supported and will be overridden to %s",
                self.BASE_CONTAINER_NAME,
            )
            self.container_logs = [self.BASE_CONTAINER_NAME]

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Mapping[str, Any],
        jinja_env: "jinja2.Environment",
        seen_oids: set,
    ) -> None:
        if id(content) not in seen_oids and isinstance(content, k8s.V1EnvVar):
            seen_oids.add(id(content))
            self._do_render_template_fields(
                content, ("value", "name"), cast(Context, context), jinja_env, seen_oids
            )
            return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    def manage_template_specs(self):
        if self.application_file:
            try:
                filepath = Path(self.application_file.rstrip()).resolve(strict=True)
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                application_file_body = self.application_file
            else:
                application_file_body = filepath.read_text()
            template_body = _load_body_to_dict(application_file_body)
            if not isinstance(template_body, dict):
                msg = f"application_file body can't transformed into the dictionary:\n{application_file_body}"
                raise TypeError(msg)
        elif self.template_spec:
            template_body = self.template_spec
        else:
            raise AirflowException(
                "either application_file or template_spec should be passed"
            )
        if "ray" not in template_body:
            template_body = {"ray": template_body}
        return template_body

    def create_job_name(self):
        name = (
            self.name
            or self.template_body.get("ray", {}).get("metadata", {}).get("name")
            or self.task_id
        )

        if self.random_name_suffix:
            updated_name = add_unique_suffix(name=name, max_len=MAX_LABEL_LEN)
        else:
            # truncation is required to maintain the same behavior as before
            updated_name = name[:MAX_LABEL_LEN]

        return self._set_name(updated_name)

    @staticmethod
    def _get_pod_identifying_label_string(labels) -> str:
        filtered_labels = {
            label_id: label
            for label_id, label in labels.items()
            if label_id != "try_number"
        }
        return ",".join(
            [
                label_id + "=" + label
                for label_id, label in sorted(filtered_labels.items())
            ]
        )

    @staticmethod
    def create_labels_for_pod(
        context: dict | None = None, include_try_number: bool = True
    ) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash.

        :param include_try_number: add try number to labels
        :param context: task context provided by airflow DAG
        :return: dict.
        """
        if not context:
            return {}

        ti = context["ti"]
        run_id = context["run_id"]

        labels = {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": run_id,
            "ray_kubernetes_operator": "True",
        }

        # If running on Airflow 2.3+:
        map_index = getattr(ti, "map_index", -1)
        if map_index >= 0:
            labels["map_index"] = map_index

        if include_try_number:
            labels.update(try_number=ti.try_number)

        # In the case of sub dags this is just useful
        if getattr(context["dag"], "is_subdag", False):
            labels["parent_dag_id"] = context["dag"].parent_dag.dag_id
        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self.client)

    @staticmethod
    def _try_numbers_match(context, pod) -> bool:
        return pod.metadata.labels["try_number"] == context["ti"].try_number

    @property
    def template_body(self):
        """Templated body for CustomObjectLauncher."""
        return self.manage_template_specs()

    def find_ray_job(self, context):
        labels = self.create_labels_for_pod(context, include_try_number=False)
        label_selector = (
            self._get_pod_identifying_label_string(labels) + ",ray.io/node-type=head"
        )
        pod_list = self.client.list_namespaced_pod(
            self.namespace, label_selector=label_selector
        ).items

        pod = None
        if len(pod_list) > 1:  # and self.reattach_on_restart:
            raise AirflowException(
                f"More than one pod running with labels: {label_selector}"
            )
        elif len(pod_list) == 1:
            pod = pod_list[0]
            self.log.info(
                "Found matching head pod %s with labels %s",
                pod.metadata.name,
                pod.metadata.labels,
            )
            self.log.info("`try_number` of task_instance: %s", context["ti"].try_number)
            self.log.info("`try_number` of pod: %s", pod.metadata.labels["try_number"])
        return pod

    def get_or_create_ray_crd(
        self, launcher: RayCustomObjectLauncher, context
    ) -> k8s.V1Pod:
        if self.reattach_on_restart:
            head_pod = self.find_ray_job(context)
            if head_pod:
                return head_pod

        head_pod, ray_obj_spec = launcher.start_ray_job(
            image=self.image,
            entrypoint=self.entrypoint,
            startup_timeout=self.startup_timeout_seconds,
        )
        return head_pod

    def process_pod_deletion(self, pod, *, reraise=True):
        if pod is not None:
            name = cast(str, cast(V1ObjectMeta, pod.metadata).name)
            if self.delete_on_termination:
                self.log.info("Deleting ray job: %s", name.replace("-head", ""))
                self.launcher.delete_ray_job(name.replace("-head", ""))
            else:
                self.log.info("skipping deleting ray job: %s", name)

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster
            or self.template_body.get("kubernetes", {}).get("in_cluster", False),
            config_file=self.config_file
            or self.template_body.get("kubernetes", {}).get("kube_config_file", None),
            cluster_context=self.cluster_context
            or self.template_body.get("kubernetes", {}).get("cluster_context", None),
        )
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    @cached_property
    def custom_obj_api(self) -> CustomObjectsApi:
        return CustomObjectsApi()

    def execute(self, context: Context):
        self.name = self.create_job_name()

        self.log.info("Creating RayJob.")
        self.launcher = RayCustomObjectLauncher(
            name=self.name,
            namespace=self.namespace,
            kube_client=self.client,
            custom_obj_api=self.custom_obj_api,
            template_body=self.template_body,
        )
        self.pod = self.get_or_create_ray_crd(self.launcher, context)
        self.pod_request_obj = self.launcher.pod_spec

        return super().execute(context=context)

    def on_kill(self) -> None:
        if self.launcher:
            self.log.debug("Deleting ray job for task %s", self.task_id)
            self.launcher.delete_ray_job()

    def patch_already_checked(self, pod: k8s.V1Pod, *, reraise=True):
        """Add an "already checked" annotation to ensure we don't reattach on retries."""
        metadata = cast(V1ObjectMeta, pod.metadata)
        name = cast(str, metadata.name)
        cast(Dict[str, str], metadata.labels)["already_checked"] = "True"
        body = PodGenerator.serialize_pod(pod)
        self.client.patch_namespaced_pod(name, metadata.namespace, body)

    def dry_run(self) -> None:
        """Print out the ray job that would be created by this operator."""
        print(prune_dict(self.launcher.body, mode="strict"))
