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

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from kubernetes.client import CoreV1Api, CustomObjectsApi, models as k8s

from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook, _load_body_to_dict
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_unique_suffix
from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import CustomObjectLauncher
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.pod_generator import MAX_LABEL_LEN, PodGenerator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    import jinja2

    from airflow.sdk import Context


class SparkKubernetesOperator(KubernetesPodOperator):
    """
    Creates sparkApplication object in kubernetes cluster.

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.3.3-3.1.1/docs/api-docs.md#sparkapplication

    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
    :param code_path: path to the spark code in image,
    :param namespace: kubernetes namespace to put sparkApplication
    :param name: name of the pod in which the task will run, will be used (plus a random
        suffix if random_name_suffix is True) to generate a pod id (DNS-1123 subdomain,
        containing only [a-z0-9.-]).
    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
    :param template_spec: kubernetes sparkApplication specification
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :param success_run_history_limit: Number of past successful runs of the application to keep.
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor.
        When enabled, the operator automatically adds Airflow task context labels (dag_id, task_id, run_id)
        to the driver and executor pods to enable finding them for reattachment.
    :param delete_on_termination: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :param random_name_suffix: If True, adds a random suffix to the pod name
    """

    template_fields = ["application_file", "namespace", "template_spec", "kubernetes_conn_id"]
    template_fields_renderers = {"template_spec": "py"}
    template_ext = ("yaml", "yml", "json")
    ui_color = "#f4a460"

    BASE_CONTAINER_NAME = "spark-kubernetes-driver"

    def __init__(
        self,
        *,
        image: str | None = None,
        code_path: str | None = None,
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
        self.code_path = code_path
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

        # fix mypy typing
        self.base_container_name: str
        self.container_logs: list[str]

        if self.base_container_name != self.BASE_CONTAINER_NAME:
            self.log.warning(
                "base_container_name is not supported and will be overridden to %s", self.BASE_CONTAINER_NAME
            )
            self.base_container_name = self.BASE_CONTAINER_NAME

        if self.get_logs and self.container_logs != self.BASE_CONTAINER_NAME:
            self.log.warning(
                "container_logs is not supported and will be overridden to %s", self.BASE_CONTAINER_NAME
            )
            self.container_logs = [self.BASE_CONTAINER_NAME]

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set,
    ) -> None:
        if id(content) not in seen_oids and isinstance(content, k8s.V1EnvVar):
            seen_oids.add(id(content))
            self._do_render_template_fields(content, ("value", "name"), context, jinja_env, seen_oids)
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
            raise AirflowException("either application_file or template_spec should be passed")
        if "spark" not in template_body:
            template_body = {"spark": template_body}
        return template_body

    def create_job_name(self):
        name = (
            self.name or self.template_body.get("spark", {}).get("metadata", {}).get("name") or self.task_id
        )

        if self.random_name_suffix:
            updated_name = add_unique_suffix(name=name, max_len=MAX_LABEL_LEN)
        else:
            # truncation is required to maintain the same behavior as before
            updated_name = name[:MAX_LABEL_LEN]

        return self._set_name(updated_name)

    @staticmethod
    def _get_ti_pod_labels(context: Context | None = None, include_try_number: bool = True) -> dict[str, str]:
        """
        Generate labels for the pod to track the pod in case of Operator crash.

        :param include_try_number: add try number to labels
        :param context: task context provided by airflow DAG
        :return: dict.
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
            "spark_kubernetes_operator": "True",
        }

        map_index = ti.map_index
        if map_index is not None and map_index >= 0:
            labels["map_index"] = str(map_index)

        if include_try_number:
            labels.update(try_number=str(ti.try_number))

        # In the case of sub dags this is just useful
        # TODO: Remove this when the minimum version of Airflow is bumped to 3.0
        if getattr(context_dict["dag"], "parent_dag", False):
            labels["parent_dag_id"] = context_dict["dag"].parent_dag.dag_id
        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self.client)

    def _try_numbers_match(self, context, pod) -> bool:
        task_instance = context["task_instance"]
        task_context_labels = self._get_ti_pod_labels(context)
        pod_try_number = pod.metadata.labels.get(task_context_labels.get("try_number", ""), "")
        return str(task_instance.try_number) == str(pod_try_number)

    @property
    def template_body(self):
        """Templated body for CustomObjectLauncher."""
        return self.manage_template_specs()

    def find_spark_job(self, context, exclude_checked: bool = True):
        label_selector = (
            self._build_find_pod_label_selector(context, exclude_checked=exclude_checked)
            + ",spark-role=driver"
        )
        pod_list = self.client.list_namespaced_pod(self.namespace, label_selector=label_selector).items

        pod = None
        if len(pod_list) > 1:  # and self.reattach_on_restart:
            raise AirflowException(f"More than one pod running with labels: {label_selector}")
        if len(pod_list) == 1:
            pod = pod_list[0]
            self.log.info(
                "Found matching driver pod %s with labels %s", pod.metadata.name, pod.metadata.labels
            )
            self.log.info("`try_number` of task_instance: %s", context["ti"].try_number)
            self.log.info("`try_number` of pod: %s", pod.metadata.labels.get("try_number", "unknown"))
        return pod

    def process_pod_deletion(self, pod, *, reraise=True):
        if pod is not None:
            if self.delete_on_termination:
                pod_name = pod.metadata.name.replace("-driver", "")
                self.log.info("Deleting spark job: %s", pod_name)
                self.launcher.delete_spark_job(pod_name)
            else:
                self.log.info("skipping deleting spark job: %s", pod.metadata.name)

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster or self.template_body.get("kubernetes", {}).get("in_cluster", False),
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

    @cached_property
    def launcher(self) -> CustomObjectLauncher:
        return CustomObjectLauncher(
            name=self.name,
            namespace=self.namespace,
            kube_client=self.client,
            custom_obj_api=self.custom_obj_api,
            template_body=self.template_body,
        )

    def get_or_create_spark_crd(self, launcher: CustomObjectLauncher, context) -> k8s.V1Pod:
        if self.reattach_on_restart:
            driver_pod = self.find_spark_job(context)
            if driver_pod:
                return driver_pod

        driver_pod, spark_obj_spec = launcher.start_spark_job(
            image=self.image, code_path=self.code_path, startup_timeout=self.startup_timeout_seconds
        )
        return driver_pod

    def execute(self, context: Context):
        self.name = self.create_job_name()

        self._setup_spark_configuration(context)

        if self.deferrable:
            self.execute_async(context)

        return super().execute(context)

    def _setup_spark_configuration(self, context: Context):
        """Set up Spark-specific configuration including reattach logic."""
        import copy

        template_body = copy.deepcopy(self.template_body)

        if self.reattach_on_restart:
            task_context_labels = self._get_ti_pod_labels(context)

            existing_pod = self.find_spark_job(context)
            if existing_pod:
                self.log.info(
                    "Found existing Spark driver pod %s. Reattaching to it.", existing_pod.metadata.name
                )
                self.pod = existing_pod
                self.pod_request_obj = None
                if self.pod.metadata.name.endswith("-driver"):
                    self.name = self.pod.metadata.name.removesuffix("-driver")
                return

            if "spark" not in template_body:
                template_body["spark"] = {}
            if "spec" not in template_body["spark"]:
                template_body["spark"]["spec"] = {}

            spec_dict = template_body["spark"]["spec"]

            if "labels" not in spec_dict:
                spec_dict["labels"] = {}
            spec_dict["labels"].update(task_context_labels)

            for component in ["driver", "executor"]:
                if component not in spec_dict:
                    spec_dict[component] = {}

                if "labels" not in spec_dict[component]:
                    spec_dict[component]["labels"] = {}

                spec_dict[component]["labels"].update(task_context_labels)

        self.log.info("Creating sparkApplication.")
        self.launcher = CustomObjectLauncher(
            name=self.name,
            namespace=self.namespace,
            kube_client=self.client,
            custom_obj_api=self.custom_obj_api,
            template_body=template_body,
        )
        self.pod = self.get_or_create_spark_crd(self.launcher, context)
        self.pod_request_obj = self.launcher.pod_spec

    def find_pod(self, namespace: str, context: Context, *, exclude_checked: bool = True):
        """Override parent's find_pod to use our Spark-specific find_spark_job method."""
        return self.find_spark_job(context, exclude_checked=exclude_checked)

    def on_kill(self) -> None:
        self.log.debug("Deleting spark job for task %s", self.task_id)
        job_name = self.name
        if self.pod and self.pod.metadata and self.pod.metadata.name:
            if self.pod.metadata.name.endswith("-driver"):
                job_name = self.pod.metadata.name.removesuffix("-driver")
        self.launcher.delete_spark_job(spark_job_name=job_name)

    def patch_already_checked(self, pod: k8s.V1Pod, *, reraise=True):
        """Add an "already checked" annotation to ensure we don't reattach on retries."""
        pod.metadata.labels["already_checked"] = "True"
        body = PodGenerator.serialize_pod(pod)
        self.client.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)

    def dry_run(self) -> None:
        """Print out the spark job that would be created by this operator."""
        print(prune_dict(self.launcher.body, mode="strict"))
