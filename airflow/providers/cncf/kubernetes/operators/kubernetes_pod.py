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
"""Executes task in a Kubernetes POD"""
from __future__ import annotations

import json
import logging
import re
import warnings
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Sequence

from kubernetes.client import CoreV1Api, models as k8s

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.kubernetes import pod_generator
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.secret import Secret
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_affinity,
    convert_configmap,
    convert_env_vars,
    convert_image_pull_secrets,
    convert_pod_runtime_info_env,
    convert_port,
    convert_toleration,
    convert_volume,
    convert_volume_mount,
)
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.utils import xcom_sidecar  # type: ignore[attr-defined]
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodLaunchFailedException,
    PodManager,
    PodPhase,
    get_container_termination_message,
)
from airflow.settings import pod_mutation_hook
from airflow.utils import yaml
from airflow.utils.helpers import prune_dict, validate_key
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    import jinja2

    from airflow.utils.context import Context


def _task_id_to_pod_name(val: str) -> str:
    """
    Given a task_id, convert it to a pod name.
    Adds a 0 if start or end char is invalid.
    Replaces any other invalid char with `-`.

    :param val: non-empty string, presumed to be a task id
    :return valid kubernetes object name.
    """
    if not val:
        raise ValueError("_task_id_to_pod_name requires non-empty string.")
    val = val.lower()
    if not re.match(r"[a-z0-9]", val[0]):
        val = f"0{val}"
    if not re.match(r"[a-z0-9]", val[-1]):
        val = f"{val}0"
    val = re.sub(r"[^a-z0-9\-.]", "-", val)
    if len(val) > 253:
        raise ValueError(
            f"Pod name {val} is longer than 253 characters. "
            "See https://kubernetes.io/docs/concepts/overview/working-with-objects/names/."
        )
    return val


class PodReattachFailure(AirflowException):
    """When we expect to be able to find a pod but cannot."""


class KubernetesPodOperator(BaseOperator):
    """
    Execute a task in a Kubernetes Pod

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesPodOperator`

    .. note::
        If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
        and Airflow is not running in the same cluster, consider using
        :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`, which
        simplifies the authorization process.

    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param namespace: the namespace to run within kubernetes.
    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
        but fully qualified URLS will point to custom repositories. (templated)
    :param name: name of the pod in which the task will run, will be used (plus a random
        suffix if random_name_suffix is True) to generate a pod id (DNS-1123 subdomain,
        containing only [a-z0-9.-]).
    :param random_name_suffix: if True, will generate a random suffix.
    :param cmds: entrypoint of the container. (templated)
        The docker images's entrypoint is used if this is not provided.
    :param arguments: arguments of the entrypoint. (templated)
        The docker image's CMD is used if this is not provided.
    :param ports: ports for the launched pod.
    :param volume_mounts: volumeMounts for the launched pod.
    :param volumes: volumes for the launched pod. Includes ConfigMaps and PersistentVolumes.
    :param env_vars: Environment variables initialized in the container. (templated)
    :param env_from: (Optional) List of sources to populate environment variables in the container.
    :param secrets: Kubernetes secrets to inject in the container.
        They can be exposed as environment vars or files in a volume.
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :param reattach_on_restart: if the worker dies while the pod is running, reattach and monitor
        during the next try. If False, always create a new pod for each try.
    :param labels: labels to apply to the Pod. (templated)
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param image_pull_policy: Specify a policy to cache or always pull an image.
    :param annotations: non-identifying metadata you can attach to the Pod.
        Can be a large range of data, and can include characters
        that are not permitted by labels.
    :param container_resources: resources for the launched pod. (templated)
    :param affinity: affinity scheduling rules for the launched pod.
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    :param node_selector: A dict containing a group of scheduling rules.
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a
        comma separated list: secret_a,secret_b
    :param service_account_name: Name of the service account
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param hostnetwork: If True enable host networking on the pod.
    :param tolerations: A list of kubernetes tolerations.
    :param security_context: security options the pod should run with (PodSecurityContext).
    :param container_security_context: security options the container should run with.
    :param dnspolicy: dnspolicy for the pod.
    :param schedulername: Specify a schedulername for the pod
    :param full_pod_spec: The complete podSpec
    :param init_containers: init container for the launched Pod
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :param pod_template_file: path to pod template file (templated)
    :param priority_class_name: priority class name for the launched Pod
    :param pod_runtime_info_envs: (Optional) A list of environment variables,
        to be set in the container.
    :param termination_grace_period: Termination grace period if task killed in UI,
        defaults to kubernetes default
    :param configmaps: (Optional) A list of names of config maps from which it collects ConfigMaps
        to populate the environment variables with. The contents of the target
        ConfigMap's Data field will represent the key-value pairs as environment variables.
        Extends env_from.
    """

    BASE_CONTAINER_NAME = "base"
    POD_CHECKED_KEY = "already_checked"

    template_fields: Sequence[str] = (
        "image",
        "cmds",
        "arguments",
        "env_vars",
        "labels",
        "config_file",
        "pod_template_file",
        "namespace",
        "container_resources",
        "volumes",
        "volume_mounts",
    )
    template_fields_renderers = {"env_vars": "py"}

    def __init__(
        self,
        *,
        kubernetes_conn_id: str | None = None,  # 'kubernetes_default',
        namespace: str | None = None,
        image: str | None = None,
        name: str | None = None,
        random_name_suffix: bool | None = True,
        cmds: list[str] | None = None,
        arguments: list[str] | None = None,
        ports: list[k8s.V1ContainerPort] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        env_vars: list[k8s.V1EnvVar] | None = None,
        env_from: list[k8s.V1EnvFromSource] | None = None,
        secrets: list[Secret] | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        labels: dict | None = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        get_logs: bool = True,
        image_pull_policy: str | None = None,
        annotations: dict | None = None,
        container_resources: k8s.V1ResourceRequirements | None = None,
        affinity: k8s.V1Affinity | None = None,
        config_file: str | None = None,
        node_selector: dict | None = None,
        image_pull_secrets: list[k8s.V1LocalObjectReference] | None = None,
        service_account_name: str | None = None,
        is_delete_operator_pod: bool = True,
        hostnetwork: bool = False,
        tolerations: list[k8s.V1Toleration] | None = None,
        security_context: dict | None = None,
        container_security_context: dict | None = None,
        dnspolicy: str | None = None,
        schedulername: str | None = None,
        full_pod_spec: k8s.V1Pod | None = None,
        init_containers: list[k8s.V1Container] | None = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: str | None = None,
        priority_class_name: str | None = None,
        pod_runtime_info_envs: list[k8s.V1EnvVar] | None = None,
        termination_grace_period: int | None = None,
        configmaps: list[str] | None = None,
        **kwargs,
    ) -> None:
        # TODO: remove in provider 6.0.0 release. This is a mitigate step to advise users to switch to the
        # container_resources parameter.
        if isinstance(kwargs.get("resources"), k8s.V1ResourceRequirements):
            raise AirflowException(
                "Specifying resources for the launched pod with 'resources' is deprecated. "
                "Use 'container_resources' instead."
            )
        # TODO: remove in provider 6.0.0 release. This is a mitigate step to advise users to switch to the
        # node_selector parameter.
        if "node_selectors" in kwargs:
            raise ValueError(
                "Param `node_selectors` supplied. This param is no longer supported. "
                "Use `node_selector` instead."
            )
        super().__init__(**kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id
        self.do_xcom_push = do_xcom_push
        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.env_vars = convert_env_vars(env_vars) if env_vars else []
        if pod_runtime_info_envs:
            self.env_vars.extend([convert_pod_runtime_info_env(p) for p in pod_runtime_info_envs])
        self.env_from = env_from or []
        if configmaps:
            self.env_from.extend([convert_configmap(c) for c in configmaps])
        self.ports = [convert_port(p) for p in ports] if ports else []
        self.volume_mounts = [convert_volume_mount(v) for v in volume_mounts] if volume_mounts else []
        self.volumes = [convert_volume(volume) for volume in volumes] if volumes else []
        self.secrets = secrets or []
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.reattach_on_restart = reattach_on_restart
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        self.node_selector = node_selector or {}
        self.annotations = annotations or {}
        self.affinity = convert_affinity(affinity) if affinity else {}
        self.container_resources = container_resources
        self.config_file = config_file
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if image_pull_secrets else []
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = (
            [convert_toleration(toleration) for toleration in tolerations] if tolerations else []
        )
        self.security_context = security_context or {}
        self.container_security_context = container_security_context
        self.dnspolicy = dnspolicy
        self.schedulername = schedulername
        self.full_pod_spec = full_pod_spec
        self.init_containers = init_containers or []
        self.log_events_on_failure = log_events_on_failure
        self.priority_class_name = priority_class_name
        self.pod_template_file = pod_template_file
        self.name = self._set_name(name)
        self.random_name_suffix = random_name_suffix
        self.termination_grace_period = termination_grace_period
        self.pod_request_obj: k8s.V1Pod | None = None
        self.pod: k8s.V1Pod | None = None

    @cached_property
    def _incluster_namespace(self):
        from pathlib import Path

        path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        return path.exists() and path.read_text() or None

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set,
    ) -> None:
        if id(content) not in seen_oids:
            template_fields: tuple | None = None

            if isinstance(content, k8s.V1EnvVar):
                template_fields = ("value", "name")

            if isinstance(content, k8s.V1ResourceRequirements):
                template_fields = ("limits", "requests")

            if isinstance(content, k8s.V1Volume):
                template_fields = ("name", "persistent_volume_claim")

            if isinstance(content, k8s.V1VolumeMount):
                template_fields = ("name",)

            if isinstance(content, k8s.V1PersistentVolumeClaimVolumeSource):
                template_fields = ("claim_name",)

            if template_fields:
                seen_oids.add(id(content))
                self._do_render_template_fields(content, template_fields, context, jinja_env, seen_oids)
                return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    @staticmethod
    def _get_ti_pod_labels(context: Context | None = None, include_try_number: bool = True) -> dict[str, str]:
        """
        Generate labels for the pod to track the pod in case of Operator crash

        :param context: task context provided by airflow DAG
        :return: dict
        """
        if not context:
            return {}

        ti = context["ti"]
        run_id = context["run_id"]

        labels = {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": run_id,
            "kubernetes_pod_operator": "True",
        }

        map_index = ti.map_index
        if map_index >= 0:
            labels["map_index"] = map_index

        if include_try_number:
            labels.update(try_number=ti.try_number)
        # In the case of sub dags this is just useful
        if context["dag"].parent_dag:
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

    def get_hook(self):
        warnings.warn("get_hook is deprecated. Please use hook instead.", DeprecationWarning, stacklevel=2)
        return self.hook

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    def find_pod(self, namespace: str, context: Context, *, exclude_checked: bool = True) -> k8s.V1Pod | None:
        """Returns an already-running pod for this task instance if one exists."""
        label_selector = self._build_find_pod_label_selector(context, exclude_checked=exclude_checked)
        pod_list = self.client.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
        ).items

        pod = None
        num_pods = len(pod_list)
        if num_pods > 1:
            raise AirflowException(f"More than one pod running with labels {label_selector}")
        elif num_pods == 1:
            pod = pod_list[0]
            self.log.info("Found matching pod %s with labels %s", pod.metadata.name, pod.metadata.labels)
            self.log.info("`try_number` of task_instance: %s", context["ti"].try_number)
            self.log.info("`try_number` of pod: %s", pod.metadata.labels["try_number"])
        return pod

    def get_or_create_pod(self, pod_request_obj: k8s.V1Pod, context: Context) -> k8s.V1Pod:
        if self.reattach_on_restart:
            pod = self.find_pod(self.namespace or pod_request_obj.metadata.namespace, context=context)
            if pod:
                return pod
        self.log.debug("Starting pod:\n%s", yaml.safe_dump(pod_request_obj.to_dict()))
        self.pod_manager.create_pod(pod=pod_request_obj)
        return pod_request_obj

    def await_pod_start(self, pod: k8s.V1Pod):
        try:
            self.pod_manager.await_pod_start(pod=pod, startup_timeout=self.startup_timeout_seconds)
        except PodLaunchFailedException:
            if self.log_events_on_failure:
                self._read_pod_log_events(pod, reraise=True)
            raise

    def extract_xcom(self, pod: k8s.V1Pod):
        """Retrieves xcom value and kills xcom sidecar container"""
        result = self.pod_manager.extract_xcom(pod)
        if isinstance(result, str) and result.rstrip() == "__airflow_xcom_result_empty__":
            self.log.info("Result file is empty.")
            return None
        else:
            self.log.info("xcom result: \n%s", result)
            return json.loads(result)

    def execute(self, context: Context):
        remote_pod = None
        try:
            self.pod_request_obj = self.build_pod_request_obj(context)
            self.pod = self.get_or_create_pod(  # must set `self.pod` for `on_kill`
                pod_request_obj=self.pod_request_obj,
                context=context,
            )
            # get remote pod for use in cleanup methods
            remote_pod = self.find_pod(self.pod.metadata.namespace, context=context)
            self.await_pod_start(pod=self.pod)

            if self.get_logs:
                self.pod_manager.fetch_container_logs(
                    pod=self.pod,
                    container_name=self.BASE_CONTAINER_NAME,
                    follow=True,
                )
            else:
                self.pod_manager.await_container_completion(
                    pod=self.pod, container_name=self.BASE_CONTAINER_NAME
                )

            if self.do_xcom_push:
                self.pod_manager.await_xcom_sidecar_container_start(pod=self.pod)
                result = self.extract_xcom(pod=self.pod)
            remote_pod = self.pod_manager.await_pod_completion(self.pod)
        finally:
            self.cleanup(
                pod=self.pod or self.pod_request_obj,
                remote_pod=remote_pod,
            )
        ti = context["ti"]
        ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
        ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)
        if self.do_xcom_push:
            return result

    def _read_pod_log_events(self, pod, *, reraise=True):
        """Will fetch and emit events from pod"""
        with _optionally_suppress(reraise=reraise):
            for event in self.pod_manager.read_pod_events(pod).items:
                self.log.error("Pod Event: %s - %s", event.reason, event.message)

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, "status") else None
        if pod_phase != PodPhase.SUCCEEDED or not self.is_delete_operator_pod:
            self.patch_already_checked(remote_pod, reraise=False)
        if pod_phase != PodPhase.SUCCEEDED:
            if self.log_events_on_failure:
                self._read_pod_log_events(pod, reraise=False)
            self.process_pod_deletion(remote_pod, reraise=False)
            error_message = get_container_termination_message(remote_pod, self.BASE_CONTAINER_NAME)
            raise AirflowException(
                f"Pod {pod and pod.metadata.name} returned a failure:\n{error_message}\n"
                f"remote_pod: {remote_pod}"
            )
        else:
            self.process_pod_deletion(remote_pod, reraise=False)

    def process_pod_deletion(self, pod: k8s.V1Pod, *, reraise=True):
        with _optionally_suppress(reraise=reraise):
            if pod is not None:
                if self.is_delete_operator_pod:
                    self.log.info("Deleting pod: %s", pod.metadata.name)
                    self.pod_manager.delete_pod(pod)
                else:
                    self.log.info("skipping deleting pod: %s", pod.metadata.name)

    def _build_find_pod_label_selector(self, context: Context | None = None, *, exclude_checked=True) -> str:
        labels = self._get_ti_pod_labels(context, include_try_number=False)
        label_strings = [f"{label_id}={label}" for label_id, label in sorted(labels.items())]
        labels_value = ",".join(label_strings)
        if exclude_checked:
            labels_value += f",{self.POD_CHECKED_KEY}!=True"
        labels_value += ",!airflow-worker"
        return labels_value

    @staticmethod
    def _set_name(name: str | None) -> str | None:
        if name is not None:
            validate_key(name, max_length=220)
            return re.sub(r"[^a-z0-9-]+", "-", name.lower())
        return None

    def patch_already_checked(self, pod: k8s.V1Pod, *, reraise=True):
        """Add an "already checked" annotation to ensure we don't reattach on retries"""
        with _optionally_suppress(reraise=reraise):
            pod.metadata.labels[self.POD_CHECKED_KEY] = "True"
            body = PodGenerator.serialize_pod(pod)
            self.client.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)

    def on_kill(self) -> None:
        if self.pod:
            pod = self.pod
            kwargs = dict(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
            )
            if self.termination_grace_period is not None:
                kwargs.update(grace_period_seconds=self.termination_grace_period)
            self.client.delete_namespaced_pod(**kwargs)

    def build_pod_request_obj(self, context: Context | None = None) -> k8s.V1Pod:
        """
        Returns V1Pod object based on pod template file, full pod spec, and other operator parameters.

        The V1Pod attributes are derived (in order of precedence) from operator params, full pod spec, pod
        template file.
        """
        self.log.debug("Creating pod for KubernetesPodOperator task %s", self.task_id)
        if self.pod_template_file:
            self.log.debug("Pod template file found, will parse for base pod")
            pod_template = pod_generator.PodGenerator.deserialize_model_file(self.pod_template_file)
            if self.full_pod_spec:
                pod_template = PodGenerator.reconcile_pods(pod_template, self.full_pod_spec)
        elif self.full_pod_spec:
            pod_template = self.full_pod_spec
        else:
            pod_template = k8s.V1Pod(metadata=k8s.V1ObjectMeta())

        pod = k8s.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s.V1ObjectMeta(
                namespace=self.namespace,
                labels=self.labels,
                name=self.name,
                annotations=self.annotations,
            ),
            spec=k8s.V1PodSpec(
                node_selector=self.node_selector,
                affinity=self.affinity,
                tolerations=self.tolerations,
                init_containers=self.init_containers,
                containers=[
                    k8s.V1Container(
                        image=self.image,
                        name=self.BASE_CONTAINER_NAME,
                        command=self.cmds,
                        ports=self.ports,
                        image_pull_policy=self.image_pull_policy,
                        resources=self.container_resources,
                        volume_mounts=self.volume_mounts,
                        args=self.arguments,
                        env=self.env_vars,
                        env_from=self.env_from,
                        security_context=self.container_security_context,
                    )
                ],
                image_pull_secrets=self.image_pull_secrets,
                service_account_name=self.service_account_name,
                host_network=self.hostnetwork,
                security_context=self.security_context,
                dns_policy=self.dnspolicy,
                scheduler_name=self.schedulername,
                restart_policy="Never",
                priority_class_name=self.priority_class_name,
                volumes=self.volumes,
            ),
        )

        pod = PodGenerator.reconcile_pods(pod_template, pod)

        if not pod.metadata.name:
            pod.metadata.name = _task_id_to_pod_name(self.task_id)

        if self.random_name_suffix:
            pod.metadata.name = PodGenerator.make_unique_pod_id(pod.metadata.name)

        if not pod.metadata.namespace:
            # todo: replace with call to `hook.get_namespace` in 6.0, when it doesn't default to `default`.
            # if namespace not actually defined in hook, we want to check k8s if in cluster
            hook_namespace = self.hook._get_namespace()
            pod_namespace = self.namespace or hook_namespace or self._incluster_namespace or "default"
            pod.metadata.namespace = pod_namespace

        for secret in self.secrets:
            self.log.debug("Adding secret to task %s", self.task_id)
            pod = secret.attach_to_pod(pod)
        if self.do_xcom_push:
            self.log.debug("Adding xcom sidecar to task %s", self.task_id)
            pod = xcom_sidecar.add_xcom_sidecar(
                pod, sidecar_container_image=self.hook.get_xcom_sidecar_container_image()
            )

        labels = self._get_ti_pod_labels(context)
        self.log.info("Building pod %s with labels: %s", pod.metadata.name, labels)

        # Merge Pod Identifying labels with labels passed to operator
        pod.metadata.labels.update(labels)
        # Add Airflow Version to the label
        # And a label to identify that pod is launched by KubernetesPodOperator
        pod.metadata.labels.update(
            {
                "airflow_version": airflow_version.replace("+", "-"),
                "airflow_kpo_in_cluster": str(self.hook.is_in_cluster),
            }
        )
        pod_mutation_hook(pod)
        return pod

    def dry_run(self) -> None:
        """
        Prints out the pod definition that would be created by this operator.
        Does not include labels specific to the task instance (since there isn't
        one in a dry_run) and excludes all empty elements.
        """
        pod = self.build_pod_request_obj()
        print(yaml.dump(prune_dict(pod.to_dict(), mode="strict")))


class _optionally_suppress(AbstractContextManager):
    """
    Returns context manager that will swallow and log exceptions.

    By default swallows descendents of Exception, but you can provide other classes through
    the vararg ``exceptions``.

    Suppression behavior can be disabled with reraise=True.

    :meta private:
    """

    def __init__(self, *exceptions, reraise=False):
        self._exceptions = exceptions or (Exception,)
        self.reraise = reraise
        self.exception = None

    def __enter__(self):
        return self

    def __exit__(self, exctype, excinst, exctb):
        error = exctype is not None
        matching_error = error and issubclass(exctype, self._exceptions)
        if error and not matching_error:
            return False
        elif matching_error and self.reraise:
            return False
        elif matching_error:
            self.exception = excinst
            logger = logging.getLogger(__name__)
            logger.exception(excinst)
            return True
        else:
            return True
