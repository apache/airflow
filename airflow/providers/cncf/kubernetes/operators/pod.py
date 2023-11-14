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
"""Executes task in a Kubernetes POD."""

from __future__ import annotations

import json
import logging
import re
import secrets
import shlex
import string
import warnings
from collections.abc import Container
from contextlib import AbstractContextManager
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence

from kubernetes.client import CoreV1Api, V1Pod, models as k8s
from kubernetes.stream import stream
from slugify import slugify
from urllib3.exceptions import HTTPError

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes import pod_generator
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
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.cncf.kubernetes.utils import xcom_sidecar  # type: ignore[attr-defined]
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    EMPTY_XCOM_RESULT,
    OnFinishAction,
    PodLaunchFailedException,
    PodManager,
    PodOperatorHookProtocol,
    PodPhase,
    container_is_succeeded,
    get_container_termination_message,
)
from airflow.settings import pod_mutation_hook
from airflow.utils import yaml
from airflow.utils.helpers import prune_dict, validate_key
from airflow.utils.timezone import utcnow
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    import jinja2
    from typing_extensions import Literal

    from airflow.providers.cncf.kubernetes.secret import Secret
    from airflow.utils.context import Context

alphanum_lower = string.ascii_lowercase + string.digits

KUBE_CONFIG_ENV_VAR = "KUBECONFIG"


def _rand_str(num):
    """Generate random lowercase alphanumeric string of length num.

    TODO: when min airflow version >= 2.5, delete this function and import from kubernetes_helper_functions.

    :meta private:
    """
    return "".join(secrets.choice(alphanum_lower) for _ in range(num))


def _add_pod_suffix(*, pod_name, rand_len=8, max_len=253):
    """Add random string to pod name while staying under max len.

    TODO: when min airflow version >= 2.5, delete this function and import from kubernetes_helper_functions.

    :meta private:
    """
    suffix = "-" + _rand_str(rand_len)
    return pod_name[: max_len - len(suffix)].strip("-.") + suffix


def _create_pod_id(
    dag_id: str | None = None,
    task_id: str | None = None,
    *,
    max_length: int = 80,
    unique: bool = True,
) -> str:
    """
    Generate unique pod ID given a dag_id and / or task_id.

    TODO: when min airflow version >= 2.5, delete this function and import from kubernetes_helper_functions.

    :param dag_id: DAG ID
    :param task_id: Task ID
    :param max_length: max number of characters
    :param unique: whether a random string suffix should be added
    :return: A valid identifier for a kubernetes pod name
    """
    if not (dag_id or task_id):
        raise ValueError("Must supply either dag_id or task_id.")
    name = ""
    if dag_id:
        name += dag_id
    if task_id:
        if name:
            name += "-"
        name += task_id
    base_name = slugify(name, lowercase=True)[:max_length].strip(".-")
    if unique:
        return _add_pod_suffix(pod_name=base_name, max_len=max_length)
    else:
        return base_name


class PodReattachFailure(AirflowException):
    """When we expect to be able to find a pod but cannot."""


class KubernetesPodOperator(BaseOperator):
    """
    Execute a task in a Kubernetes Pod.

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
        Ignored when in_cluster is True. If None, current-context is used. (templated)
    :param reattach_on_restart: if the worker dies while the pod is running, reattach and monitor
        during the next try. If False, always create a new pod for each try.
    :param labels: labels to apply to the Pod. (templated)
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :param startup_check_interval_seconds: interval in seconds to check if the pod has already started
    :param get_logs: get the stdout of the base container as logs of the tasks.
    :param container_logs: list of containers whose logs will be published to stdout
        Takes a sequence of containers, a single container name or True. If True,
        all the containers logs are published. Works in conjunction with get_logs param.
        The default value is the base container.
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
    :param hostnetwork: If True enable host networking on the pod.
    :param host_aliases: A list of host aliases to apply to the containers in the pod.
    :param tolerations: A list of kubernetes tolerations.
    :param security_context: security options the pod should run with (PodSecurityContext).
    :param container_security_context: security options the container should run with.
    :param dnspolicy: dnspolicy for the pod.
    :param dns_config: dns configuration (ip addresses, searches, options) for the pod.
    :param hostname: hostname for the pod.
    :param subdomain: subdomain for the pod.
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
    :param skip_on_exit_code: If task exits with this exit code, leave the task
        in ``skipped`` state (default: None). If set to ``None``, any non-zero
        exit code will be treated as a failure.
    :param base_container_name: The name of the base container in the pod. This container's logs
        will appear as part of this task's logs if get_logs is True. Defaults to None. If None,
        will consult the class variable BASE_CONTAINER_NAME (which defaults to "base") for the base
        container name to use.
    :param deferrable: Run operator in the deferrable mode.
    :param poll_interval: Polling period in seconds to check for the status. Used only in deferrable mode.
    :param log_pod_spec_on_failure: Log the pod's specification if a failure occurs
    :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
        If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
        only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
        Deprecated - use `on_finish_action` instead.
    :param termination_message_policy: The termination message policy of the base container.
        Default value is "File"
    :param active_deadline_seconds: The active_deadline_seconds which matches to active_deadline_seconds
        in V1PodSpec.
    :param progress_callback: Callback function for receiving k8s container logs.
    """

    # This field can be overloaded at the instance level via base_container_name
    BASE_CONTAINER_NAME = "base"
    ISTIO_CONTAINER_NAME = "istio-proxy"
    KILL_ISTIO_PROXY_SUCCESS_MSG = "HTTP/1.1 200"
    POD_CHECKED_KEY = "already_checked"
    POST_TERMINATION_TIMEOUT = 120

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
        "cluster_context",
    )
    template_fields_renderers = {"env_vars": "py"}

    def __init__(
        self,
        *,
        kubernetes_conn_id: str | None = KubernetesHook.default_conn_name,
        namespace: str | None = None,
        image: str | None = None,
        name: str | None = None,
        random_name_suffix: bool = True,
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
        startup_check_interval_seconds: int = 1,
        get_logs: bool = True,
        container_logs: Iterable[str] | str | Literal[True] = BASE_CONTAINER_NAME,
        image_pull_policy: str | None = None,
        annotations: dict | None = None,
        container_resources: k8s.V1ResourceRequirements | None = None,
        affinity: k8s.V1Affinity | None = None,
        config_file: str | None = None,
        node_selector: dict | None = None,
        image_pull_secrets: list[k8s.V1LocalObjectReference] | None = None,
        service_account_name: str | None = None,
        hostnetwork: bool = False,
        host_aliases: list[k8s.V1HostAlias] | None = None,
        tolerations: list[k8s.V1Toleration] | None = None,
        security_context: k8s.V1PodSecurityContext | dict | None = None,
        container_security_context: k8s.V1SecurityContext | dict | None = None,
        dnspolicy: str | None = None,
        dns_config: k8s.V1PodDNSConfig | None = None,
        hostname: str | None = None,
        subdomain: str | None = None,
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
        skip_on_exit_code: int | Container[int] | None = None,
        base_container_name: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 2,
        log_pod_spec_on_failure: bool = True,
        on_finish_action: str = "delete_pod",
        is_delete_operator_pod: None | bool = None,
        termination_message_policy: str = "File",
        active_deadline_seconds: int | None = None,
        progress_callback: Callable[[str], None] | None = None,
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
        self.startup_check_interval_seconds = startup_check_interval_seconds
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
        self.container_logs = container_logs
        if self.container_logs == KubernetesPodOperator.BASE_CONTAINER_NAME:
            self.container_logs = base_container_name or self.BASE_CONTAINER_NAME
        self.image_pull_policy = image_pull_policy
        self.node_selector = node_selector or {}
        self.annotations = annotations or {}
        self.affinity = convert_affinity(affinity) if affinity else {}
        self.container_resources = container_resources
        self.config_file = config_file
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if image_pull_secrets else []
        self.service_account_name = service_account_name
        self.hostnetwork = hostnetwork
        self.host_aliases = host_aliases
        self.tolerations = (
            [convert_toleration(toleration) for toleration in tolerations] if tolerations else []
        )
        self.security_context = security_context or {}
        self.container_security_context = container_security_context
        self.dnspolicy = dnspolicy
        self.dns_config = dns_config
        self.hostname = hostname
        self.subdomain = subdomain
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
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code
            else []
        )
        self.base_container_name = base_container_name or self.BASE_CONTAINER_NAME
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.remote_pod: k8s.V1Pod | None = None
        self.log_pod_spec_on_failure = log_pod_spec_on_failure
        if is_delete_operator_pod is not None:
            warnings.warn(
                "`is_delete_operator_pod` parameter is deprecated, please use `on_finish_action`",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            self.on_finish_action = (
                OnFinishAction.DELETE_POD if is_delete_operator_pod else OnFinishAction.KEEP_POD
            )
            self.is_delete_operator_pod = is_delete_operator_pod
        else:
            self.on_finish_action = OnFinishAction(on_finish_action)
            self.is_delete_operator_pod = self.on_finish_action == OnFinishAction.DELETE_POD
        self.termination_message_policy = termination_message_policy
        self.active_deadline_seconds = active_deadline_seconds

        self._config_dict: dict | None = None  # TODO: remove it when removing convert_config_file_to_dict
        self._progress_callback = progress_callback

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
            template_fields: tuple | None

            if isinstance(content, k8s.V1EnvVar):
                template_fields = ("value", "name")
            elif isinstance(content, k8s.V1ResourceRequirements):
                template_fields = ("limits", "requests")
            elif isinstance(content, k8s.V1Volume):
                template_fields = ("name", "persistent_volume_claim")
            elif isinstance(content, k8s.V1VolumeMount):
                template_fields = ("name", "sub_path")
            elif isinstance(content, k8s.V1PersistentVolumeClaimVolumeSource):
                template_fields = ("claim_name",)
            else:
                template_fields = None

            if template_fields:
                seen_oids.add(id(content))
                self._do_render_template_fields(content, template_fields, context, jinja_env, seen_oids)
                return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    @staticmethod
    def _get_ti_pod_labels(context: Context | None = None, include_try_number: bool = True) -> dict[str, str]:
        """
        Generate labels for the pod to track the pod in case of Operator crash.

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
        return PodManager(kube_client=self.client, progress_callback=self._progress_callback)

    @cached_property
    def hook(self) -> PodOperatorHookProtocol:
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
        """Return an already-running pod for this task instance if one exists."""
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
            self.pod_manager.await_pod_start(
                pod=pod,
                startup_timeout=self.startup_timeout_seconds,
                startup_check_interval=self.startup_check_interval_seconds,
            )
        except PodLaunchFailedException:
            if self.log_events_on_failure:
                for event in self.pod_manager.read_pod_events(pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            raise

    def extract_xcom(self, pod: k8s.V1Pod):
        """Retrieve xcom value and kill xcom sidecar container."""
        result = self.pod_manager.extract_xcom(pod)
        if isinstance(result, str) and result.rstrip() == EMPTY_XCOM_RESULT:
            self.log.info("xcom result file is empty.")
            return None
        else:
            self.log.info("xcom result: \n%s", result)
            return json.loads(result)

    def execute(self, context: Context):
        """Based on the deferrable parameter runs the pod asynchronously or synchronously."""
        if self.deferrable:
            self.execute_async(context)
        else:
            return self.execute_sync(context)

    def execute_sync(self, context: Context):
        result = None
        try:
            self.pod_request_obj = self.build_pod_request_obj(context)
            self.pod = self.get_or_create_pod(  # must set `self.pod` for `on_kill`
                pod_request_obj=self.pod_request_obj,
                context=context,
            )
            # push to xcom now so that if there is an error we still have the values
            ti = context["ti"]
            ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
            ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)

            # get remote pod for use in cleanup methods
            self.remote_pod = self.find_pod(self.pod.metadata.namespace, context=context)
            self.await_pod_start(pod=self.pod)

            if self.get_logs:
                self.pod_manager.fetch_requested_container_logs(
                    pod=self.pod,
                    containers=self.container_logs,
                    follow_logs=True,
                )
            if not self.get_logs or (
                self.container_logs is not True and self.base_container_name not in self.container_logs
            ):
                self.pod_manager.await_container_completion(
                    pod=self.pod, container_name=self.base_container_name
                )

            if self.do_xcom_push:
                self.pod_manager.await_xcom_sidecar_container_start(pod=self.pod)
                result = self.extract_xcom(pod=self.pod)
            istio_enabled = self.is_istio_enabled(self.pod)
            self.remote_pod = self.pod_manager.await_pod_completion(
                self.pod, istio_enabled, self.base_container_name
            )
        finally:
            self.cleanup(
                pod=self.pod or self.pod_request_obj,
                remote_pod=self.remote_pod,
            )
        if self.do_xcom_push:
            return result

    def execute_async(self, context: Context):
        self.pod_request_obj = self.build_pod_request_obj(context)
        self.pod = self.get_or_create_pod(  # must set `self.pod` for `on_kill`
            pod_request_obj=self.pod_request_obj,
            context=context,
        )
        ti = context["ti"]
        ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
        ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)

        self.invoke_defer_method()

    def invoke_defer_method(self):
        """Redefine triggers which are being used in child classes."""
        trigger_start_time = utcnow()
        self.defer(
            trigger=KubernetesPodTrigger(
                pod_name=self.pod.metadata.name,
                pod_namespace=self.pod.metadata.namespace,
                trigger_start_time=trigger_start_time,
                kubernetes_conn_id=self.kubernetes_conn_id,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
                in_cluster=self.in_cluster,
                poll_interval=self.poll_interval,
                get_logs=self.get_logs,
                startup_timeout=self.startup_timeout_seconds,
                startup_check_interval=self.startup_check_interval_seconds,
                base_container_name=self.base_container_name,
                on_finish_action=self.on_finish_action.value,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict, **kwargs):
        pod = None
        try:
            pod = self.hook.get_pod(
                event["name"],
                event["namespace"],
            )
            if event["status"] in ("error", "failed", "timeout"):
                # fetch some logs when pod is failed
                if self.get_logs:
                    self.write_logs(pod)
                raise AirflowException(event["message"])
            elif event["status"] == "success":
                # fetch some logs when pod is executed successfully
                if self.get_logs:
                    self.write_logs(pod)

                if self.do_xcom_push:
                    xcom_sidecar_output = self.extract_xcom(pod=pod)
                    return xcom_sidecar_output
        finally:
            istio_enabled = self.is_istio_enabled(pod)
            # Skip await_pod_completion when the event is 'timeout' due to the pod can hang
            # on the ErrImagePull or ContainerCreating step and it will never complete
            if event["status"] != "timeout":
                pod = self.pod_manager.await_pod_completion(pod, istio_enabled, self.base_container_name)
            if pod is not None:
                self.post_complete_action(
                    pod=pod,
                    remote_pod=pod,
                )

    def write_logs(self, pod: k8s.V1Pod):
        try:
            logs = self.pod_manager.read_pod_logs(
                pod=pod,
                container_name=self.base_container_name,
                follow=False,
            )
            for raw_line in logs:
                line = raw_line.decode("utf-8", errors="backslashreplace").rstrip("\n")
                self.log.info("Container logs: %s", line)
        except HTTPError as e:
            self.log.warning(
                "Reading of logs interrupted with error %r; will retry. "
                "Set log level to DEBUG for traceback.",
                e,
            )

    def post_complete_action(self, *, pod, remote_pod, **kwargs):
        """Actions that must be done after operator finishes logic of the deferrable_execution."""
        self.cleanup(
            pod=pod,
            remote_pod=remote_pod,
        )

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        istio_enabled = self.is_istio_enabled(remote_pod)
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, "status") else None

        # if the pod fails or success, but we don't want to delete it
        if pod_phase != PodPhase.SUCCEEDED or self.on_finish_action == OnFinishAction.KEEP_POD:
            self.patch_already_checked(remote_pod, reraise=False)

        if (pod_phase != PodPhase.SUCCEEDED and not istio_enabled) or (
            istio_enabled and not container_is_succeeded(remote_pod, self.base_container_name)
        ):
            if self.log_events_on_failure:
                self._read_pod_events(pod, reraise=False)

            self.process_pod_deletion(remote_pod, reraise=False)

            error_message = get_container_termination_message(remote_pod, self.base_container_name)
            if self.skip_on_exit_code is not None:
                container_statuses = (
                    remote_pod.status.container_statuses if remote_pod and remote_pod.status else None
                ) or []
                base_container_status = next(
                    (x for x in container_statuses if x.name == self.base_container_name), None
                )
                exit_code = (
                    base_container_status.state.terminated.exit_code
                    if base_container_status
                    and base_container_status.state
                    and base_container_status.state.terminated
                    else None
                )
                if exit_code in self.skip_on_exit_code:
                    raise AirflowSkipException(
                        f"Pod {pod and pod.metadata.name} returned exit code "
                        f"{self.skip_on_exit_code}. Skipping."
                    )
            raise AirflowException(
                "\n".join(
                    filter(
                        None,
                        [
                            f"Pod {pod and pod.metadata.name} returned a failure.",
                            error_message if isinstance(error_message, str) else None,
                            f"remote_pod: {remote_pod}" if self.log_pod_spec_on_failure else None,
                        ],
                    )
                )
            )
        else:
            self.process_pod_deletion(remote_pod, reraise=False)

    def _read_pod_events(self, pod, *, reraise=True):
        """Will fetch and emit events from pod."""
        with _optionally_suppress(reraise=reraise):
            for event in self.pod_manager.read_pod_events(pod).items:
                self.log.error("Pod Event: %s - %s", event.reason, event.message)

    def is_istio_enabled(self, pod: V1Pod) -> bool:
        """Check if istio is enabled for the namespace of the pod by inspecting the namespace labels."""
        if not pod:
            return False

        remote_pod = self.pod_manager.read_pod(pod)

        return any(container.name == self.ISTIO_CONTAINER_NAME for container in remote_pod.spec.containers)

    def kill_istio_sidecar(self, pod: V1Pod) -> None:
        command = "/bin/sh -c 'curl -fsI -X POST http://localhost:15020/quitquitquit'"
        command_to_container = shlex.split(command)
        resp = stream(
            self.client.connect_get_namespaced_pod_exec,
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            container=self.ISTIO_CONTAINER_NAME,
            command=command_to_container,
            stderr=True,
            stdin=True,
            stdout=True,
            tty=False,
            _preload_content=False,
        )
        output = []
        while resp.is_open():
            if resp.peek_stdout():
                output.append(resp.read_stdout())

        resp.close()
        output_str = "".join(output)
        self.log.info("Output of curl command to kill istio: %s", output_str)
        resp.close()
        if self.KILL_ISTIO_PROXY_SUCCESS_MSG not in output_str:
            raise Exception("Error while deleting istio-proxy sidecar: %s", output_str)

    def process_pod_deletion(self, pod: k8s.V1Pod, *, reraise=True):
        with _optionally_suppress(reraise=reraise):
            if pod is not None:
                should_delete_pod = (
                    (self.on_finish_action == OnFinishAction.DELETE_POD)
                    or (
                        self.on_finish_action == OnFinishAction.DELETE_SUCCEEDED_POD
                        and pod.status.phase == PodPhase.SUCCEEDED
                    )
                    or (
                        self.on_finish_action == OnFinishAction.DELETE_SUCCEEDED_POD
                        and container_is_succeeded(pod, self.base_container_name)
                    )
                )
                if should_delete_pod:
                    self.log.info("Deleting pod: %s", pod.metadata.name)
                    self.pod_manager.delete_pod(pod)
                else:
                    self.log.info("Skipping deleting pod: %s", pod.metadata.name)

    def _build_find_pod_label_selector(self, context: Context | None = None, *, exclude_checked=True) -> str:
        labels = {
            **self.labels,
            **self._get_ti_pod_labels(context, include_try_number=False),
        }
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
        """Add an "already checked" annotation to ensure we don't reattach on retries."""
        with _optionally_suppress(reraise=reraise):
            self.client.patch_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body={"metadata": {"labels": {self.POD_CHECKED_KEY: "True"}}},
            )

    def on_kill(self) -> None:
        if self.pod:
            pod = self.pod
            kwargs = {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
            }
            if self.termination_grace_period is not None:
                kwargs.update(grace_period_seconds=self.termination_grace_period)
            self.client.delete_namespaced_pod(**kwargs)

    def build_pod_request_obj(self, context: Context | None = None) -> k8s.V1Pod:
        """
        Return V1Pod object based on pod template file, full pod spec, and other operator parameters.

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
                host_aliases=self.host_aliases,
                containers=[
                    k8s.V1Container(
                        image=self.image,
                        name=self.base_container_name,
                        command=self.cmds,
                        ports=self.ports,
                        image_pull_policy=self.image_pull_policy,
                        resources=self.container_resources,
                        volume_mounts=self.volume_mounts,
                        args=self.arguments,
                        env=self.env_vars,
                        env_from=self.env_from,
                        security_context=self.container_security_context,
                        termination_message_policy=self.termination_message_policy,
                    )
                ],
                image_pull_secrets=self.image_pull_secrets,
                service_account_name=self.service_account_name,
                host_network=self.hostnetwork,
                hostname=self.hostname,
                subdomain=self.subdomain,
                security_context=self.security_context,
                dns_policy=self.dnspolicy,
                dns_config=self.dns_config,
                scheduler_name=self.schedulername,
                restart_policy="Never",
                priority_class_name=self.priority_class_name,
                volumes=self.volumes,
                active_deadline_seconds=self.active_deadline_seconds,
            ),
        )

        pod = PodGenerator.reconcile_pods(pod_template, pod)

        if not pod.metadata.name:
            pod.metadata.name = _create_pod_id(
                task_id=self.task_id, unique=self.random_name_suffix, max_length=80
            )
        elif self.random_name_suffix:
            # user has supplied pod name, we're just adding suffix
            pod.metadata.name = _add_pod_suffix(pod_name=pod.metadata.name)

        if not pod.metadata.namespace:
            hook_namespace = self.hook.get_namespace()
            pod_namespace = self.namespace or hook_namespace or self._incluster_namespace or "default"
            pod.metadata.namespace = pod_namespace

        for secret in self.secrets:
            self.log.debug("Adding secret to task %s", self.task_id)
            pod = secret.attach_to_pod(pod)
        if self.do_xcom_push:
            self.log.debug("Adding xcom sidecar to task %s", self.task_id)
            pod = xcom_sidecar.add_xcom_sidecar(
                pod,
                sidecar_container_image=self.hook.get_xcom_sidecar_container_image(),
                sidecar_container_resources=self.hook.get_xcom_sidecar_container_resources(),
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
        Print out the pod definition that would be created by this operator.

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
        if (error and not matching_error) or (matching_error and self.reraise):
            return False
        elif matching_error:
            self.exception = excinst
            logger = logging.getLogger(__name__)
            logger.exception(excinst)
        return True
