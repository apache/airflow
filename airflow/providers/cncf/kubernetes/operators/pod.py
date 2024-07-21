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

import datetime
import json
import logging
import math
import os
import re
import shlex
import string
import warnings
from collections.abc import Container
from contextlib import AbstractContextManager
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence

import kubernetes
import tenacity
from deprecated import deprecated
from kubernetes.client import CoreV1Api, V1Pod, models as k8s
from kubernetes.client.exceptions import ApiException
from kubernetes.stream import stream
from urllib3.exceptions import HTTPError

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowProviderDeprecationWarning,
    AirflowSkipException,
    TaskDeferred,
)
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_affinity,
    convert_configmap,
    convert_env_vars,
    convert_env_vars_or_raise_error,
    convert_image_pull_secrets,
    convert_pod_runtime_info_env,
    convert_port,
    convert_toleration,
    convert_volume,
    convert_volume_mount,
)
from airflow.providers.cncf.kubernetes.callbacks import ExecutionMode, KubernetesPodOperatorCallback
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    POD_NAME_MAX_LENGTH,
    add_unique_suffix,
    create_unique_id,
)
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.cncf.kubernetes.utils import xcom_sidecar  # type: ignore[attr-defined]
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    EMPTY_XCOM_RESULT,
    OnFinishAction,
    PodLaunchFailedException,
    PodManager,
    PodNotFoundException,
    PodOperatorHookProtocol,
    PodPhase,
    check_exception_is_kubernetes_api_unauthorized,
    container_is_succeeded,
    get_container_termination_message,
)
from airflow.settings import pod_mutation_hook
from airflow.utils import yaml
from airflow.utils.helpers import prune_dict, validate_key
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    import jinja2
    from pendulum import DateTime
    from typing_extensions import Literal

    from airflow.providers.cncf.kubernetes.secret import Secret
    from airflow.utils.context import Context

alphanum_lower = string.ascii_lowercase + string.digits

KUBE_CONFIG_ENV_VAR = "KUBECONFIG"


class PodEventType(Enum):
    """Type of Events emitted by kubernetes pod."""

    WARNING = "Warning"
    NORMAL = "Normal"


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
        that are not permitted by labels. (templated)
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
    :param pod_template_dict: pod template dictionary (templated)
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
    :param active_deadline_seconds: The active_deadline_seconds which translates to active_deadline_seconds
        in V1PodSpec.
    :param callbacks: KubernetesPodOperatorCallback instance contains the callbacks methods on different step
        of KubernetesPodOperator.
    :param progress_callback: Callback function for receiving k8s container logs.
        `progress_callback` is deprecated, please use :param `callbacks` instead.
    :param logging_interval: max time in seconds that task should be in deferred state before
        resuming to fetch the latest logs. If ``None``, then the task will remain in deferred state until pod
        is done, and no logs will be visible until that time.
    """

    # !!! Changes in KubernetesPodOperator's arguments should be also reflected in !!!
    #  - airflow/decorators/__init__.pyi  (by a separate PR)

    # This field can be overloaded at the instance level via base_container_name
    BASE_CONTAINER_NAME = "base"
    ISTIO_CONTAINER_NAME = "istio-proxy"
    KILL_ISTIO_PROXY_SUCCESS_MSG = "HTTP/1.1 200"
    POD_CHECKED_KEY = "already_checked"
    POST_TERMINATION_TIMEOUT = 120

    template_fields: Sequence[str] = (
        "image",
        "cmds",
        "annotations",
        "arguments",
        "env_vars",
        "labels",
        "config_file",
        "pod_template_file",
        "pod_template_dict",
        "namespace",
        "container_resources",
        "volumes",
        "volume_mounts",
        "cluster_context",
        "env_from",
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
        env_vars: list[k8s.V1EnvVar] | dict[str, str] | None = None,
        env_from: list[k8s.V1EnvFromSource] | None = None,
        secrets: list[Secret] | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        labels: dict | None = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        startup_check_interval_seconds: int = 5,
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
        pod_template_dict: dict | None = None,
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
        callbacks: type[KubernetesPodOperatorCallback] | None = None,
        progress_callback: Callable[[str], None] | None = None,
        logging_interval: int | None = None,
        **kwargs,
    ) -> None:
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
        env_vars = convert_env_vars(env_vars) if env_vars else []
        self.env_vars = env_vars
        pod_runtime_info_envs = (
            [convert_pod_runtime_info_env(p) for p in pod_runtime_info_envs] if pod_runtime_info_envs else []
        )
        self.pod_runtime_info_envs = pod_runtime_info_envs
        self.env_from = env_from or []
        if configmaps:
            self.env_from.extend([convert_configmap(c) for c in configmaps])
        self.ports = [convert_port(p) for p in ports] if ports else []
        volume_mounts = [convert_volume_mount(v) for v in volume_mounts] if volume_mounts else []
        self.volume_mounts = volume_mounts
        volumes = [convert_volume(volume) for volume in volumes] if volumes else []
        self.volumes = volumes
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
        self.pod_template_dict = pod_template_dict
        self.name = self._set_name(name)
        self.random_name_suffix = random_name_suffix
        self.termination_grace_period = termination_grace_period
        self.pod_request_obj: k8s.V1Pod | None = None
        self.pod: k8s.V1Pod | None = None
        self.skip_on_exit_code = (
            skip_on_exit_code
            if isinstance(skip_on_exit_code, Container)
            else [skip_on_exit_code]
            if skip_on_exit_code is not None
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
        self.logging_interval = logging_interval

        self._config_dict: dict | None = None  # TODO: remove it when removing convert_config_file_to_dict
        self._progress_callback = progress_callback
        self.callbacks = callbacks
        self._killed: bool = False

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
                template_fields = ("name", "persistent_volume_claim", "config_map")
            elif isinstance(content, k8s.V1VolumeMount):
                template_fields = ("name", "sub_path")
            elif isinstance(content, k8s.V1PersistentVolumeClaimVolumeSource):
                template_fields = ("claim_name",)
            elif isinstance(content, k8s.V1ConfigMapVolumeSource):
                template_fields = ("name",)
            elif isinstance(content, k8s.V1EnvFromSource):
                template_fields = ("config_map_ref",)
            elif isinstance(content, k8s.V1ConfigMapEnvSource):
                template_fields = ("name",)
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

        :param context: task context provided by airflow DAG.
        :param include_try_number: if set to True will add the try number
            from the task context to the pod labels.
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
        # TODO: Remove this when the minimum version of Airflow is bumped to 3.0
        if getattr(context["dag"], "parent_dag", False):
            labels["parent_dag_id"] = context["dag"].parent_dag.dag_id  # type: ignore[attr-defined]

        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(
            kube_client=self.client, callbacks=self.callbacks, progress_callback=self._progress_callback
        )

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
        client = self.hook.core_v1_client
        if self.callbacks:
            self.callbacks.on_sync_client_creation(client=client)
        return client

    def find_pod(self, namespace: str, context: Context, *, exclude_checked: bool = True) -> k8s.V1Pod | None:
        """Return an already-running pod for this task instance if one exists."""
        label_selector = self._build_find_pod_label_selector(context, exclude_checked=exclude_checked)
        pod_list = self.client.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
        ).items

        pod = None
        num_pods = len(pod_list)

        if num_pods == 1:
            pod = pod_list[0]
            self.log_matching_pod(pod=pod, context=context)
        elif num_pods > 1:
            if self.reattach_on_restart:
                raise AirflowException(f"More than one pod running with labels {label_selector}")
            self.log.warning("Found more than one pod running with labels %s, resolving ...", label_selector)
            pod = self.process_duplicate_label_pods(pod_list)
            self.log_matching_pod(pod=pod, context=context)

        return pod

    def log_matching_pod(self, pod: k8s.V1Pod, context: Context) -> None:
        self.log.info("Found matching pod %s with labels %s", pod.metadata.name, pod.metadata.labels)
        self.log.info("`try_number` of task_instance: %s", context["ti"].try_number)
        self.log.info("`try_number` of pod: %s", pod.metadata.labels["try_number"])

    def get_or_create_pod(self, pod_request_obj: k8s.V1Pod, context: Context) -> k8s.V1Pod:
        if self.reattach_on_restart:
            pod = self.find_pod(self.namespace or pod_request_obj.metadata.namespace, context=context)
            if pod:
                return pod
        self.log.debug("Starting pod:\n%s", yaml.safe_dump(pod_request_obj.to_dict()))
        self.pod_manager.create_pod(pod=pod_request_obj)
        return pod_request_obj

    def await_pod_start(self, pod: k8s.V1Pod) -> None:
        try:
            self.pod_manager.await_pod_start(
                pod=pod,
                startup_timeout=self.startup_timeout_seconds,
                startup_check_interval=self.startup_check_interval_seconds,
            )
        except PodLaunchFailedException:
            if self.log_events_on_failure:
                self._read_pod_events(pod, reraise=False)
            raise

    def extract_xcom(self, pod: k8s.V1Pod) -> dict[Any, Any] | None:
        """Retrieve xcom value and kill xcom sidecar container."""
        result = self.pod_manager.extract_xcom(pod)
        if isinstance(result, str) and result.rstrip() == EMPTY_XCOM_RESULT:
            self.log.info("xcom result file is empty.")
            return None

        self.log.info("xcom result: \n%s", result)
        return json.loads(result)

    def execute(self, context: Context):
        """Based on the deferrable parameter runs the pod asynchronously or synchronously."""
        if not self.deferrable:
            return self.execute_sync(context)

        self.execute_async(context)

    def execute_sync(self, context: Context):
        result = None
        try:
            if self.pod_request_obj is None:
                self.pod_request_obj = self.build_pod_request_obj(context)
            if self.pod is None:
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
            if self.callbacks:
                self.callbacks.on_pod_creation(
                    pod=self.remote_pod, client=self.client, mode=ExecutionMode.SYNC
                )
            self.await_pod_start(pod=self.pod)
            if self.callbacks:
                self.callbacks.on_pod_starting(
                    pod=self.find_pod(self.pod.metadata.namespace, context=context),
                    client=self.client,
                    mode=ExecutionMode.SYNC,
                )

            self.await_pod_completion(pod=self.pod)
            if self.callbacks:
                self.callbacks.on_pod_completion(
                    pod=self.find_pod(self.pod.metadata.namespace, context=context),
                    client=self.client,
                    mode=ExecutionMode.SYNC,
                )

            if self.do_xcom_push:
                self.pod_manager.await_xcom_sidecar_container_start(
                    pod=self.pod, startup_timeout=self.startup_timeout_seconds
                )
                result = self.extract_xcom(pod=self.pod)
            istio_enabled = self.is_istio_enabled(self.pod)
            self.remote_pod = self.pod_manager.await_pod_completion(
                self.pod, istio_enabled, self.base_container_name
            )
        finally:
            pod_to_clean = self.pod or self.pod_request_obj
            self.cleanup(
                pod=pod_to_clean,
                remote_pod=self.remote_pod,
            )
            if self.callbacks:
                self.callbacks.on_pod_cleanup(pod=pod_to_clean, client=self.client, mode=ExecutionMode.SYNC)

        if self.do_xcom_push:
            return result

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(max=15),
        retry=tenacity.retry_if_exception(lambda exc: check_exception_is_kubernetes_api_unauthorized(exc)),
        reraise=True,
    )
    def await_pod_completion(self, pod: k8s.V1Pod):
        try:
            if self.get_logs:
                self.pod_manager.fetch_requested_container_logs(
                    pod=pod,
                    containers=self.container_logs,
                    follow_logs=True,
                )
            if not self.get_logs or (
                self.container_logs is not True and self.base_container_name not in self.container_logs
            ):
                self.pod_manager.await_container_completion(pod=pod, container_name=self.base_container_name)
        except kubernetes.client.exceptions.ApiException as exc:
            if exc.status and str(exc.status) == "401":
                self.log.warning(
                    "Failed to check container status due to permission error. Refreshing credentials and retrying."
                )
                self._refresh_cached_properties()
            raise exc

    def _refresh_cached_properties(self):
        del self.hook
        del self.client
        del self.pod_manager

    def execute_async(self, context: Context) -> None:
        self.pod_request_obj = self.build_pod_request_obj(context)
        self.pod = self.get_or_create_pod(  # must set `self.pod` for `on_kill`
            pod_request_obj=self.pod_request_obj,
            context=context,
        )
        if self.callbacks:
            self.callbacks.on_pod_creation(
                pod=self.find_pod(self.pod.metadata.namespace, context=context),
                client=self.client,
                mode=ExecutionMode.SYNC,
            )
        ti = context["ti"]
        ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
        ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)

        self.convert_config_file_to_dict()
        self.invoke_defer_method()

    def convert_config_file_to_dict(self):
        """Convert passed config_file to dict representation."""
        config_file = self.config_file if self.config_file else os.environ.get(KUBE_CONFIG_ENV_VAR)
        if config_file:
            with open(config_file) as f:
                self._config_dict = yaml.safe_load(f)
        else:
            self._config_dict = None

    def invoke_defer_method(self, last_log_time: DateTime | None = None) -> None:
        """Redefine triggers which are being used in child classes."""
        trigger_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        self.defer(
            trigger=KubernetesPodTrigger(
                pod_name=self.pod.metadata.name,  # type: ignore[union-attr]
                pod_namespace=self.pod.metadata.namespace,  # type: ignore[union-attr]
                trigger_start_time=trigger_start_time,
                kubernetes_conn_id=self.kubernetes_conn_id,
                cluster_context=self.cluster_context,
                config_dict=self._config_dict,
                in_cluster=self.in_cluster,
                poll_interval=self.poll_interval,
                get_logs=self.get_logs,
                startup_timeout=self.startup_timeout_seconds,
                startup_check_interval=self.startup_check_interval_seconds,
                base_container_name=self.base_container_name,
                on_finish_action=self.on_finish_action.value,
                last_log_time=last_log_time,
                logging_interval=self.logging_interval,
            ),
            method_name="trigger_reentry",
        )

    def trigger_reentry(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Point of re-entry from trigger.

        If ``logging_interval`` is None, then at this point, the pod should be done, and we'll just fetch
        the logs and exit.

        If ``logging_interval`` is not None, it could be that the pod is still running, and we'll just
        grab the latest logs and defer back to the trigger again.
        """
        self.pod = None
        try:
            pod_name = event["name"]
            pod_namespace = event["namespace"]

            self.pod = self.hook.get_pod(pod_name, pod_namespace)

            if not self.pod:
                raise PodNotFoundException("Could not find pod after resuming from deferral")

            if self.callbacks and event["status"] != "running":
                self.callbacks.on_operator_resuming(
                    pod=self.pod, event=event, client=self.client, mode=ExecutionMode.SYNC
                )

            follow = self.logging_interval is None
            last_log_time = event.get("last_log_time")

            if event["status"] in ("error", "failed", "timeout"):
                # fetch some logs when pod is failed
                if self.get_logs:
                    self._write_logs(self.pod, follow=follow, since_time=last_log_time)

                if self.do_xcom_push:
                    _ = self.extract_xcom(pod=self.pod)

                message = event.get("stack_trace", event["message"])
                raise AirflowException(message)

            elif event["status"] == "running":
                if self.get_logs:
                    self.log.info("Resuming logs read from time %r", last_log_time)

                    pod_log_status = self.pod_manager.fetch_container_logs(
                        pod=self.pod,
                        container_name=self.BASE_CONTAINER_NAME,
                        follow=follow,
                        since_time=last_log_time,
                    )

                    if pod_log_status.running:
                        self.log.info("Container still running; deferring again.")
                        self.invoke_defer_method(pod_log_status.last_log_time)
                else:
                    self.invoke_defer_method()

            elif event["status"] == "success":
                # fetch some logs when pod is executed successfully
                if self.get_logs:
                    self._write_logs(self.pod, follow=follow, since_time=last_log_time)

                if self.do_xcom_push:
                    xcom_sidecar_output = self.extract_xcom(pod=self.pod)
                    return xcom_sidecar_output
                return
        except TaskDeferred:
            raise
        finally:
            self._clean(event)

    def _clean(self, event: dict[str, Any]) -> None:
        if event["status"] == "running":
            return
        istio_enabled = self.is_istio_enabled(self.pod)
        # Skip await_pod_completion when the event is 'timeout' due to the pod can hang
        # on the ErrImagePull or ContainerCreating step and it will never complete
        if event["status"] != "timeout":
            try:
                self.pod = self.pod_manager.await_pod_completion(
                    self.pod, istio_enabled, self.base_container_name
                )
            except ApiException as e:
                if e.status == 404:
                    self.pod = None
                    self.log.warning(
                        "Pod not found while waiting for completion. The last status was %r", event["status"]
                    )
                else:
                    raise e
        if self.pod is not None:
            self.post_complete_action(
                pod=self.pod,
                remote_pod=self.pod,
            )

    def _write_logs(self, pod: k8s.V1Pod, follow: bool = False, since_time: DateTime | None = None) -> None:
        try:
            since_seconds = (
                math.ceil((datetime.datetime.now(tz=datetime.timezone.utc) - since_time).total_seconds())
                if since_time
                else None
            )
            logs = self.client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container=self.base_container_name,
                follow=follow,
                timestamps=False,
                since_seconds=since_seconds,
                _preload_content=False,
            )
            for raw_line in logs:
                line = raw_line.decode("utf-8", errors="backslashreplace").rstrip("\n")
                if line:
                    self.log.info("[%s] logs: %s", self.base_container_name, line)
        except (HTTPError, ApiException) as e:
            self.log.warning(
                "Reading of logs interrupted with error %r; will retry. "
                "Set log level to DEBUG for traceback.",
                e if not isinstance(e, ApiException) else e.reason,
            )

    def post_complete_action(self, *, pod, remote_pod, **kwargs) -> None:
        """Actions that must be done after operator finishes logic of the deferrable_execution."""
        self.cleanup(
            pod=pod,
            remote_pod=remote_pod,
        )
        if self.callbacks:
            self.callbacks.on_pod_cleanup(pod=pod, client=self.client, mode=ExecutionMode.SYNC)

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        # Skip cleaning the pod in the following scenarios.
        # 1. If a task got marked as failed, "on_kill" method would be called and the pod will be cleaned up
        # there. Cleaning it up again will raise an exception (which might cause retry).
        # 2. remote pod is null (ex: pod creation failed)
        if self._killed or not remote_pod:
            return

        istio_enabled = self.is_istio_enabled(remote_pod)
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, "status") else None

        # if the pod fails or success, but we don't want to delete it
        if pod_phase != PodPhase.SUCCEEDED or self.on_finish_action == OnFinishAction.KEEP_POD:
            self.patch_already_checked(remote_pod, reraise=False)

        failed = (pod_phase != PodPhase.SUCCEEDED and not istio_enabled) or (
            istio_enabled and not container_is_succeeded(remote_pod, self.base_container_name)
        )

        if failed:
            if self.log_events_on_failure:
                self._read_pod_events(pod, reraise=False)

        self.process_pod_deletion(remote_pod, reraise=False)

        if self.skip_on_exit_code:
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
                    f"Pod {pod and pod.metadata.name} returned exit code {exit_code}. Skipping."
                )

        if failed:
            error_message = get_container_termination_message(remote_pod, self.base_container_name)
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

    def _read_pod_events(self, pod, *, reraise=True) -> None:
        """Will fetch and emit events from pod."""
        with _optionally_suppress(reraise=reraise):
            for event in self.pod_manager.read_pod_events(pod).items:
                if event.type == PodEventType.NORMAL.value:
                    self.log.info("Pod Event: %s - %s", event.reason, event.message)
                else:
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
            raise AirflowException("Error while deleting istio-proxy sidecar: %s", output_str)

    def process_pod_deletion(self, pod: k8s.V1Pod, *, reraise=True):
        with _optionally_suppress(reraise=reraise):
            if pod is not None:
                should_delete_pod = (self.on_finish_action == OnFinishAction.DELETE_POD) or (
                    self.on_finish_action == OnFinishAction.DELETE_SUCCEEDED_POD
                    and (
                        pod.status.phase == PodPhase.SUCCEEDED
                        or container_is_succeeded(pod, self.base_container_name)
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
            labels_value = f"{labels_value},{self.POD_CHECKED_KEY}!=True"
        labels_value = f"{labels_value},!airflow-worker"
        return labels_value

    @staticmethod
    def _set_name(name: str | None) -> str | None:
        if name is not None:
            validate_key(name, max_length=220)
            return re.sub(r"[^a-z0-9-]+", "-", name.lower())
        return None

    def patch_already_checked(self, pod: k8s.V1Pod, *, reraise=True):
        """Add an "already checked" label to ensure we don't reattach on retries."""
        with _optionally_suppress(reraise=reraise):
            self.client.patch_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body={"metadata": {"labels": {self.POD_CHECKED_KEY: "True"}}},
            )

    def on_kill(self) -> None:
        self._killed = True
        if self.pod:
            pod = self.pod
            kwargs = {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
            }
            if self.termination_grace_period is not None:
                kwargs.update(grace_period_seconds=self.termination_grace_period)

            try:
                self.client.delete_namespaced_pod(**kwargs)
            except kubernetes.client.exceptions.ApiException:
                self.log.exception("Unable to delete pod %s", self.pod.metadata.name)

    def build_pod_request_obj(self, context: Context | None = None) -> k8s.V1Pod:
        """
        Return V1Pod object based on pod template file, full pod spec, and other operator parameters.

        The V1Pod attributes are derived (in order of precedence) from operator params, full pod spec, pod
        template file.
        """
        self.log.debug("Creating pod for KubernetesPodOperator task %s", self.task_id)

        self.env_vars = convert_env_vars_or_raise_error(self.env_vars) if self.env_vars else []
        if self.pod_runtime_info_envs:
            self.env_vars.extend(self.pod_runtime_info_envs)

        if self.pod_template_file:
            self.log.debug("Pod template file found, will parse for base pod")
            pod_template = pod_generator.PodGenerator.deserialize_model_file(self.pod_template_file)
            if self.full_pod_spec:
                pod_template = PodGenerator.reconcile_pods(pod_template, self.full_pod_spec)
        elif self.pod_template_dict:
            self.log.debug("Pod template dict found, will parse for base pod")
            pod_template = pod_generator.PodGenerator.deserialize_model_dict(self.pod_template_dict)
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
            pod.metadata.name = create_unique_id(
                task_id=self.task_id, unique=self.random_name_suffix, max_length=POD_NAME_MAX_LENGTH
            )
        elif self.random_name_suffix:
            # user has supplied pod name, we're just adding suffix
            pod.metadata.name = add_unique_suffix(name=pod.metadata.name)

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

    @deprecated(reason="use `trigger_reentry` instead.", category=AirflowProviderDeprecationWarning)
    def execute_complete(self, context: Context, event: dict, **kwargs):
        return self.trigger_reentry(context=context, event=event)

    def process_duplicate_label_pods(self, pod_list: list[k8s.V1Pod]) -> k8s.V1Pod:
        """
        Patch or delete the existing pod with duplicate labels.

        This is to handle an edge case that can happen only if reattach_on_restart
        flag is False, and the previous run attempt has failed because the task
        process has been killed externally by the cluster or another process.

        If the task process is killed externally, it breaks the code execution and
        immediately exists the task. As a result the pod created in the previous attempt
        will not be properly deleted or patched by cleanup() method.

        Return the newly created pod to be used for the next run attempt.
        """
        new_pod = pod_list.pop(self._get_most_recent_pod_index(pod_list))
        old_pod = pod_list[0]
        self.patch_already_checked(old_pod, reraise=False)
        if self.on_finish_action == OnFinishAction.DELETE_POD:
            self.process_pod_deletion(old_pod)
        return new_pod

    @staticmethod
    def _get_most_recent_pod_index(pod_list: list[k8s.V1Pod]) -> int:
        """Loop through a list of V1Pod objects and get the index of the most recent one."""
        pod_start_times: list[datetime.datetime] = [
            pod.to_dict().get("status").get("start_time") for pod in pod_list
        ]
        most_recent_start_time = max(pod_start_times)
        return pod_start_times.index(most_recent_start_time)


class _optionally_suppress(AbstractContextManager):
    """
    Returns context manager that will swallow and log exceptions.

    By default swallows descendents of Exception, but you can provide other classes through
    the vararg ``exceptions``.

    Suppression behavior can be disabled with reraise=True.

    :meta private:
    """

    def __init__(self, *exceptions, reraise: bool = False) -> None:
        self._exceptions = exceptions or (Exception,)
        self.reraise = reraise
        self.exception = None

    def __enter__(self):
        return self

    def __exit__(self, exctype, excinst, exctb) -> bool:
        error = exctype is not None
        matching_error = error and issubclass(exctype, self._exceptions)
        if (error and not matching_error) or (matching_error and self.reraise):
            return False
        elif matching_error:
            self.exception = excinst
            logger = logging.getLogger(__name__)
            logger.exception(excinst)
        return True
