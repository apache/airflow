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
import json
import logging
import re
import warnings
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from kubernetes.client import CoreV1Api, models as k8s

from airflow.compat.functools import cached_property
from airflow.configuration import conf
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
    convert_resources,
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
    :param container_resources: resources for the launched pod.
    :param affinity: affinity scheduling rules for the launched pod.
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    :param node_selectors: (Deprecated) A dict containing a group of scheduling rules.
        Please use node_selector instead.
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
    :param: kubernetes_conn_id: To retrieve credentials for your k8s cluster from an Airflow connection
    """

    BASE_CONTAINER_NAME = 'base'
    POD_CHECKED_KEY = 'already_checked'

    template_fields: Sequence[str] = (
        'image',
        'cmds',
        'arguments',
        'env_vars',
        'labels',
        'config_file',
        'pod_template_file',
        'namespace',
    )

    def __init__(
        self,
        *,
        kubernetes_conn_id: Optional[str] = None,  # 'kubernetes_default',
        namespace: Optional[str] = None,
        image: Optional[str] = None,
        name: Optional[str] = None,
        random_name_suffix: Optional[bool] = True,
        cmds: Optional[List[str]] = None,
        arguments: Optional[List[str]] = None,
        ports: Optional[List[k8s.V1ContainerPort]] = None,
        volume_mounts: Optional[List[k8s.V1VolumeMount]] = None,
        volumes: Optional[List[k8s.V1Volume]] = None,
        env_vars: Optional[List[k8s.V1EnvVar]] = None,
        env_from: Optional[List[k8s.V1EnvFromSource]] = None,
        secrets: Optional[List[Secret]] = None,
        in_cluster: Optional[bool] = None,
        cluster_context: Optional[str] = None,
        labels: Optional[Dict] = None,
        reattach_on_restart: bool = True,
        startup_timeout_seconds: int = 120,
        get_logs: bool = True,
        image_pull_policy: Optional[str] = None,
        annotations: Optional[Dict] = None,
        container_resources: Optional[k8s.V1ResourceRequirements] = None,
        affinity: Optional[k8s.V1Affinity] = None,
        config_file: Optional[str] = None,
        node_selectors: Optional[dict] = None,
        node_selector: Optional[dict] = None,
        image_pull_secrets: Optional[List[k8s.V1LocalObjectReference]] = None,
        service_account_name: Optional[str] = None,
        is_delete_operator_pod: bool = True,
        hostnetwork: bool = False,
        tolerations: Optional[List[k8s.V1Toleration]] = None,
        security_context: Optional[Dict] = None,
        dnspolicy: Optional[str] = None,
        schedulername: Optional[str] = None,
        full_pod_spec: Optional[k8s.V1Pod] = None,
        init_containers: Optional[List[k8s.V1Container]] = None,
        log_events_on_failure: bool = False,
        do_xcom_push: bool = False,
        pod_template_file: Optional[str] = None,
        priority_class_name: Optional[str] = None,
        pod_runtime_info_envs: Optional[List[k8s.V1EnvVar]] = None,
        termination_grace_period: Optional[int] = None,
        configmaps: Optional[List[str]] = None,
        resources: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")

        if isinstance(resources, k8s.V1ResourceRequirements):
            warnings.warn(
                "Specifying resources for the launched pod with 'resources' is deprecated. "
                "Use 'container_resources' instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            container_resources = resources
            resources = None

        super().__init__(resources=resources, **kwargs)
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
        if node_selectors:
            # Node selectors is incorrect based on k8s API
            warnings.warn(
                "node_selectors is deprecated. Please use node_selector instead.", DeprecationWarning
            )
            self.node_selector = node_selectors
        elif node_selector:
            self.node_selector = node_selector
        else:
            self.node_selector = {}
        self.annotations = annotations or {}
        self.affinity = convert_affinity(affinity) if affinity else {}
        self.k8s_resources = convert_resources(container_resources) if container_resources else {}
        self.config_file = config_file
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if image_pull_secrets else []
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = (
            [convert_toleration(toleration) for toleration in tolerations] if tolerations else []
        )
        self.security_context = security_context or {}
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
        self.pod_request_obj: Optional[k8s.V1Pod] = None
        self.pod: Optional[k8s.V1Pod] = None

    def _render_nested_template_fields(
        self,
        content: Any,
        context: 'Context',
        jinja_env: "jinja2.Environment",
        seen_oids: set,
    ) -> None:
        if id(content) not in seen_oids and isinstance(content, k8s.V1EnvVar):
            seen_oids.add(id(content))
            self._do_render_template_fields(content, ('value', 'name'), context, jinja_env, seen_oids)
            return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    @staticmethod
    def _get_ti_pod_labels(context: Optional[dict] = None, include_try_number: bool = True) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash

        :param context: task context provided by airflow DAG
        :return: dict
        """
        if not context:
            return {}

        ti = context['ti']
        run_id = context['run_id']

        labels = {
            'dag_id': ti.dag_id,
            'task_id': ti.task_id,
            'run_id': run_id,
            'kubernetes_pod_operator': 'True',
        }

        # If running on Airflow 2.3+:
        map_index = getattr(ti, 'map_index', -1)
        if map_index >= 0:
            labels['map_index'] = map_index

        if include_try_number:
            labels.update(try_number=ti.try_number)
        # In the case of sub dags this is just useful
        if context['dag'].is_subdag:
            labels['parent_dag_id'] = context['dag'].parent_dag.dag_id
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
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        self._patch_deprecated_k8s_settings(hook)
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        hook = self.get_hook()
        return hook.core_v1_client

    def find_pod(self, namespace, context, *, exclude_checked=True) -> Optional[k8s.V1Pod]:
        """Returns an already-running pod for this task instance if one exists."""
        label_selector = self._build_find_pod_label_selector(context, exclude_checked=exclude_checked)
        pod_list = self.client.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
        ).items

        pod = None
        num_pods = len(pod_list)
        if num_pods > 1:
            raise AirflowException(f'More than one pod running with labels {label_selector}')
        elif num_pods == 1:
            pod = pod_list[0]
            self.log.info("Found matching pod %s with labels %s", pod.metadata.name, pod.metadata.labels)
            self.log.info("`try_number` of task_instance: %s", context['ti'].try_number)
            self.log.info("`try_number` of pod: %s", pod.metadata.labels['try_number'])
        return pod

    def get_or_create_pod(self, pod_request_obj: k8s.V1Pod, context):
        if self.reattach_on_restart:
            pod = self.find_pod(self.namespace or pod_request_obj.metadata.namespace, context=context)
            if pod:
                return pod
        self.log.debug("Starting pod:\n%s", yaml.safe_dump(pod_request_obj.to_dict()))
        self.pod_manager.create_pod(pod=pod_request_obj)
        return pod_request_obj

    def await_pod_start(self, pod):
        try:
            self.pod_manager.await_pod_start(pod=pod, startup_timeout=self.startup_timeout_seconds)
        except PodLaunchFailedException:
            if self.log_events_on_failure:
                for event in self.pod_manager.read_pod_events(pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            raise

    def extract_xcom(self, pod):
        """Retrieves xcom value and kills xcom sidecar container"""
        result = self.pod_manager.extract_xcom(pod)
        self.log.info("xcom result: \n%s", result)
        return json.loads(result)

    def execute(self, context: 'Context'):
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
                result = self.extract_xcom(pod=self.pod)
            remote_pod = self.pod_manager.await_pod_completion(self.pod)
        finally:
            self.cleanup(
                pod=self.pod or self.pod_request_obj,
                remote_pod=remote_pod,
            )
        ti = context['ti']
        ti.xcom_push(key='pod_name', value=self.pod.metadata.name)
        ti.xcom_push(key='pod_namespace', value=self.pod.metadata.namespace)
        if self.do_xcom_push:
            return result

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, 'status') else None
        if not self.is_delete_operator_pod:
            with _suppress(Exception):
                self.patch_already_checked(remote_pod)
        if pod_phase != PodPhase.SUCCEEDED:
            if self.log_events_on_failure:
                with _suppress(Exception):
                    for event in self.pod_manager.read_pod_events(pod).items:
                        self.log.error("Pod Event: %s - %s", event.reason, event.message)
            with _suppress(Exception):
                self.process_pod_deletion(remote_pod)
            error_message = get_container_termination_message(remote_pod, self.BASE_CONTAINER_NAME)
            error_message = "\n" + error_message if error_message else ""
            raise AirflowException(
                f'Pod {pod and pod.metadata.name} returned a failure:{error_message}\n{remote_pod}'
            )
        else:
            with _suppress(Exception):
                self.process_pod_deletion(remote_pod)

    def process_pod_deletion(self, pod):
        if pod is not None:
            if self.is_delete_operator_pod:
                self.log.info("Deleting pod: %s", pod.metadata.name)
                self.pod_manager.delete_pod(pod)
            else:
                self.log.info("skipping deleting pod: %s", pod.metadata.name)

    def _build_find_pod_label_selector(self, context: Optional[dict] = None, *, exclude_checked=True) -> str:
        labels = self._get_ti_pod_labels(context, include_try_number=False)
        label_strings = [f'{label_id}={label}' for label_id, label in sorted(labels.items())]
        labels_value = ','.join(label_strings)
        if exclude_checked:
            labels_value += f',{self.POD_CHECKED_KEY}!=True'
        labels_value += ',!airflow-worker'
        return labels_value

    def _set_name(self, name):
        if name is None:
            if self.pod_template_file or self.full_pod_spec:
                return None
            raise AirflowException("`name` is required unless `pod_template_file` or `full_pod_spec` is set")

        validate_key(name, max_length=220)
        return re.sub(r'[^a-z0-9-]+', '-', name.lower())

    def patch_already_checked(self, pod: k8s.V1Pod):
        """Add an "already checked" annotation to ensure we don't reattach on retries"""
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

    def build_pod_request_obj(self, context=None):
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
            pod_template = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="name"))

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
                        resources=self.k8s_resources,
                        volume_mounts=self.volume_mounts,
                        args=self.arguments,
                        env=self.env_vars,
                        env_from=self.env_from,
                    )
                ],
                image_pull_secrets=self.image_pull_secrets,
                service_account_name=self.service_account_name,
                host_network=self.hostnetwork,
                security_context=self.security_context,
                dns_policy=self.dnspolicy,
                scheduler_name=self.schedulername,
                restart_policy='Never',
                priority_class_name=self.priority_class_name,
                volumes=self.volumes,
            ),
        )

        pod = PodGenerator.reconcile_pods(pod_template, pod)

        if self.random_name_suffix:
            pod.metadata.name = PodGenerator.make_unique_pod_id(pod.metadata.name)

        for secret in self.secrets:
            self.log.debug("Adding secret to task %s", self.task_id)
            pod = secret.attach_to_pod(pod)
        if self.do_xcom_push:
            self.log.debug("Adding xcom sidecar to task %s", self.task_id)
            pod = xcom_sidecar.add_xcom_sidecar(pod)

        labels = self._get_ti_pod_labels(context)
        self.log.info("Creating pod %s with labels: %s", pod.metadata.name, labels)

        # Merge Pod Identifying labels with labels passed to operator
        pod.metadata.labels.update(labels)
        # Add Airflow Version to the label
        # And a label to identify that pod is launched by KubernetesPodOperator
        pod.metadata.labels.update(
            {
                'airflow_version': airflow_version.replace('+', '-'),
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
        print(yaml.dump(prune_dict(pod.to_dict(), mode='strict')))

    def _patch_deprecated_k8s_settings(self, hook: KubernetesHook):
        """
        Here we read config from core Airflow config [kubernetes] section.
        In a future release we will stop looking at this section and require users
        to use Airflow connections to configure KPO.

        When we find values there that we need to apply on the hook, we patch special
        hook attributes here.
        """
        # default for enable_tcp_keepalive is True; patch if False
        if conf.getboolean('kubernetes', 'enable_tcp_keepalive') is False:
            hook._deprecated_core_disable_tcp_keepalive = True

        # default verify_ssl is True; patch if False.
        if conf.getboolean('kubernetes', 'verify_ssl') is False:
            hook._deprecated_core_disable_verify_ssl = True

        # default for in_cluster is True; patch if False and no KPO param.
        conf_in_cluster = conf.getboolean('kubernetes', 'in_cluster')
        if self.in_cluster is None and conf_in_cluster is False:
            hook._deprecated_core_in_cluster = conf_in_cluster

        # there's no default for cluster context; if we get something (and no KPO param) patch it.
        conf_cluster_context = conf.get('kubernetes', 'cluster_context', fallback=None)
        if not self.cluster_context and conf_cluster_context:
            hook._deprecated_core_cluster_context = conf_cluster_context

        # there's no default for config_file; if we get something (and no KPO param) patch it.
        conf_config_file = conf.get('kubernetes', 'config_file', fallback=None)
        if not self.config_file and conf_config_file:
            hook._deprecated_core_config_file = conf_config_file


class _suppress(AbstractContextManager):
    """
    This behaves the same as ``contextlib.suppress`` but logs the suppressed
    exceptions as errors with traceback.

    The caught exception is also stored on the context manager instance under
    attribute ``exception``.
    """

    def __init__(self, *exceptions):
        self._exceptions = exceptions
        self.exception = None

    def __enter__(self):
        return self

    def __exit__(self, exctype, excinst, exctb):
        caught_error = exctype is not None and issubclass(exctype, self._exceptions)
        if caught_error:
            self.exception = excinst
            logger = logging.getLogger(__name__)
            logger.error(str(excinst), exc_info=True)
        return caught_error
