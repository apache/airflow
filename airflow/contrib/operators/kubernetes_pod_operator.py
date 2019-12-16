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
import re
from typing import Dict, List, Optional

import kubernetes.client.models as k8s

from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.pod import Port, Resources
from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import validate_key
from airflow.utils.state import State
from airflow.version import version as airflow_version


class KubernetesPodOperator(BaseOperator):  # pylint: disable=too-many-instance-attributes
    """
    Execute a task in a Kubernetes Pod

    .. note::
        If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__, use
        :class:`~airflow.gcp.operators.kubernetes_engine.GKEPodOperator`, which
        simplifies the authorization process.

    :param namespace: the namespace to run within kubernetes.
    :type namespace: str
    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
        but fully qualified URLS will point to custom repositories.
    :type image: str
    :param name: name of the pod in which the task will run, will be used to
        generate a pod id (DNS-1123 subdomain, containing only [a-z0-9.-]).
    :type name: str
    :param cmds: entrypoint of the container. (templated)
        The docker images's entrypoint is used if this is not provided.
    :type cmds: list[str]
    :param arguments: arguments of the entrypoint. (templated)
        The docker image's CMD is used if this is not provided.
    :type arguments: list[str]
    :param ports: ports for launched pod.
    :type ports: list[airflow.kubernetes.pod.Port]
    :param volume_mounts: volumeMounts for launched pod.
    :type volume_mounts: list[airflow.kubernetes.volume_mount.VolumeMount]
    :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes.
    :type volumes: list[airflow.kubernetes.volume.Volume]
    :param env_vars: Environment variables initialized in the container. (templated)
    :type env_vars: dict
    :param secrets: Kubernetes secrets to inject in the container.
        They can be exposed as environment vars or files in a volume.
    :type secrets: list[airflow.kubernetes.secret.Secret]
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :type in_cluster: bool
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :type cluster_context: str
    :param labels: labels to apply to the Pod.
    :type labels: dict
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :type startup_timeout_seconds: int
    :param get_logs: get the stdout of the container as logs of the tasks.
    :type get_logs: bool
    :param image_pull_policy: Specify a policy to cache or always pull an image.
    :type image_pull_policy: str
    :param annotations: non-identifying metadata you can attach to the Pod.
        Can be a large range of data, and can include characters
        that are not permitted by labels.
    :type annotations: dict
    :param resources: A dict containing resources requests and limits.
        Possible keys are request_memory, request_cpu, limit_memory, limit_cpu,
        and limit_gpu, which will be used to generate airflow.kubernetes.pod.Resources.
        See also kubernetes.io/docs/concepts/configuration/manage-compute-resources-container
    :type resources: dict
    :param affinity: A dict containing a group of affinity scheduling rules.
    :type affinity: dict
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    :type config_file: str
    :param node_selectors: A dict containing a group of scheduling rules.
    :type node_selectors: dict
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a
        comma separated list: secret_a,secret_b
    :type image_pull_secrets: str
    :param service_account_name: Name of the service account
    :type service_account_name: str
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted.
        If False (default): do nothing, If True: delete the pod
    :type is_delete_operator_pod: bool
    :param hostnetwork: If True enable host networking on the pod.
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations.
    :type tolerations: list tolerations
    :param configmaps: A list of configmap names objects that we
        want mount as env variables.
    :type configmaps: list[str]
    :param security_context: security options the pod should run with (PodSecurityContext).
    :type security_context: dict
    :param pod_runtime_info_envs: environment variables about
        pod runtime information (ip, namespace, nodeName, podName).
    :type pod_runtime_info_envs: list[airflow.kubernetes.pod_runtime_info_env.PodRuntimeInfoEnv]
    :param dnspolicy: dnspolicy for the pod.
    :type dnspolicy: str
    :param schedulername: Specify a schedulername for the pod
    :type schedulername: str
    :param full_pod_spec: The complete podSpec
    :type full_pod_spec: kubernetes.client.models.V1Pod
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :type do_xcom_push: bool
    :param init_containers: init container for the launched Pod
    :type init_containers: list[kubernetes.client.models.V1Container]
    """
    template_fields = ('cmds', 'arguments', 'env_vars', 'config_file')

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments,too-many-locals
                 namespace: str,
                 image: str,
                 name: str,
                 cmds: Optional[List[str]] = None,
                 arguments: Optional[List[str]] = None,
                 ports: Optional[List[Port]] = None,
                 volume_mounts: Optional[List[VolumeMount]] = None,
                 volumes: Optional[List[Volume]] = None,
                 env_vars: Optional[Dict] = None,
                 secrets: Optional[List[Secret]] = None,
                 in_cluster: bool = True,
                 cluster_context: Optional[str] = None,
                 labels: Optional[Dict] = None,
                 startup_timeout_seconds: int = 120,
                 get_logs: bool = True,
                 image_pull_policy: str = 'IfNotPresent',
                 annotations: Optional[Dict] = None,
                 resources: Optional[Dict] = None,
                 affinity: Optional[Dict] = None,
                 config_file: Optional[str] = None,
                 node_selectors: Optional[Dict] = None,
                 image_pull_secrets: Optional[str] = None,
                 service_account_name: str = 'default',
                 is_delete_operator_pod: bool = False,
                 hostnetwork: bool = False,
                 tolerations: Optional[List] = None,
                 configmaps: Optional[List] = None,
                 security_context: Optional[Dict] = None,
                 pod_runtime_info_envs: Optional[List[PodRuntimeInfoEnv]] = None,
                 dnspolicy: Optional[str] = None,
                 schedulername: Optional[str] = None,
                 full_pod_spec: Optional[k8s.V1Pod] = None,
                 init_containers: Optional[List[k8s.V1Container]] = None,
                 *args,
                 **kwargs):
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(*args, resources=None, **kwargs)

        self.pod = None

        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = self._set_name(name)
        self.env_vars = env_vars or {}
        self.ports = ports or []
        self.volume_mounts = volume_mounts or []
        self.volumes = volumes or []
        self.secrets = secrets or []
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors or {}
        self.annotations = annotations or {}
        self.affinity = affinity or {}
        self.resources = self._set_resources(resources)
        self.config_file = config_file
        self.image_pull_secrets = image_pull_secrets
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = tolerations or []
        self.configmaps = configmaps or []
        self.security_context = security_context or {}
        self.pod_runtime_info_envs = pod_runtime_info_envs or []
        self.dnspolicy = dnspolicy
        self.schedulername = schedulername
        self.full_pod_spec = full_pod_spec
        self.init_containers = init_containers or []

    def execute(self, context):
        try:
            client = kube_client.get_kube_client(in_cluster=self.in_cluster,
                                                 cluster_context=self.cluster_context,
                                                 config_file=self.config_file)

            # Add Airflow Version to the label
            # And a label to identify that pod is launched by KubernetesPodOperator
            self.labels.update(
                {
                    'airflow_version': airflow_version.replace('+', '-'),
                    'kubernetes_pod_operator': 'True',
                }
            )

            pod = pod_generator.PodGenerator(
                image=self.image,
                namespace=self.namespace,
                cmds=self.cmds,
                args=self.arguments,
                labels=self.labels,
                name=self.name,
                envs=self.env_vars,
                extract_xcom=self.do_xcom_push,
                image_pull_policy=self.image_pull_policy,
                node_selectors=self.node_selectors,
                annotations=self.annotations,
                affinity=self.affinity,
                image_pull_secrets=self.image_pull_secrets,
                service_account_name=self.service_account_name,
                hostnetwork=self.hostnetwork,
                tolerations=self.tolerations,
                configmaps=self.configmaps,
                security_context=self.security_context,
                dnspolicy=self.dnspolicy,
                schedulername=self.schedulername,
                pod=self.full_pod_spec,
                init_containers=self.init_containers,
            ).gen_pod()

            pod = append_to_pod(
                pod,
                self.pod_runtime_info_envs +
                self.ports +
                self.resources +
                self.secrets +
                self.volumes +
                self.volume_mounts
            )

            self.pod = pod

            launcher = pod_launcher.PodLauncher(kube_client=client,
                                                extract_xcom=self.do_xcom_push)

            try:
                (final_state, result) = launcher.run_pod(
                    pod,
                    startup_timeout=self.startup_timeout_seconds,
                    get_logs=self.get_logs)
            finally:
                if self.is_delete_operator_pod:
                    launcher.delete_pod(pod)

            if final_state != State.SUCCESS:
                raise AirflowException(
                    'Pod returned a failure: {state}'.format(state=final_state)
                )

            return result
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))

    @staticmethod
    def _set_resources(resources):
        if not resources:
            return []
        return [Resources(**resources)]

    @staticmethod
    def _set_name(name):
        validate_key(name, max_length=63)
        return re.sub(r'[^a-z0-9.-]+', '-', name.lower())
