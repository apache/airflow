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
from typing import Dict, Iterable, List, Optional, Tuple

import yaml
from kubernetes.client import models as k8s

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

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesPodOperator`

    .. note::
        If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
        and Airflow is not running in the same cluster, consider using
        :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`, which
        simplifies the authorization process.

    :param namespace: the namespace to run within kubernetes.
    :type namespace: str
    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
        but fully qualified URLS will point to custom repositories.
    :type image: str
    :param name: name of the pod in which the task will run, will be used (plus a random
        suffix) to generate a pod id (DNS-1123 subdomain, containing only [a-z0-9.-]).
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
    :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
    :type reattach_on_restart: bool
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
    :param init_containers: init container for the launched Pod
    :type init_containers: list[kubernetes.client.models.V1Container]
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :type log_events_on_failure: bool
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :type do_xcom_push: bool
    :param pod_template_file: path to pod template file
    :type pod_template_file: str
    :param priority_class_name: priority class name for the launched Pod
    :type priority_class_name: str
    """
    template_fields: Iterable[str] = (
        'image', 'cmds', 'arguments', 'env_vars', 'config_file', 'pod_template_file')

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments,too-many-locals
                 namespace: Optional[str] = None,
                 image: Optional[str] = None,
                 name: Optional[str] = None,
                 cmds: Optional[List[str]] = None,
                 arguments: Optional[List[str]] = None,
                 ports: Optional[List[Port]] = None,
                 volume_mounts: Optional[List[VolumeMount]] = None,
                 volumes: Optional[List[Volume]] = None,
                 env_vars: Optional[Dict] = None,
                 secrets: Optional[List[Secret]] = None,
                 in_cluster: Optional[bool] = None,
                 cluster_context: Optional[str] = None,
                 labels: Optional[Dict] = None,
                 reattach_on_restart: bool = True,
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
                 log_events_on_failure: bool = False,
                 do_xcom_push: bool = False,
                 pod_template_file: Optional[str] = None,
                 priority_class_name: Optional[str] = None,
                 **kwargs):
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(resources=None, **kwargs)

        self.pod = None
        self.do_xcom_push = do_xcom_push
        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.env_vars = env_vars or {}
        self.ports = ports or []
        self.volume_mounts = volume_mounts or []
        self.volumes = volumes or []
        self.secrets = secrets or []
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.reattach_on_restart = reattach_on_restart
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
        self.log_events_on_failure = log_events_on_failure
        self.priority_class_name = priority_class_name
        self.pod_template_file = pod_template_file
        self.name = self._set_name(name)

    @staticmethod
    def create_labels_for_pod(context) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash

        :param context: task context provided by airflow DAG
        :return: dict
        """
        labels = {
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'execution_date': context['ts'],
            'try_number': context['ti'].try_number,
        }
        # In the case of sub dags this is just useful
        if context['dag'].is_subdag:
            labels['parent_dag_id'] = context['dag'].parent_dag.dag_id
        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    def execute(self, context) -> Optional[str]:
        try:
            if self.in_cluster is not None:
                client = kube_client.get_kube_client(in_cluster=self.in_cluster,
                                                     cluster_context=self.cluster_context,
                                                     config_file=self.config_file)
            else:
                client = kube_client.get_kube_client(cluster_context=self.cluster_context,
                                                     config_file=self.config_file)

            # Add combination of labels to uniquely identify a running pod
            labels = self.create_labels_for_pod(context)

            label_selector = self._get_pod_identifying_label_string(labels)

            pod_list = client.list_namespaced_pod(self.namespace, label_selector=label_selector)

            if len(pod_list.items) > 1:
                raise AirflowException(
                    'More than one pod running with labels: '
                    '{label_selector}'.format(label_selector=label_selector))

            launcher = pod_launcher.PodLauncher(kube_client=client, extract_xcom=self.do_xcom_push)

            if len(pod_list.items) == 1 and \
                    self._try_numbers_do_not_match(context, pod_list.items[0]) and \
                    self.reattach_on_restart:
                self.log.info("found a running pod with labels %s but a different try_number"
                              "Will attach to this pod and monitor instead of starting new one", labels)
                final_state, _, result = self.create_new_pod_for_operator(labels, launcher)
            elif len(pod_list.items) == 1:
                final_state, result = self.monitor_launched_pod(launcher, pod_list[0])
            else:
                self.log.info("creating pod with labels %s and launcher %s", labels, launcher)
                final_state, _, result = self.create_new_pod_for_operator(labels, launcher)
            if final_state != State.SUCCESS:
                raise AirflowException(
                    'Pod returned a failure: {state}'.format(state=final_state))
            return result
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))

    @staticmethod
    def _get_pod_identifying_label_string(labels):
        filtered_labels = {label_id: label for label_id, label in labels.items() if label_id != 'try_number'}
        return ','.join([label_id + '=' + label for label_id, label in sorted(filtered_labels.items())])

    @staticmethod
    def _try_numbers_do_not_match(context, pod):
        return pod.metadata.labels['try_number'] != context['ti'].try_number

    @staticmethod
    def _set_resources(resources):
        if not resources:
            return []
        return [Resources(**resources)]

    def _set_name(self, name):
        if self.pod_template_file or self.full_pod_spec:
            return None
        validate_key(name, max_length=220)
        return re.sub(r'[^a-z0-9.-]+', '-', name.lower())

    def create_new_pod_for_operator(self, labels, launcher) -> Tuple[State, k8s.V1Pod, Optional[str]]:
        """
        Creates a new pod and monitors for duration of task

        :param labels: labels used to track pod
        :param launcher: pod launcher that will manage launching and monitoring pods
        :return:
        """
        if not (self.full_pod_spec or self.pod_template_file):
            # Add Airflow Version to the label
            # And a label to identify that pod is launched by KubernetesPodOperator
            self.labels.update(
                {
                    'airflow_version': airflow_version.replace('+', '-'),
                    'kubernetes_pod_operator': 'True',
                }
            )
            self.labels.update(labels)
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
            init_containers=self.init_containers,
            restart_policy='Never',
            priority_class_name=self.priority_class_name,
            pod_template_file=self.pod_template_file,
            pod=self.full_pod_spec,
        ).gen_pod()

        # noinspection PyTypeChecker
        pod = append_to_pod(
            pod,
            self.pod_runtime_info_envs +
            self.ports +  # type: ignore
            self.resources +
            self.secrets +  # type: ignore
            self.volumes +  # type: ignore
            self.volume_mounts  # type: ignore
        )

        self.pod = pod
        self.log.debug("Starting pod:\n%s", yaml.safe_dump(pod.to_dict()))
        try:
            launcher.start_pod(
                pod,
                startup_timeout=self.startup_timeout_seconds)
            final_state, result = launcher.monitor_pod(pod=pod, get_logs=self.get_logs)
        except AirflowException:
            if self.log_events_on_failure:
                for event in launcher.read_pod_events(pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            raise
        finally:
            if self.is_delete_operator_pod:
                launcher.delete_pod(pod)
        return final_state, pod, result

    def monitor_launched_pod(self, launcher, pod) -> Tuple[State, Optional[str]]:
        """
        Monitors a pod to completion that was created by a previous KubernetesPodOperator

        :param launcher: pod launcher that will manage launching and monitoring pods
        :param pod: podspec used to find pod using k8s API
        :return:
        """
        try:
            (final_state, result) = launcher.monitor_pod(pod, get_logs=self.get_logs)
        finally:
            if self.is_delete_operator_pod:
                launcher.delete_pod(pod)
        if final_state != State.SUCCESS:
            if self.log_events_on_failure:
                for event in launcher.read_pod_events(pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            raise AirflowException(
                'Pod returned a failure: {state}'.format(state=final_state)
            )
        return final_state, result
