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
from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.kubernetes.k8s_model import append_to_pod
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class KubernetesPodOperator(BaseOperator):  # pylint: disable=too-many-instance-attributes
    """
    Execute a task in a Kubernetes Pod

    .. note::
        If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__, use
        :class:`~airflow.gcp.operators.kubernetes_engine.GKEPodOperator`, which
        simplifies the authorization process.

    :param image: Docker image you wish to launch. Defaults to dockerhub.io,
        but fully qualified URLS will point to custom repositories
    :type image: str
    :param namespace: the namespace to run within kubernetes
    :type namespace: str
    :param cmds: entrypoint of the container. (templated)
        The docker images's entrypoint is used if this is not provide.
    :type cmds: list[str]
    :param arguments: arguments of the entrypoint. (templated)
        The docker image's CMD is used if this is not provided.
    :type arguments: list[str]
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
                               If more than one secret is required, provide a
                               comma separated list: secret_a,secret_b
    :type image_pull_secrets: str
    :param ports: ports for launched pod
    :type ports: list[airflow.kubernetes.models.port.Port]
    :param volume_mounts: volumeMounts for launched pod
    :type volume_mounts: list[airflow.kubernetes.models.volume_mount.VolumeMount]
    :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes
    :type volumes: list[airflow.kubernetes.models.volume.Volume]
    :param labels: labels to apply to the Pod
    :type labels: dict
    :param startup_timeout_seconds: timeout in seconds to startup the pod
    :type startup_timeout_seconds: int
    :param name: name of the task you want to run,
        will be used to generate a pod id
    :type name: str
    :param env_vars: Environment variables initialized in the container. (templated)
    :type env_vars: dict
    :param secrets: Kubernetes secrets to inject in the container,
        They can be exposed as environment vars or files in a volume.
    :type secrets: list[airflow.kubernetes.models.secret.Secret]
    :param in_cluster: run kubernetes client with in_cluster configuration
    :type in_cluster: bool
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :type cluster_context: str
    :param get_logs: get the stdout of the container as logs of the tasks
    :type get_logs: bool
    :param annotations: non-identifying metadata you can attach to the Pod.
                        Can be a large range of data, and can include characters
                        that are not permitted by labels.
    :type annotations: dict
    :param resources: A dict containing a group of resources requests and limits
    :type resources: dict
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: dict
    :param node_selectors: A dict containing a group of scheduling rules
    :type node_selectors: dict
    :param config_file: The path to the Kubernetes config file.
        If not specified, default value is ``~/.kube/config``
    :type config_file: str
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :type do_xcom_push: bool
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted.
        If False (default): do nothing, If True: delete the pod
    :type is_delete_operator_pod: bool
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: list tolerations
    :param configmaps: A list of configmap names objects that we
        want mount as env variables
    :type configmaps: list[str]
    :param pod_runtime_info_envs: environment variables about
                                  pod runtime information (ip, namespace, nodeName, podName)
    :type pod_runtime_info_envs: list[airflow.kubernetes.models.pod_runtime_info_env.PodRuntimeInfoEnv]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: str
    :param full_pod_spec: The complete podSpec
    :type full_pod_spec: kubernetes.client.models.V1Pod
    """
    template_fields = ('cmds', 'arguments', 'env_vars', 'config_file')

    def execute(self, context):
        try:
            client = kube_client.get_kube_client(in_cluster=self.in_cluster,
                                                 cluster_context=self.cluster_context,
                                                 config_file=self.config_file)

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
                resources=self.resources,
                pod=self.full_pod_spec,
            ).gen_pod()

            pod = append_to_pod(pod, self.ports)
            pod = append_to_pod(pod, self.pod_runtime_info_envs)
            pod = append_to_pod(pod, self.volumes)
            pod = append_to_pod(pod, self.volume_mounts)
            pod = append_to_pod(pod, self.secrets)

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

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments,too-many-locals
                 namespace,
                 image,
                 name,
                 cmds=None,
                 arguments=None,
                 ports=None,
                 volume_mounts=None,
                 volumes=None,
                 env_vars=None,
                 secrets=None,
                 in_cluster=False,
                 cluster_context=None,
                 labels=None,
                 startup_timeout_seconds=120,
                 get_logs=True,
                 image_pull_policy='IfNotPresent',
                 annotations=None,
                 resources=None,
                 affinity=None,
                 config_file=None,
                 do_xcom_push=False,
                 node_selectors=None,
                 image_pull_secrets=None,
                 service_account_name="default",
                 is_delete_operator_pod=False,
                 hostnetwork=False,
                 tolerations=None,
                 configmaps=None,
                 security_context=None,
                 pod_runtime_info_envs=None,
                 dnspolicy=None,
                 full_pod_spec=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.pod = None

        self.image = image
        self.namespace = namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = name
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
        self.do_xcom_push = do_xcom_push
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        self.resources = resources
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
        self.full_pod_spec = full_pod_spec
