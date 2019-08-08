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

import kubernetes.client.models as k8s
from airflow.executors import Executors
from typing import Dict
import copy


class PodDefaults:
    XCOM_MOUNT_PATH = '/airflow/xcom'
    SIDECAR_CONTAINER_NAME = 'airflow-xcom-sidecar'
    XCOM_CMD = """import time
    while True:
        try:
            time.sleep(3600)
        except KeyboardInterrupt:
            exit(0)
    """

    VOLUME_MOUNT = k8s.V1VolumeMount(
        name='xcom',
        mount_path=XCOM_MOUNT_PATH
    )

    VOLUME = k8s.V1Volume(
        name='xcom',
        empty_dir=k8s.V1EmptyDirVolumeSource()
    )

    SIDECAR_CONTAINER = k8s.V1Container(
        name=SIDECAR_CONTAINER_NAME,
        command=['python', '-c', XCOM_CMD],
        image='python:3.5-alpine',
        volume_mounts=[VOLUME_MOUNT]
    )


class PodGenerator:
    """Contains Kubernetes Airflow Worker configuration logic"""

    """
    Represents a kubernetes pod and manages execution of a single pod.
    :param image: The docker image
    :type image: str
    :param envs: A dict containing the environment variables
    :type envs: dict
    :param cmds: The command to be run on the pod
    :type cmds: list[str]
    :param secrets: Secrets to be launched to the pod
    :type secrets: list[airflow.contrib.kubernetes.secret.Secret]
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a comma separated list:
        secret_a,secret_b
    :type image_pull_secrets: str
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: dict
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: list
    :param security_context: A dict containing the security context for the pod
    :type security_context: dict
    :param configmaps: A list containing names of configmaps object
        mounting env variables to the pod
    :type configmaps: list[str]
    :param pod_runtime_info_envs: environment variables about
                                  pod runtime information (ip, namespace, nodeName, podName)
    :type pod_runtime_info_envs: list[PodRuntimeEnv]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: str
    :param pod: The fully specified pod.
    :type pod: V1Pod
    
    """

    def __init__(
        self,
        image='airflow-worker:latest',
        envs=None,
        cmds=None,
        args=None,
        labels=None,
        node_selectors=None,
        name='base',
        ports=None,
        volumes=None,
        volume_mounts=None,
        namespace='default',
        image_pull_policy='IfNotPresent',
        restart_policy='Never',
        image_pull_secrets=None,
        init_containers=None,
        service_account_name=None,
        resources=None,
        annotations=None,
        affinity=None,
        hostnetwork=False,
        tolerations=None,
        security_context=None,
        configmaps=None,
        dnspolicy=None,
        pod: k8s.V1Pod = None,
        extract_xcom=False,
    ):
        self.ud_pod = pod
        self.pod: k8s.V1Pod = k8s.V1Pod()
        self.pod.api_version = 'v1'
        self.pod.kind = 'Pod'

        # Pod Metadata
        self.metadata = k8s.V1ObjectMeta()
        self.metadata.labels = labels
        self.metadata.name = name
        self.metadata.namespace = namespace
        self.metadata.annotations = annotations

        # Pod Spec
        self.spec: k8s.V1PodSpec = k8s.V1PodSpec()
        self.spec.security_context = security_context
        self.spec.tolerations = tolerations
        self.spec.dns_policy = dnspolicy
        self.spec.host_network = hostnetwork
        self.spec.affinity = affinity
        self.spec.service_account_name = service_account_name
        self.spec.init_containers = init_containers
        self.spec.volumes = volumes or []
        self.spec.node_selector = node_selectors
        self.spec.restart_policy = restart_policy

        # Pod Container
        self.container: k8s.V1Container = k8s.V1Container()
        self.container.image = image
        self.container.envs = envs or []
        self.container.name = 'base'
        self.container.env_from = configmaps or []
        self.container.command = cmds or ["/usr/local/airflow/entrypoint.sh", "/bin/bash sleep 25"]
        self.container.args = args or []
        self.container.image_pull_policy = image_pull_policy
        self.container.ports = ports or []
        self.container.resources = resources
        self.container.volume_mounts = volume_mounts or []
        self.spec.image_pull_secrets = image_pull_secrets or []

        # Attach sidecar
        self.extract_xcom = extract_xcom

    def gen_pod(self) -> k8s.V1Pod:
        result = self.ud_pod

        if result is None:
            result = self.pod
            result.spec = self.spec
            result.metadata = self.metadata
            result.spec.containers = [self.container]

        if self.extract_xcom:
            result = self.add_sidecar(result)

        return result

    @staticmethod
    def add_sidecar(pod: k8s.V1Pod) -> k8s.V1Pod:
        pod_cp = copy.deepcopy(pod)

        pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
        pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
        pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

        return pod_cp

    @staticmethod
    def from_dict(obj) -> k8s.V1Pod:
        if obj is None:
            return k8s.V1Pod()

        if not isinstance(obj, dict):
            raise TypeError(
                'Cannot convert a non-dictionary object into a KubernetesExecutorConfig')

        namespaced = obj.get(Executors.KubernetesExecutor, {})

        resources = namespaced.get('resources')

        if resources is None:
            resources = k8s.V1ResourceRequirements(
                requests={
                    'cpu': namespaced.get('request_cpu'),
                    'memory': namespaced.get('request_memory')

                },
                limits={
                    'cpu': namespaced.get('limit_cpu'),
                    'memory': namespaced.get('limit_memory')
                }
            )

        annotations: Dict[str, str] = namespaced.get('annotations', {})
        gcp_service_account_key = namespaced.get('gcp_service_account_key', None)

        if annotations is not None and gcp_service_account_key is not None:
            annotations.update({
                'iam.cloud.google.com/service-account': gcp_service_account_key
            })

        pod_spec_generator = PodGenerator(
            image=namespaced.get('image'),
            envs=namespaced.get('env'),
            cmds=namespaced.get('cmds'),
            args=namespaced.get('args'),
            labels=namespaced.get('labels'),
            node_selectors=namespaced.get('node_selectors'),
            name=namespaced.get('name', 'base'),
            ports=namespaced.get('ports'),
            volumes=namespaced.get('volumes'),
            volume_mounts=namespaced.get('volume_mounts'),
            namespace=namespaced.get('namespace', 'default'),
            image_pull_policy=namespaced.get('image_pull_policy', 'IfNotPresent'),
            restart_policy=namespaced.get('restart_policy', 'Never'),
            image_pull_secrets=namespaced.get('image_pull_secrets'),
            init_containers=namespaced.get('init_containers'),
            service_account_name=namespaced.get('service_account_name'),
            resources=resources,
            annotations=namespaced.get('annotations'),
            affinity=namespaced.get('affinity'),
            hostnetwork=namespaced.get('hostnetwork', False),
            tolerations=namespaced.get('tolerations'),
            security_context=namespaced.get('security_context'),
            configmaps=namespaced.get('configmaps'),
            dnspolicy=namespaced.get('dnspolicy'),
            pod=namespaced.get('pod'),
            extract_xcom=namespaced.get('extract_xcom', False),
        )

        return pod_spec_generator.gen_pod()

    @staticmethod
    def reconcile_pods(base_pod: k8s.V1Pod, client_pod: k8s.V1Pod) -> k8s.V1Pod:
        """
        :param base_pod: has the base attributes which are overwritten if they exist
            in the client pod and remain if they do not exist in the client_pod
        :type base_pod: k8s.V1Pod
        :param client_pod: the pod that the client wants to create.
        :type client_pod: k8s.V1Pod
        :return: the merged pods

        This can't be done recursively as certain fields are preserved,
        some overwritten, and some concatenated, e.g. the [k8s.V1PodSpec]
        is overwritten in some spots but env should be concatenated to.
        """

        client_pod_cp = copy.deepcopy(client_pod)

        def merge_objects(base_obj, client_obj):
            for base_key, base_val in base_obj.to_dict().items():
                if getattr(client_obj, base_key, None) is None and base_val is not None:
                    setattr(client_obj, base_key, base_val)

        def extend_objects(base_obj, client_obj):
            for key, key_type in base_obj.swagger_types:
                if key_type.startswith('list'):
                    base_obj_val = getattr(base_obj, key)
                    client_obj_val = getattr(client_obj, key)
                    if base_obj_val is not None and client_obj_val is None:
                        setattr(client_obj, key, base_obj_val)
                    elif base_obj_val is not None and client_obj_val is not None:
                        setattr(client_obj_val, key, client_obj_val + base_obj_val)

        def merge_and_extend_objects(base_obj, client_obj):
            merge_objects(base_obj, client_obj)
            extend_objects(base_obj, client_obj)

        merge_and_extend_objects(base_pod, client_pod_cp)
        merge_and_extend_objects(base_pod.spec, client_pod_cp.spec)
        merge_and_extend_objects(base_pod.metadata, client_pod_cp.metadata)
        merge_and_extend_objects(base_pod.spec.containers[0], client_pod_cp.spec.containers[0])

        return client_pod_cp
