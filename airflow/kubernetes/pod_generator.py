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
"""
This module provides an interface between the previous Pod
API and outputs a kubernetes.client.models.V1Pod.
The advantage being that the full Kubernetes API
is supported and no serialization need be written.
"""

import copy
import uuid

import kubernetes.client.models as k8s

from airflow.executors import Executors


class PodDefaults:
    """
    Static defaults for the PodGenerator
    """
    XCOM_MOUNT_PATH = '/airflow/xcom'
    SIDECAR_CONTAINER_NAME = 'airflow-xcom-sidecar'
    XCOM_CMD = 'trap "exit 0" INT; while true; do sleep 30; done;'
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
        command=['sh', '-c', XCOM_CMD],
        image='alpine',
        volume_mounts=[VOLUME_MOUNT],
        resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "1m",
            }
        ),
    )


class PodGenerator:
    """
    Contains Kubernetes Airflow Worker configuration logic

    Represents a kubernetes pod and manages execution of a single pod.

    :param image: The docker image
    :type image: str
    :param envs: A dict containing the environment variables
    :type envs: Dict[str, str]
    :param cmds: The command to be run on the pod
    :type cmds: List[str]
    :param secrets: Secrets to be launched to the pod
    :type secrets: List[airflow.kubernetes.models.secret.Secret]
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
    :param configmaps: Any configmap refs to envfrom.
        If more than one configmap is required, provide a comma separated list
        configmap_a,configmap_b
    :type configmaps: str
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: str
    :param pod: The fully specified pod.
    :type pod: kubernetes.client.models.V1Pod
    """
    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        image,
        name=None,
        namespace=None,
        volume_mounts=None,
        envs=None,
        cmds=None,
        args=None,
        labels=None,
        node_selectors=None,
        ports=None,
        volumes=None,
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
        pod=None,
        extract_xcom=False,
    ):
        self.ud_pod = pod
        self.pod = k8s.V1Pod()
        self.pod.api_version = 'v1'
        self.pod.kind = 'Pod'

        # Pod Metadata
        self.metadata = k8s.V1ObjectMeta()
        self.metadata.labels = labels
        self.metadata.name = name + "-" + str(uuid.uuid4())[:8] if name else None
        self.metadata.namespace = namespace
        self.metadata.annotations = annotations

        # Pod Container
        self.container = k8s.V1Container(name='base')
        self.container.image = image
        self.container.env = []

        if envs:
            if isinstance(envs, dict):
                for key, val in envs.items():
                    self.container.env.append(k8s.V1EnvVar(
                        name=key,
                        value=val
                    ))
            elif isinstance(envs, list):
                self.container.env.extend(envs)

        configmaps = configmaps or []
        self.container.env_from = []
        for configmap in configmaps:
            self.container.env_from.append(k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=configmap
                )
            ))

        self.container.command = cmds or []
        self.container.args = args or []
        self.container.image_pull_policy = image_pull_policy
        self.container.ports = ports or []
        self.container.resources = resources
        self.container.volume_mounts = volume_mounts or []

        # Pod Spec
        self.spec = k8s.V1PodSpec(containers=[])
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

        self.spec.image_pull_secrets = []

        if image_pull_secrets:
            for image_pull_secret in image_pull_secrets.split(','):
                self.spec.image_pull_secrets.append(k8s.V1LocalObjectReference(
                    name=image_pull_secret
                ))

        # Attach sidecar
        self.extract_xcom = extract_xcom

    def gen_pod(self) -> k8s.V1Pod:
        """Generates pod"""
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
        """Adds sidecar"""
        pod_cp = copy.deepcopy(pod)

        pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
        pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
        pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

        return pod_cp

    @staticmethod
    def from_obj(obj) -> k8s.V1Pod:
        """Converts to pod from obj"""
        if obj is None:
            return k8s.V1Pod()

        if isinstance(obj, PodGenerator):
            return obj.gen_pod()

        if not isinstance(obj, dict):
            raise TypeError(
                'Cannot convert a non-dictionary or non-PodGenerator '
                'object into a KubernetesExecutorConfig')

        namespaced = obj.get(Executors.KubernetesExecutor, {})

        resources = namespaced.get('resources')

        if resources is None:
            requests = {
                'cpu': namespaced.get('request_cpu'),
                'memory': namespaced.get('request_memory')

            }
            limits = {
                'cpu': namespaced.get('limit_cpu'),
                'memory': namespaced.get('limit_memory')
            }
            all_resources = list(requests.values()) + list(limits.values())
            if all(r is None for r in all_resources):
                resources = None
            else:
                resources = k8s.V1ResourceRequirements(
                    requests=requests,
                    limits=limits
                )

        annotations = namespaced.get('annotations', {})
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
            name=namespaced.get('name'),
            ports=namespaced.get('ports'),
            volumes=namespaced.get('volumes'),
            volume_mounts=namespaced.get('volume_mounts'),
            namespace=namespaced.get('namespace'),
            image_pull_policy=namespaced.get('image_pull_policy'),
            restart_policy=namespaced.get('restart_policy'),
            image_pull_secrets=namespaced.get('image_pull_secrets'),
            init_containers=namespaced.get('init_containers'),
            service_account_name=namespaced.get('service_account_name'),
            resources=resources,
            annotations=namespaced.get('annotations'),
            affinity=namespaced.get('affinity'),
            hostnetwork=namespaced.get('hostnetwork'),
            tolerations=namespaced.get('tolerations'),
            security_context=namespaced.get('security_context'),
            configmaps=namespaced.get('configmaps'),
            dnspolicy=namespaced.get('dnspolicy'),
            pod=namespaced.get('pod'),
            extract_xcom=namespaced.get('extract_xcom'),
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
        some overwritten, and some concatenated, e.g. The command
        should be preserved from base, the volumes appended to and
        the other fields overwritten.
        """

        client_pod_cp = copy.deepcopy(client_pod)

        def merge_objects(base_obj, client_obj):
            for base_key in base_obj.to_dict().keys():
                base_val = getattr(base_obj, base_key, None)
                if not getattr(client_obj, base_key, None) and base_val:
                    setattr(client_obj, base_key, base_val)

        def extend_object_field(base_obj, client_obj, field_name):
            base_obj_field = getattr(base_obj, field_name, None)
            client_obj_field = getattr(client_obj, field_name, None)
            if not base_obj_field:
                return
            if not client_obj_field:
                setattr(client_obj, field_name, base_obj_field)
                return
            appended_fields = base_obj_field + client_obj_field
            setattr(client_obj, field_name, appended_fields)

        # Values at the pod and metadata should be overwritten where they exist,
        # but certain values at the spec and container level must be conserved.
        base_container = base_pod.spec.containers[0]
        client_container = client_pod_cp.spec.containers[0]

        extend_object_field(base_container, client_container, 'volume_mounts')
        extend_object_field(base_container, client_container, 'env')
        extend_object_field(base_container, client_container, 'env_from')
        extend_object_field(base_container, client_container, 'ports')
        extend_object_field(base_container, client_container, 'volume_devices')
        client_container.command = base_container.command
        client_container.args = base_container.args
        merge_objects(base_pod.spec.containers[0], client_pod_cp.spec.containers[0])
        # Just append any additional containers from the base pod
        client_pod_cp.spec.containers.extend(base_pod.spec.containers[1:])

        merge_objects(base_pod.metadata, client_pod_cp.metadata)

        extend_object_field(base_pod.spec, client_pod_cp.spec, 'volumes')
        merge_objects(base_pod.spec, client_pod_cp.spec)
        merge_objects(base_pod, client_pod_cp)

        return client_pod_cp
