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
from typing import Dict, List, Optional, Union

import kubernetes.client.models as k8s

from airflow.version import version as airflow_version

MAX_POD_ID_LEN = 253


class PodDefaults:
    """
    Static defaults for Pods
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
    Any configuration that is container specific gets applied to
    the first container in the list of containers.

    Parameters with a type of `kubernetes.client.models.*`/`k8s.*` can
    often be replaced with their dictionary equivalent, for example the output of
    `sanitize_for_serialization`.

    :param image: The docker image
    :type image: Optional[str]
    :param name: name in the metadata section (not the container name)
    :type name: Optional[str]
    :param namespace: pod namespace
    :type namespace: Optional[str]
    :param volume_mounts: list of kubernetes volumes mounts
    :type volume_mounts: Optional[List[Union[k8s.V1VolumeMount, dict]]]
    :param envs: A dict containing the environment variables
    :type envs: Optional[Dict[str, str]]
    :param cmds: The command to be run on the first container
    :type cmds: Optional[List[str]]
    :param args: The arguments to be run on the pod
    :type args: Optional[List[str]]
    :param labels: labels for the pod metadata
    :type labels: Optional[Dict[str, str]]
    :param node_selectors: node selectors for the pod
    :type node_selectors: Optional[Dict[str, str]]
    :param ports: list of ports. Applies to the first container.
    :type ports: Optional[List[Union[k8s.V1ContainerPort, dict]]]
    :param volumes: Volumes to be attached to the first container
    :type volumes: Optional[List[Union[k8s.V1Volume, dict]]]
    :param image_pull_policy: Specify a policy to cache or always pull an image
    :type image_pull_policy: str
    :param restart_policy: The restart policy of the pod
    :type restart_policy: str
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a comma separated list:
        secret_a,secret_b
    :type image_pull_secrets: str
    :param init_containers: A list of init containers
    :type init_containers: Optional[List[k8s.V1Container]]
    :param service_account_name: Identity for processes that run in a Pod
    :type service_account_name: Optional[str]
    :param resources: Resource requirements for the first containers
    :type resources: Optional[Union[k8s.V1ResourceRequirements, dict]]
    :param annotations: annotations for the pod
    :type annotations: Optional[Dict[str, str]]
    :param affinity: A dict containing a group of affinity scheduling rules
    :type affinity: Optional[dict]
    :param hostnetwork: If True enable host networking on the pod
    :type hostnetwork: bool
    :param tolerations: A list of kubernetes tolerations
    :type tolerations: Optional[list]
    :param security_context: A dict containing the security context for the pod
    :type security_context: Optional[Union[k8s.V1PodSecurityContext, dict]]
    :param configmaps: Any configmap refs to envfrom.
        If more than one configmap is required, provide a comma separated list
        configmap_a,configmap_b
    :type configmaps: List[str]
    :param dnspolicy: Specify a dnspolicy for the pod
    :type dnspolicy: Optional[str]
    :param schedulername: Specify a schedulername for the pod
    :type schedulername: Optional[str]
    :param pod: The fully specified pod. Mutually exclusive with `path_or_string`
    :type pod: Optional[kubernetes.client.models.V1Pod]
    :param extract_xcom: Whether to bring up a container for xcom
    :type extract_xcom: bool
    """
    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        image: Optional[str] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        volume_mounts: Optional[List[Union[k8s.V1VolumeMount, dict]]] = None,
        envs: Optional[Dict[str, str]] = None,
        cmds: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
        labels: Optional[Dict[str, str]] = None,
        node_selectors: Optional[Dict[str, str]] = None,
        ports: Optional[List[Union[k8s.V1ContainerPort, dict]]] = None,
        volumes: Optional[List[Union[k8s.V1Volume, dict]]] = None,
        image_pull_policy: str = 'IfNotPresent',
        restart_policy: str = 'Never',
        image_pull_secrets: Optional[str] = None,
        init_containers: Optional[List[k8s.V1Container]] = None,
        service_account_name: Optional[str] = None,
        resources: Optional[Union[k8s.V1ResourceRequirements, dict]] = None,
        annotations: Optional[Dict[str, str]] = None,
        affinity: Optional[dict] = None,
        hostnetwork: bool = False,
        tolerations: Optional[list] = None,
        security_context: Optional[Union[k8s.V1PodSecurityContext, dict]] = None,
        configmaps: Optional[List[str]] = None,
        dnspolicy: Optional[str] = None,
        schedulername: Optional[str] = None,
        pod: Optional[k8s.V1Pod] = None,
        extract_xcom: bool = False,
    ):

        self.ud_pod = pod

        self.pod = k8s.V1Pod()
        self.pod.api_version = 'v1'
        self.pod.kind = 'Pod'

        # Pod Metadata
        self.metadata = k8s.V1ObjectMeta()
        self.metadata.labels = labels
        self.metadata.name = name
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
        self.spec.scheduler_name = schedulername
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

        result.metadata.name = self.make_unique_pod_id(result.metadata.name)

        if self.extract_xcom:
            result = self.add_sidecar(result)

        return result

    @staticmethod
    def add_sidecar(pod: k8s.V1Pod) -> k8s.V1Pod:
        """Adds sidecar"""
        pod_cp = copy.deepcopy(pod)
        pod_cp.spec.volumes = pod.spec.volumes or []
        pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
        pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
        pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
        pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

        return pod_cp

    @staticmethod
    def from_obj(obj) -> Optional[k8s.V1Pod]:
        """Converts to pod from obj"""
        if obj is None:
            return None

        if isinstance(obj, PodGenerator):
            return obj.gen_pod()

        if not isinstance(obj, dict):
            raise TypeError(
                'Cannot convert a non-dictionary or non-PodGenerator '
                'object into a KubernetesExecutorConfig')

        # We do not want to extract constant here from ExecutorLoader because it is just
        # A name in dictionary rather than executor selection mechanism and it causes cyclic import
        namespaced = obj.get("KubernetesExecutor", {})

        if not namespaced:
            return None

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
            schedulername=namespaced.get('schedulername'),
            pod=namespaced.get('pod'),
            extract_xcom=namespaced.get('extract_xcom'),
        )

        return pod_spec_generator.gen_pod()

    @staticmethod
    def reconcile_pods(base_pod: k8s.V1Pod, client_pod: Optional[k8s.V1Pod]) -> k8s.V1Pod:
        """
        :param base_pod: has the base attributes which are overwritten if they exist
            in the client pod and remain if they do not exist in the client_pod
        :type base_pod: k8s.V1Pod
        :param client_pod: the pod that the client wants to create.
        :type client_pod: k8s.V1Pod
        :return: the merged pods

        This can't be done recursively as certain fields some overwritten, and some concatenated.
        """
        if client_pod is None:
            return base_pod

        client_pod_cp = copy.deepcopy(client_pod)
        client_pod_cp.spec = PodGenerator.reconcile_specs(base_pod.spec, client_pod_cp.spec)

        client_pod_cp.metadata = merge_objects(base_pod.metadata, client_pod_cp.metadata)
        client_pod_cp = merge_objects(base_pod, client_pod_cp)

        return client_pod_cp

    @staticmethod
    def reconcile_specs(base_spec: Optional[k8s.V1PodSpec],
                        client_spec: Optional[k8s.V1PodSpec]) -> Optional[k8s.V1PodSpec]:
        """
        :param base_spec: has the base attributes which are overwritten if they exist
            in the client_spec and remain if they do not exist in the client_spec
        :type base_spec: k8s.V1PodSpec
        :param client_spec: the spec that the client wants to create.
        :type client_spec: k8s.V1PodSpec
        :return: the merged specs
        """
        if base_spec and not client_spec:
            return base_spec
        if not base_spec and client_spec:
            return client_spec
        elif client_spec and base_spec:
            client_spec.containers = PodGenerator.reconcile_containers(
                base_spec.containers, client_spec.containers
            )
            merged_spec = extend_object_field(base_spec, client_spec, 'volumes')
            return merge_objects(base_spec, merged_spec)

        return None

    @staticmethod
    def reconcile_containers(base_containers: List[k8s.V1Container],
                             client_containers: List[k8s.V1Container]) -> List[k8s.V1Container]:
        """
        :param base_containers: has the base attributes which are overwritten if they exist
            in the client_containers and remain if they do not exist in the client_containers
        :type base_containers: List[k8s.V1Container]
        :param client_containers: the containers that the client wants to create.
        :type client_containers: List[k8s.V1Container]
        :return: the merged containers

        The runs recursively over the list of containers.
        """
        if not base_containers:
            return client_containers
        if not client_containers:
            return base_containers

        client_container = client_containers[0]
        base_container = base_containers[0]
        client_container = extend_object_field(base_container, client_container, 'volume_mounts')
        client_container = extend_object_field(base_container, client_container, 'env')
        client_container = extend_object_field(base_container, client_container, 'env_from')
        client_container = extend_object_field(base_container, client_container, 'ports')
        client_container = extend_object_field(base_container, client_container, 'volume_devices')
        client_container = merge_objects(base_container, client_container)

        return [client_container] + PodGenerator.reconcile_containers(
            base_containers[1:], client_containers[1:]
        )

    @staticmethod
    def construct_pod(
        dag_id: str,
        task_id: str,
        pod_id: str,
        try_number: int,
        date: str,
        command: List[str],
        kube_executor_config: Optional[k8s.V1Pod],
        worker_config: k8s.V1Pod,
        namespace: str,
        worker_uuid: str
    ) -> k8s.V1Pod:
        """
        Construct a pod by gathering and consolidating the configuration from 3 places:
            - airflow.cfg
            - executor_config
            - dynamic arguments
        """
        dynamic_pod = PodGenerator(
            namespace=namespace,
            labels={
                'airflow-worker': worker_uuid,
                'dag_id': dag_id,
                'task_id': task_id,
                'execution_date': date,
                'try_number': str(try_number),
                'airflow_version': airflow_version.replace('+', '-'),
                'kubernetes_executor': 'True',
            },
            cmds=command,
            name=pod_id
        ).gen_pod()

        # Reconcile the pod generated by the Operator and the Pod
        # generated by the .cfg file
        pod_with_executor_config = PodGenerator.reconcile_pods(worker_config,
                                                               kube_executor_config)
        # Reconcile that pod with the dynamic fields.
        return PodGenerator.reconcile_pods(pod_with_executor_config, dynamic_pod)

    @staticmethod
    def make_unique_pod_id(dag_id):
        """
        Kubernetes pod names must be <= 253 chars and must pass the following regex for
        validation
        ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

        :param dag_id: a dag_id with only alphanumeric characters
        :return: ``str`` valid Pod name of appropriate length
        """
        if not dag_id:
            return None

        safe_uuid = uuid.uuid4().hex
        safe_pod_id = dag_id[:MAX_POD_ID_LEN - len(safe_uuid) - 1] + "-" + safe_uuid

        return safe_pod_id


def merge_objects(base_obj, client_obj):
    """
    :param base_obj: has the base attributes which are overwritten if they exist
        in the client_obj and remain if they do not exist in the client_obj
    :param client_obj: the object that the client wants to create.
    :return: the merged objects
    """
    if not base_obj:
        return client_obj
    if not client_obj:
        return base_obj

    client_obj_cp = copy.deepcopy(client_obj)

    for base_key in base_obj.to_dict().keys():
        base_val = getattr(base_obj, base_key, None)
        if not getattr(client_obj, base_key, None) and base_val:
            setattr(client_obj_cp, base_key, base_val)
    return client_obj_cp


def extend_object_field(base_obj, client_obj, field_name):
    """
    :param base_obj: an object which has a property `field_name` that is a list
    :param client_obj: an object which has a property `field_name` that is a list.
        A copy of this object is returned with `field_name` modified
    :param field_name: the name of the list field
    :type field_name: str
    :return: the client_obj with the property `field_name` being the two properties appended
    """
    client_obj_cp = copy.deepcopy(client_obj)
    base_obj_field = getattr(base_obj, field_name, None)
    client_obj_field = getattr(client_obj, field_name, None)

    if (not isinstance(base_obj_field, list) and base_obj_field is not None) or \
       (not isinstance(client_obj_field, list) and client_obj_field is not None):
        raise ValueError("The chosen field must be a list.")

    if not base_obj_field:
        return client_obj_cp
    if not client_obj_field:
        setattr(client_obj_cp, field_name, base_obj_field)
        return client_obj_cp

    appended_fields = base_obj_field + client_obj_field
    setattr(client_obj_cp, field_name, appended_fields)
    return client_obj_cp
