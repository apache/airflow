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
import datetime
import hashlib
import inspect
import os
import re
import uuid
from functools import reduce
from typing import Dict, List, Optional, Union

import yaml
from dateutil import parser
from kubernetes.client import models as k8s
from kubernetes.client.api_client import ApiClient

from airflow.exceptions import AirflowConfigException
from airflow.version import version as airflow_version

MAX_POD_ID_LEN = 253

MAX_LABEL_LEN = 63


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


def make_safe_label_value(string):
    """
    Valid label values must be 63 characters or less and must be empty or begin and
    end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
    dots (.), and alphanumerics between.

    If the label value is greater than 63 chars once made safe, or differs in any
    way from the original value sent to this function, then we need to truncate to
    53 chars, and append it with a unique hash.
    """
    safe_label = re.sub(r"^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$", "", string)

    if len(safe_label) > MAX_LABEL_LEN or string != safe_label:
        safe_hash = hashlib.md5(string.encode()).hexdigest()[:9]
        safe_label = safe_label[:MAX_LABEL_LEN - len(safe_hash) - 1] + "-" + safe_hash

    return safe_label


def datetime_to_label_safe_datestring(datetime_obj: datetime.datetime) -> str:
    """
    Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but
    not "_" let's
    replace ":" with "_"

    :param datetime_obj: datetime.datetime object
    :return: ISO-like string representing the datetime
    """
    return datetime_obj.isoformat().replace(":", "_").replace('+', '_plus_')


def label_safe_datestring_to_datetime(string: str) -> datetime.datetime:
    """
    Kubernetes doesn't permit ":" in labels. ISO datetime format uses ":" but not
    "_", let's
    replace ":" with "_"

    :param string: str
    :return: datetime.datetime object
    """
    return parser.parse(string.replace('_plus_', '+').replace("_", ":"))


class PodGenerator:
    """
    Contains Kubernetes Airflow Worker configuration logic

    Represents a kubernetes pod and manages execution of a single pod.
    Any configuration that is container specific gets applied to
    the first container in the list of containers.

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
    :param pod_template_file: Path to YAML file. Mutually exclusive with `pod`
    :type pod_template_file: Optional[str]
    :param extract_xcom: Whether to bring up a container for xcom
    :type extract_xcom: bool
    :param priority_class_name: priority class name for the launched Pod
    :type priority_class_name: str
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
        image_pull_policy: Optional[str] = None,
        restart_policy: Optional[str] = None,
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
        pod_template_file: Optional[str] = None,
        extract_xcom: bool = False,
        priority_class_name: Optional[str] = None,
    ):
        self.validate_pod_generator_args(locals())

        if pod_template_file:
            self.ud_pod = self.deserialize_model_file(pod_template_file)
        else:
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
        self.spec.priority_class_name = priority_class_name

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
                'memory': namespaced.get('request_memory'),
                'ephemeral-storage': namespaced.get('ephemeral-storage')
            }
            limits = {
                'cpu': namespaced.get('limit_cpu'),
                'memory': namespaced.get('limit_memory'),
                'ephemeral-storage': namespaced.get('ephemeral-storage')
            }
            all_resources = list(requests.values()) + list(limits.values())
            if all(r is None for r in all_resources):
                resources = None
            else:
                resources = k8s.V1ResourceRequirements(
                    requests=requests,
                    limits=limits
                )
        namespaced['resources'] = resources
        return PodGenerator(**namespaced).gen_pod()

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
        client_pod_cp.metadata = PodGenerator.reconcile_metadata(base_pod.metadata, client_pod_cp.metadata)
        client_pod_cp = merge_objects(base_pod, client_pod_cp)

        return client_pod_cp

    @staticmethod
    def reconcile_metadata(base_meta, client_meta):
        """
        Merge kubernetes Metadata objects
        :param base_meta: has the base attributes which are overwritten if they exist
            in the client_meta and remain if they do not exist in the client_meta
        :type base_meta: k8s.V1ObjectMeta
        :param client_meta: the spec that the client wants to create.
        :type client_meta: k8s.V1ObjectMeta
        :return: the merged specs
        """
        if base_meta and not client_meta:
            return base_meta
        if not base_meta and client_meta:
            return client_meta
        elif client_meta and base_meta:
            client_meta.labels = merge_objects(base_meta.labels, client_meta.labels)
            client_meta.annotations = merge_objects(base_meta.annotations, client_meta.annotations)
            extend_object_field(base_meta, client_meta, 'managed_fields')
            extend_object_field(base_meta, client_meta, 'finalizers')
            extend_object_field(base_meta, client_meta, 'owner_references')
            return merge_objects(base_meta, client_meta)

        return None

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
        date: datetime.datetime,
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
                'dag_id': make_safe_label_value(dag_id),
                'task_id': make_safe_label_value(task_id),
                'execution_date': datetime_to_label_safe_datestring(date),
                'try_number': str(try_number),
                'airflow_version': airflow_version.replace('+', '-'),
                'kubernetes_executor': 'True',
            },
            annotations={
                'dag_id': dag_id,
                'task_id': task_id,
                'execution_date': date.isoformat(),
                'try_number': str(try_number),
            },
            cmds=command,
            name=pod_id
        ).gen_pod()

        # Reconcile the pods starting with the first chronologically,
        # Pod from the airflow.cfg -> Pod from executor_config arg -> Pod from the K8s executor
        pod_list = [worker_config, kube_executor_config, dynamic_pod]

        return reduce(PodGenerator.reconcile_pods, pod_list)

    @staticmethod
    def deserialize_model_file(path: str) -> k8s.V1Pod:
        """
        :param path: Path to the file
        :return: a kubernetes.client.models.V1Pod

        Unfortunately we need access to the private method
        ``_ApiClient__deserialize_model`` from the kubernetes client.
        This issue is tracked here; https://github.com/kubernetes-client/python/issues/977.
        """
        api_client = ApiClient()
        if os.path.exists(path):
            with open(path) as stream:
                pod = yaml.safe_load(stream)
        else:
            pod = yaml.safe_load(path)

        # pylint: disable=protected-access
        return api_client._ApiClient__deserialize_model(pod, k8s.V1Pod)

    @staticmethod
    def make_unique_pod_id(dag_id):
        r"""
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

    @staticmethod
    def validate_pod_generator_args(given_args):
        """
        :param given_args: The arguments passed to the PodGenerator constructor.
        :type given_args: dict
        :return: None

        Validate that if `pod` or `pod_template_file` are set that the user is not attempting
        to configure the pod with the other arguments.
        """
        pod_args = list(inspect.signature(PodGenerator).parameters.items())

        def predicate(k, v):
            """
            :param k: an arg to PodGenerator
            :type k: string
            :param v: the parameter of the given arg
            :type v: inspect.Parameter
            :return: bool

            returns True if the PodGenerator argument has no default arguments
            or the default argument is None, and it is not one of the listed field
            in `non_empty_fields`.
            """
            non_empty_fields = {
                'pod', 'pod_template_file', 'extract_xcom', 'service_account_name', 'image_pull_policy',
                'restart_policy'
            }

            return (v.default is None or v.default is v.empty) and k not in non_empty_fields

        args_without_defaults = {k: given_args[k] for k, v in pod_args if predicate(k, v) and given_args[k]}

        if given_args['pod'] and given_args['pod_template_file']:
            raise AirflowConfigException("Cannot pass both `pod` and `pod_template_file` arguments")
        if args_without_defaults and (given_args['pod'] or given_args['pod_template_file']):
            raise AirflowConfigException(
                "Cannot configure pod and pass either `pod` or `pod_template_file`. Fields {} passed.".format(
                    list(args_without_defaults.keys())
                )
            )


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

    if isinstance(base_obj, dict) and isinstance(client_obj_cp, dict):
        base_obj_cp = copy.deepcopy(base_obj)
        base_obj_cp.update(client_obj_cp)
        return base_obj_cp

    for base_key in base_obj.to_dict().keys():
        base_val = getattr(base_obj, base_key, None)
        if not getattr(client_obj, base_key, None) and base_val:
            if not isinstance(client_obj_cp, dict):
                setattr(client_obj_cp, base_key, base_val)
            else:
                client_obj_cp[base_key] = base_val
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
